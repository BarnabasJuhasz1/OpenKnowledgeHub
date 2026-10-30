"""Freshness and rebuild recovery through the HTTP client, without sockets."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID

import httpx
import pytest

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.read_tables import ClickHouseReadTables
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_contracts.tables.clickhouse import version_expression
from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion.errors import TableRejected, TableUnavailable
from openknowledge_core.ingestion.read_tables import READ_TABLES
from tests.fakes.ports import NEXT, PRODUCER, RELEASE, applied_release, clock


@pytest.mark.parametrize("table", READ_TABLES)
@pytest.mark.parametrize(
    "state",
    ["current_empty", "missing_table", "replaced", "missing_marker", "stale", "papers_moved"],
)
async def test_read_tables_current_only_for_all_sources(table: str, state: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        query = request.content.decode()
        if "FROM system.tables" in query:
            assert "database = currentDatabase()" in query
            assert all(name in query for name in READ_TABLES)
            # Empty tables still have identities. No row-count heuristic is needed.
            rows = [
                json.dumps(
                    {
                        "name": name,
                        "uuid": str(UUID(int=2 if name == table and state == "replaced" else 1)),
                    }
                )
                for name in READ_TABLES
                if not (name == table and state == "missing_table")
            ]
            return httpx.Response(200, text="\n".join(rows))
        assert "FROM applied" in query
        assert "ORDER BY applied_at DESC LIMIT 1" in query
        target = request.url.params["param_target"]
        if target == table and state == "missing_marker":
            return httpx.Response(200)
        release = NEXT if target == table and state == "stale" else RELEASE
        marker = applied_release(target, release, timedelta())
        return httpx.Response(200, text=marker.model_dump_json())

    async with httpx.AsyncClient(
        base_url="http://clickhouse", transport=httpx.MockTransport(respond)
    ) as http:
        client = ClickHouseClient(http, database="test")
        edges = ClickHouseReadTables(
            client, ClickHouseTables(client, raw_bucket="raw"), ClickHouseAppliedReleases(client)
        )
        assert await edges.is_current(
            {"papers": NEXT if state == "papers_moved" else RELEASE, "citations": RELEASE}
        ) is (state == "current_empty")


@pytest.mark.parametrize(
    "failure", ["disk", "missing_column", "partial_swap", "markers", "lost_answer"]
)
async def test_rebuild_reclaims_staging_before_capacity_and_recovers(failure: str) -> None:
    live = {name: UUID(int=index + 1) for index, name in enumerate(READ_TABLES)}
    initial = live.copy()
    staging = {f"{name}_new": UUID(int=index + 3) for index, name in enumerate(READ_TABLES)}
    events: list[str] = []
    markers: list[str] = []
    failed = False
    retry = False
    next_uuid = 20
    stages: list[tuple[str, str]] = []

    @asynccontextmanager
    async def stage(name: str) -> AsyncGenerator[None]:
        stages.append((name, "started"))
        try:
            yield
        except BaseException:
            stages.append((name, "failed"))
            raise
        else:
            stages.append((name, "completed"))

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal failed, next_uuid
        query = request.url.params.get("query", request.content.decode())
        events.append(query)
        if "FROM system.columns" in query and "citation_edges_out" in query:
            rows = [{"table": "citation_edges_out"}]
            if failure != "missing_column" or retry:
                rows.append({"table": "citation_edges_in"})
        elif "FROM system.columns" in query:
            rows = [
                {
                    "default_expression": version_expression(
                        CONTRACTS[request.url.params["param_table"]]
                    )
                }
            ]
        elif "FROM applied" in query:
            target = request.url.params["param_target"]
            marker = next(
                (
                    marker
                    for insert in reversed(markers)
                    for marker in reversed(insert.splitlines())
                    if json.loads(marker)["target"] == target
                ),
                applied_release(target, NEXT, timedelta()).model_dump_json(),
            )
            return httpx.Response(200, text=marker)
        elif "FROM system.disks" in query:
            assert staging == {}, "capacity must be measured after reclaiming both copies"
            rows = [{"free_space": 0 if failure == "disk" and not retry else 12800}]
        elif "SELECT name, total_rows" in query:
            rows = [
                {"name": name, "total_rows": 100} for name in ("papers", "citations", *READ_TABLES)
            ]
        elif "SELECT name, uuid" in query:
            rows = [{"name": name, "uuid": str(uuid)} for name, uuid in live.items()]
        elif "FROM system.tables" in query:
            name = request.url.params["param_table"]
            rows = [{"uuid": str(live.get(name, UUID(int=1))), "total_rows": 100}]
        elif query.startswith("SELECT 1"):
            assert request.url.params["query_id"] == "citation-edges"
            assert "replace_running_query = 1" in query
            rows = []
        elif query.startswith("DROP TABLE"):
            assert any("replace_running_query = 1" in event for event in events)
            name = query.split()[-2]
            assert name.endswith("_new")
            staging.pop(name, None)
            rows = []
        elif query.startswith("CREATE TABLE"):
            name = query.split()[2]
            staging[name] = UUID(int=next_uuid)
            next_uuid += 1
            rows = []
        elif query.startswith(tuple(f"INSERT INTO {name}_new" for name in READ_TABLES)):
            return httpx.Response(200, headers={"X-ClickHouse-Summary": '{"written_rows":100}'})
        elif query.startswith("EXCHANGE"):
            new, name = query.split()[2], query.split()[4]
            if failure == "partial_swap" and name == READ_TABLES[1] and not failed:
                failed = True
                return httpx.Response(500, text="synthetic exchange failure")
            live[name], staging[new] = staging[new], live[name]
            rows = []
        elif query.startswith("INSERT INTO applied"):
            if failure in ("markers", "lost_answer") and not failed:
                failed = True
                if failure == "lost_answer":
                    markers.append(request.content.decode())
                return httpx.Response(500, text="synthetic marker failure")
            markers.append(request.content.decode())
            rows = []
        else:
            raise AssertionError(query)
        return httpx.Response(200, text="\n".join(json.dumps(row) for row in rows))

    async with httpx.AsyncClient(
        base_url="http://clickhouse", transport=httpx.MockTransport(respond)
    ) as http:
        client = ClickHouseClient(http, database="test")
        edges = ClickHouseReadTables(
            client,
            ClickHouseTables(client, raw_bucket="raw"),
            ClickHouseAppliedReleases(client),
            stage,
        )
        with pytest.raises(
            TableRejected if failure in ("disk", "missing_column") else TableUnavailable
        ) as error:
            await edges.build(PRODUCER, clock)
        if failure == "missing_column":
            assert "citation_edges_in.citationid is missing; run migration 00019" in str(
                error.value
            )
        if failure == "markers":
            # The new generation stays live but not current: swapping back is the riskier move.
            assert all(live[name] != initial[name] for name in READ_TABLES) and not markers
            assert not await edges.is_current({"papers": NEXT, "citations": NEXT})
        elif failure == "lost_answer":
            # The markers committed, so the next sync skips the build and only reclaims.
            assert await edges.is_current({"papers": NEXT, "citations": NEXT})
            assert set(staging.values()) == set(initial.values())
            await edges.reclaim()
            assert staging == {}
            return
        else:
            assert live == initial and not markers
        if failure in ("disk", "missing_column"):
            assert not any(event.startswith("CREATE TABLE") for event in events)
        assert stages[-1] == (
            "read tables: preflight and recovery"
            if failure in ("disk", "missing_column")
            else "read tables: publish",
            "failed",
        )
        stages.clear()
        retry = True
        result = await edges.build(PRODUCER, clock)
        assert result.source_releases == {"papers": NEXT, "citations": NEXT}
        assert await edges.is_current(result.source_releases)
        assert [name for name, state in stages if state == "completed"] == [
            "read tables: preflight and recovery",
            *(f"read tables: build {name}" for name in READ_TABLES),
            "read tables: validate counts",
            "read tables: publish",
        ]
        assert staging == {}
        # One insert, so a failure cannot leave some tables marked and others not.
        for name, marker in zip(READ_TABLES, markers[-1].splitlines(), strict=True):
            recorded = json.loads(marker)
            assert recorded["target"] == name
            assert recorded["target_uuid"] == str(live[name])
            assert recorded["release"] == NEXT
