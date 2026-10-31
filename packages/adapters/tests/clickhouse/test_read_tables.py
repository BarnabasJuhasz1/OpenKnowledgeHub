"""Freshness, rebuild recovery and in-place updates through the HTTP client, without sockets."""

import json
import logging
from collections.abc import AsyncGenerator, Callable, Iterable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import timedelta
from typing import Any
from uuid import UUID

import httpx
import pytest

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.read_tables import (
    BYTES_PER_CITATION,
    DELTA,
    DELTA_HEADROOM,
    ClickHouseReadTables,
)
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_contracts.tables.clickhouse import version_expression
from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion.errors import TableRejected, TableUnavailable
from openknowledge_core.ingestion.read_tables import (
    GROUPS,
    READ_TABLES,
    SOURCE_OF,
    ReadTablesPath,
)
from tests.fakes.ports import NEXT, PRODUCER, RELEASE, applied_release, clock

PAPERS, OUT, IN = READ_TABLES
FULL, INCREMENTAL, CURRENT = ReadTablesPath.FULL, ReadTablesPath.INCREMENTAL, ReadTablesPath.CURRENT


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


def marker(target: str, release: str, uuid: int = 1, from_release: str | None = None) -> str:
    found = applied_release(target, release, timedelta())
    update = {"target_uuid": UUID(int=uuid), "from_release": from_release}
    return found.model_copy(update=update).model_dump_json()


class FakeClickHouse:
    """The server a build talks to: live tables, scratch copies and applied markers."""

    def __init__(self) -> None:
        self.live = {name: UUID(int=index + 1) for index, name in enumerate(READ_TABLES)}
        self.staging = {
            f"{name}_new": UUID(int=index + 3) for index, name in enumerate(READ_TABLES)
        }
        self.history: list[str] = []
        """Markers present before the build, oldest first."""
        self.markers: list[str] = []
        """The bodies of the build's marker inserts."""
        self.events: list[str] = []
        self.replaced: set[str] = set()
        self.columns = {OUT, IN}
        self.free_space = DELTA_HEADROOM + 12800
        self.counts: dict[str, int] = {}
        self.touched = {"papers": 0, "citations": 0}
        self.zero_ids: set[str] = set()
        self.fail: Callable[[httpx.Request], httpx.Response | None] = lambda request: None
        self.next_uuid = 20

    def marked(self) -> list[dict[str, Any]]:
        bodies = (line for body in self.markers for line in body.splitlines())
        return [json.loads(line) for line in (*self.history, *bodies)]

    def respond(self, request: httpx.Request) -> httpx.Response:
        params = request.url.params
        query = params.get("query", request.content.decode())
        self.events.append(query)
        if (failure := self.fail(request)) is not None:
            return failure
        if query.startswith("SELECT 1 SETTINGS"):
            assert "replace_running_query = 1" in query
            self.replaced.add(params["query_id"])
            rows = []
        elif query.startswith("SELECT 1 FROM applied"):
            mine = [
                found
                for found in self.marked()
                if (found["target"], found["target_uuid"])
                == (params["param_target"], params["param_uuid"])
            ]
            if "from_release IS NULL" in query:
                after, through = params["param_after"], params["param_through"]
                mine = [
                    found
                    for found in mine
                    if found["from_release"] is None and after < found["release"] <= through
                ]
            else:
                mine = [found for found in mine if found["release"] == params["param_release"]]
            rows = [{"1": 1}] if mine else []
        elif query.startswith("SELECT 1 FROM"):
            assert "citationid = 0 LIMIT 1" in query
            if query.split()[3] not in self.columns:
                return httpx.Response(
                    404,
                    headers={"X-ClickHouse-Exception-Code": "47"},
                    text="Code: 47. DB::Exception: Unknown expression identifier `citationid`",
                )
            rows = [{"1": 1}] if query.split()[3] in self.zero_ids else []
        elif query.startswith("SELECT count() AS rows FROM"):
            rows = [{"rows": self.counts.get(query.split()[5], 100)}]
        elif "FROM system.columns" in query and OUT in query:
            rows = [{"table": table} for table in sorted(self.columns)]
        elif "FROM system.columns" in query:
            contract = CONTRACTS[params["param_table"]]
            rows = [{"default_expression": version_expression(contract)}]
        elif "FROM applied" in query:
            found = [item for item in self.marked() if item["target"] == params["param_target"]]
            rows = found[-1:]
        elif "FROM system.disks" in query:
            assert not any(name.endswith("_new") for name in self.staging), (
                "capacity must be measured after reclaiming the copies"
            )
            rows = [{"free_space": self.free_space}]
        elif "SELECT name, total_rows" in query:
            rows = [
                {"name": name, "total_rows": 100} for name in ("papers", "citations", *READ_TABLES)
            ]
        elif "SELECT name, uuid" in query:
            rows = [{"name": name, "uuid": str(uuid)} for name, uuid in self.live.items()]
        elif "FROM system.tables" in query:
            name = params["param_table"]
            rows = [{"uuid": str(self.live.get(name, UUID(int=1))), "total_rows": 100}]
        elif query.startswith("DROP TABLE"):
            assert self.replaced == {"citation-edges", "read-tables-delete"}
            name = query.removesuffix(" SYNC").split()[-1]
            # DROP ... SYNC after an insert hangs in ClickHouse 26.8, so scratch keys never wait.
            assert name.endswith("_new") or (name in DELTA.values() and not query.endswith("SYNC"))
            self.staging.pop(name, None)
            rows = []
        elif query.startswith("CREATE TABLE"):
            self.staging[query.split()[2]] = UUID(int=self.next_uuid)
            self.next_uuid += 1
            rows = []
        elif query.startswith("INSERT INTO applied"):
            self.markers.append(request.content.decode())
            rows = []
        elif query.startswith("INSERT INTO"):
            # A rerun replaces a still-running statement of the same kind.
            assert params["query_id"] == "citation-edges"
            name = query.split()[2]
            source = next((source for source, delta in DELTA.items() if delta == name), None)
            if source is not None:
                written = self.counts[name] = self.touched[source]
            else:
                written = 100 if name.endswith("_new") else self.touched[SOURCE_OF[name]]
            summary = json.dumps({"written_rows": written})
            return httpx.Response(200, headers={"X-ClickHouse-Summary": summary})
        elif query.startswith("DELETE FROM"):
            assert params["query_id"] == "read-tables-delete"
            assert query.split()[2] in self.live
            rows = []
        elif query.startswith("EXCHANGE"):
            new, name = query.split()[2], query.split()[4]
            self.live[name], self.staging[new] = self.staging[new], self.live[name]
            rows = []
        else:
            raise AssertionError(query)
        return httpx.Response(200, text="\n".join(json.dumps(row) for row in rows))


@asynccontextmanager
async def read_tables(
    server: FakeClickHouse,
    stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
    max_delta_keys: int = 100,
) -> AsyncGenerator[ClickHouseReadTables]:
    async with httpx.AsyncClient(
        base_url="http://clickhouse", transport=httpx.MockTransport(server.respond)
    ) as http:
        client = ClickHouseClient(http, database="test")
        yield ClickHouseReadTables(
            client,
            ClickHouseTables(client, raw_bucket="raw"),
            ClickHouseAppliedReleases(client),
            stage,
            max_delta_keys=max_delta_keys,
        )


def in_order(events: Iterable[str], prefixes: Iterable[str]) -> bool:
    remaining = iter(events)
    return all(any(event.startswith(prefix) for event in remaining) for prefix in prefixes)


@pytest.mark.parametrize(
    "failure", ["disk", "missing_column", "partial_swap", "markers", "lost_answer"]
)
async def test_rebuild_reclaims_staging_before_capacity_and_recovers(failure: str) -> None:
    server = FakeClickHouse()
    server.history = [marker(name, NEXT) for name in GROUPS]
    initial = server.live.copy()
    failed = False
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

    def fail(request: httpx.Request) -> httpx.Response | None:
        nonlocal failed
        query = request.url.params.get("query", request.content.decode())
        if failed:
            return None
        if failure == "partial_swap" and query.startswith(f"EXCHANGE TABLES {OUT}_new"):
            failed = True
            return httpx.Response(500, text="synthetic exchange failure")
        if failure in ("markers", "lost_answer") and query.startswith("INSERT INTO applied"):
            failed = True
            if failure == "lost_answer":
                server.markers.append(request.content.decode())
            return httpx.Response(500, text="synthetic marker failure")
        return None

    server.fail = fail
    if failure == "disk":
        server.free_space = 0
    if failure == "missing_column":
        server.columns = {OUT}
    async with read_tables(server, stage) as edges:
        with pytest.raises(
            TableRejected if failure in ("disk", "missing_column") else TableUnavailable
        ) as error:
            await edges.build(PRODUCER, clock)
        if failure == "missing_column":
            assert "citation_edges_in.citationid is missing; run migration 00020" in str(
                error.value
            )
        if failure == "markers":
            # The new generation stays live but not current: swapping back is the riskier move.
            assert all(server.live[name] != initial[name] for name in READ_TABLES)
            assert not server.markers
            assert not await edges.is_current({"papers": NEXT, "citations": NEXT})
        elif failure == "lost_answer":
            # The markers committed, so the next sync skips the build and only reclaims.
            assert await edges.is_current({"papers": NEXT, "citations": NEXT})
            assert set(server.staging.values()) == set(initial.values())
            await edges.reclaim()
            assert server.staging == {}
            return
        else:
            assert server.live == initial and not server.markers
        if failure in ("disk", "missing_column"):
            assert not any(event.startswith("CREATE TABLE") for event in server.events)
        assert stages[-1] == (
            "read tables: preflight and recovery"
            if failure in ("disk", "missing_column")
            else "read tables: publish",
            "failed",
        )
        stages.clear()
        server.free_space, server.columns = 12800, {OUT, IN}
        result = await edges.build(PRODUCER, clock)
        assert result.source_releases == {"papers": NEXT, "citations": NEXT}
        assert result.paths == {"papers": FULL, "citations": FULL}
        assert await edges.is_current(result.source_releases)
    assert [name for name, state in stages if state == "completed"] == [
        "read tables: recovery and plan",
        "read tables: preflight and recovery",
        *(f"read tables: build {name}" for name in READ_TABLES),
        "read tables: validate counts",
        "read tables: publish",
    ]
    assert server.staging == {}
    # One insert, so a failure cannot leave some tables marked and others not.
    for name, line in zip(READ_TABLES, server.markers[-1].splitlines(), strict=True):
        recorded = json.loads(line)
        assert (recorded["target"], recorded["target_uuid"], recorded["release"]) == (
            name,
            str(server.live[name]),
            NEXT,
        )


@pytest.mark.parametrize(
    "case",
    [
        "papers",
        "citations",
        "in_only_first",
        "in_only",
        "in_only_mismatch",
        "mismatch",
        "over_share",
        "over_cap",
        "low_disk",
        "missing_column",
        "full",
        "missing_marker",
        "uuid_moved",
        "markers_disagree",
        "rebased",
        "new_base",
        "ids_out",
    ],
)
async def test_build_updates_in_place_when_the_sources_moved_by_diffs(
    case: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    server = FakeClickHouse()
    moved = {"papers": "papers", "in_only": "", "in_only_mismatch": ""}.get(case, "citations")
    for source in GROUPS:
        server.history.append(marker(source, RELEASE, 9 if case == "rebased" else 1))
        if source == moved:
            start = None if case == "new_base" else RELEASE
            server.history.append(marker(source, NEXT, from_release=start))
        else:
            server.history.append(marker(source, RELEASE))
    for table in READ_TABLES:
        release = "2026-09-29" if case == "markers_disagree" and table == IN else RELEASE
        if not (case == "missing_marker" and table == IN):
            server.history.append(marker(table, release, server.live[table].int))
    if case == "uuid_moved":
        server.live[IN] = UUID(int=9)
    server.zero_ids = {OUT} if case == "ids_out" else {IN} if "in_only" in case else set()
    # 11 of the read tables' 100 rows could drop more than a full rebuild keeps.
    server.touched = {"papers": 4, "citations": 11 if case == "over_share" else 5}
    if case == "mismatch":
        server.counts[IN] = 99
    if case == "in_only_mismatch":
        server.counts[f"{IN}_new"] = 99
    if case == "low_disk":
        server.free_space = DELTA_HEADROOM + server.touched["citations"] * BYTES_PER_CITATION - 1
    if case == "missing_column":
        server.columns = {OUT}
    refusals = {
        "in_only_mismatch": "citation edge counts differ",
        "low_disk": "updating the citations read tables needs about 5 GiB",
        "missing_column": "citation_edges_in.citationid is missing; run migration 00020",
    }
    initial = server.live.copy()
    async with read_tables(server, max_delta_keys=4 if case == "over_cap" else 100) as edges:
        if case in refusals:
            with pytest.raises(TableRejected, match=refusals[case]):
                await edges.build(PRODUCER, clock)
            assert not any(event.startswith(("EXCHANGE", "DELETE")) for event in server.events)
            assert server.live == initial and not server.markers
            return
        result = await edges.build(PRODUCER, clock, full=case == "full")
    writes = [
        event
        for event in server.events
        if event.startswith(("CREATE", "INSERT INTO", "DELETE", "EXCHANGE"))
    ]
    recorded = [json.loads(line) for line in server.markers[-1].splitlines()]
    assert server.staging == {}
    if case in ("papers", "citations", "in_only_first"):
        assert result.paths == {
            source: INCREMENTAL if source == moved else CURRENT for source in GROUPS
        }
        assert result.touched == {moved: server.touched[moved]}
        assert [found["target"] for found in recorded] == list(GROUPS[moved])
        for found in recorded:
            assert found["target_uuid"] == str(server.live[found["target"]])
            assert (found["from_release"], found["release"]) == (RELEASE, NEXT)
        assert not any(event.startswith(f"CREATE TABLE {OUT}_new") for event in writes)
    if case == "papers":
        # Review Focus 4: a papers-only diff writes nothing to the edge tables.
        assert not any("citation_edges" in event for event in writes)
        assert server.live == initial and len(server.markers) == 1
    elif case == "citations":
        assert in_order(
            server.events,
            [
                f"INSERT INTO {DELTA['citations']}",
                f"DELETE FROM {OUT}",
                f"DELETE FROM {IN}",
                f"INSERT INTO {OUT} ",
                f"INSERT INTO {IN} ",
                f"SELECT count() AS rows FROM {OUT}",
                f"SELECT count() AS rows FROM {IN}",
                "INSERT INTO applied",
            ],
        )
        assert not any(PAPERS in event for event in writes)
        assert f"DELETE FROM {OUT} took" in caplog.text
        assert server.live == initial and len(server.markers) == 1
    elif case == "in_only_first":
        # The incoming edges are rebuilt from the outgoing ones, then both take the diff.
        assert in_order(
            server.events,
            [
                f"CREATE TABLE {IN}_new AS {IN}",
                f"INSERT INTO {IN}_new",
                f"EXCHANGE TABLES {IN}_new AND {IN}",
                "INSERT INTO applied",
                f"INSERT INTO {DELTA['citations']}",
                f"DELETE FROM {IN}",
            ],
        )
        first = json.loads(server.markers[0])
        assert (first["target"], first["release"], first["from_release"]) == (IN, RELEASE, None)
        assert server.live[IN] != initial[IN] and server.live[OUT] == initial[OUT]
    elif case == "in_only":
        # Rollout step 2: only the incoming edges are rebuilt, from the outgoing ones.
        assert result.paths == {"papers": CURRENT, "citations": ReadTablesPath.IN_ONLY}
        assert result.touched == {}
        assert [event for event in writes if event.startswith("EXCHANGE")] == [
            f"EXCHANGE TABLES {IN}_new AND {IN}"
        ]
        assert not any(
            event.startswith("DELETE") or DELTA["citations"] in event for event in writes
        )
        assert len(server.markers) == 1
        assert [
            (found["target"], found["release"], found["from_release"]) for found in recorded
        ] == [(IN, RELEASE, None)]
        assert recorded[0]["target_uuid"] == str(server.live[IN]) != str(initial[IN])
    else:
        assert result.paths == {"papers": FULL, "citations": FULL}
        # A fallback keeps the count that sent it there.
        fell_back = case in ("over_share", "over_cap", "mismatch")
        assert result.touched == ({"citations": server.touched["citations"]} if fell_back else {})
        assert all(f"CREATE TABLE {table}_new AS {table}" in writes for table in READ_TABLES)
        assert [found["target"] for found in recorded] == list(READ_TABLES)
        assert all(found["from_release"] is None for found in recorded)
        assert any(event.startswith("DELETE") for event in writes) is (case == "mismatch")
        if case == "mismatch":
            assert "citation edge counts differ" in caplog.text
