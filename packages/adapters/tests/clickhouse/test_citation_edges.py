"""Freshness through the real HTTP client and applied-release adapter, without sockets."""

import json
from datetime import timedelta
from uuid import UUID

import httpx
import pytest

from openknowledge_adapters.clickhouse.citation_edges import ClickHouseCitationEdges
from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_core.ingestion.citation_edges import EDGE_TABLES
from tests.fakes.ports import NEXT, RELEASE, applied_release


@pytest.mark.parametrize("table", EDGE_TABLES)
@pytest.mark.parametrize(
    "state", ["current_empty", "missing_table", "replaced", "missing_marker", "stale"]
)
async def test_edge_freshness_checks_live_identity(table: str, state: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        query = request.content.decode()
        if "FROM system.tables" in query:
            assert "database = currentDatabase()" in query
            assert all(name in query for name in EDGE_TABLES)
            # Empty tables still have identities. No row-count heuristic is needed.
            rows = [
                json.dumps(
                    {
                        "name": name,
                        "uuid": str(UUID(int=2 if name == table and state == "replaced" else 1)),
                    }
                )
                for name in EDGE_TABLES
                if not (name == table and state == "missing_table")
            ]
            return httpx.Response(200, text="\n".join(rows))
        assert "FROM applied" in query
        assert "ORDER BY applied_at DESC LIMIT 1" in query
        target = request.url.params["param_target"]
        if target == table and state == "missing_marker":
            return httpx.Response(200)
        release = RELEASE if target == table and state == "stale" else NEXT
        marker = applied_release(target, release, timedelta())
        return httpx.Response(200, text=marker.model_dump_json())

    async with httpx.AsyncClient(
        base_url="http://clickhouse", transport=httpx.MockTransport(respond)
    ) as http:
        client = ClickHouseClient(http, database="test")
        edges = ClickHouseCitationEdges(
            client, ClickHouseTables(client, raw_bucket="raw"), ClickHouseAppliedReleases(client)
        )
        assert await edges.is_current(NEXT) is (state == "current_empty")
