import httpx

from openknowledge_adapters import clickhouse
from packages.adapters.tests.clickhouse.test_tables import CONFIG


async def test_missing_telemetry_grants_have_safe_table_specific_hints() -> None:
    def denied(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            headers={"X-ClickHouse-Exception-Code": "497"},
            text="untrusted diagnostic with credentials and sensitive query contents",
        )

    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(denied)) as ch:
        snapshot = await ch.telemetry.snapshot("publication-venues")
    assert snapshot.active_table_bytes is None
    assert snapshot.server_memory_tracking_bytes is None
    assert snapshot.table_running_merges is None
    assert snapshot.server_merged_rows_total is None
    assert len(snapshot.errors) == 4
    for error, table in zip(snapshot.errors, ("parts", "metrics", "merges", "events"), strict=True):
        assert f"grant SELECT ON system.{table} to the load account" in error
        assert "access denied (code 497)" in error
        assert len(error) < 256
        assert "sensitive" not in error and "credentials" not in error
