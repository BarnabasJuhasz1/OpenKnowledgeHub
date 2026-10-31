"""Exercise indexed receipt serialization and source identity against real ClickHouse SQL."""

from datetime import UTC, datetime
from pathlib import Path

import httpx

from openknowledge_adapters import clickhouse
from openknowledge_core.search.progress import Indexed, plan_index
from tests.clickhouse_local import needs_clickhouse, run, up


@needs_clickhouse
async def test_receipts_round_trip_and_replaced_sources_need_new_applied(tmp_path: Path) -> None:
    initialized = run(
        "CREATE DATABASE source ENGINE = Atomic; USE source;"
        + up()
        + """
        INSERT INTO applied (target, target_uuid, release, software_version, applied_at)
        SELECT name, uuid, '2026-10-20', 'test', now64(3) FROM system.tables
        WHERE database = currentDatabase() AND name IN ('papers', 'abstracts', 'tldrs');
        """,
        tmp_path,
    )
    assert initialized.returncode == 0, initialized.stderr

    def handle(request: httpx.Request) -> httpx.Response:
        query = request.url.params.get("query", "")
        sql = (query + "\n" if query else "") + request.content.decode()
        result = run(
            "USE source; SET date_time_input_format='best_effort', date_time_output_format='iso'; "
            + sql,
            tmp_path,
        )
        assert result.returncode == 0, result.stderr
        return httpx.Response(200, text=result.stdout)

    config = clickhouse.ClickHouseConfig.model_validate(
        {
            "url": "http://clickhouse:8123",
            "password": "test",
            "user": "test",
            "timeout_seconds": 5,
            "raw_bucket": "raw",
        }
    )
    async with clickhouse.connect(config, transport=httpx.MockTransport(handle)) as ch:
        state = await ch.indexed.state()
        assert set(state.releases) == {"papers", "abstracts", "tldrs"}
        decision = plan_index(state, [], base_parts=2, diff_parts=2)
        assert decision.plan is not None
        receipt = Indexed(plan=decision.plan, kind="part", part=1, recorded_at=datetime.now(UTC))
        await ch.indexed.record(receipt)
        assert await ch.indexed.receipts() == [receipt]
        result = run(
            "USE source; RENAME TABLE papers TO old_papers; CREATE TABLE papers AS old_papers;",
            tmp_path,
        )
        assert result.returncode == 0, result.stderr
        assert "papers" not in (await ch.indexed.state()).releases
