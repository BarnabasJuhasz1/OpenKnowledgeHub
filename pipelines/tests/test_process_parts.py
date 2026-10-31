import asyncio
from functools import partial
from pathlib import Path

import pytest
from pydantic import ValidationError

from openknowledge_core.search.commands import IndexPapers
from openknowledge_pipelines.flows.process_parts import PartProcesses, PartWorkerLost
from openknowledge_pipelines.settings import SearchSettings
from tests.fakes.search_parts import delayed_worker, failed_worker, generated_index, index_generated


def command(part: int = 1) -> IndexPapers:
    return IndexPapers(
        index="papers-v2-test", release="2026-10-27", part=part, parts=2, bounds=(1, 20)
    )


async def test_spawned_index_matches_in_process() -> None:
    workers = PartProcesses(index_generated, 2)
    try:
        expected = await generated_index(command())
        results = await asyncio.gather(workers.index(command()), workers.index(command(2)))
        assert results[0] == expected
        assert sum(r.written for r in results) == 20
    finally:
        await workers.close()


async def test_abrupt_worker_failure_is_reported() -> None:
    workers = PartProcesses(failed_worker, 1)
    try:
        with pytest.raises(PartWorkerLost, match="admission retained"):
            await workers.index(command())
    finally:
        await workers.close()


async def test_cancellation_drains_and_submission_is_bounded(tmp_path: Path) -> None:
    workers = PartProcesses(partial(delayed_worker, str(tmp_path)), 1)
    first = asyncio.create_task(workers.index(command()))
    second = asyncio.create_task(workers.index(command(2)))
    try:
        async with asyncio.timeout(15):
            while not (tmp_path / "1.started").exists():  # noqa: ASYNC110
                await asyncio.sleep(0.01)
        assert not (tmp_path / "2.started").exists()
        second.cancel()
        first.cancel()
        await asyncio.sleep(0.05)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        with pytest.raises(asyncio.CancelledError):
            await second
        assert (tmp_path / "1.finished").exists()
        assert not (tmp_path / "2.started").exists()
    finally:
        await workers.close()


def test_worker_settings_bounds_and_default() -> None:
    settings = SearchSettings.model_validate(
        {
            "clickhouse_url": "http://localhost:8123",
            "clickhouse_password": "test",
            "clickhouse_raw_bucket": "test-bucket",
            "opensearch_url": "http://localhost:9200",
            "opensearch_password": "test",
        }
    )
    assert settings.index_parallelism == 2
    for invalid in (0, 5):
        with pytest.raises(ValidationError):
            SearchSettings.model_validate({**settings.model_dump(), "index_parallelism": invalid})


async def test_typed_domain_failure_survives_spawn() -> None:
    from openknowledge_core.search.errors import SearchUnavailable
    from tests.fakes.search_parts import domain_failure

    workers = PartProcesses(domain_failure, 1)
    try:
        with pytest.raises(SearchUnavailable, match="corpusid 12: overloaded; rerun"):
            await workers.index(command())
    finally:
        await workers.close()
