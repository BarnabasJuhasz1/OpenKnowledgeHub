import asyncio
import base64
from pathlib import Path

import pytest
from prefect.client.orchestration import get_client
from prefect.client.schemas.sorting import FlowRunSort
from prefect.settings import PREFECT_RESULTS_LOCAL_STORAGE_PATH, temporary_settings

from openknowledge_contracts.manifests import DatasetManifest
from openknowledge_core.ingestion import DownloadDatasetService, LoadDataset, LoadDatasetService
from openknowledge_core.ingestion.errors import ContractViolation, GzipInvalid, TableUnavailable
from openknowledge_pipelines.flows.s2_download import S2DownloadFlow, s2_download_flow
from openknowledge_pipelines.flows.s2_load import S2LoadFlow, s2_load_flow
from tests.fakes.ports import (
    COMMAND,
    PAPERS,
    PRODUCER,
    FakeSource,
    FakeStore,
    FakeTables,
    clock,
)

pytestmark = pytest.mark.usefixtures("prefect_server")


def build(service: DownloadDatasetService) -> S2DownloadFlow:
    return s2_download_flow(service, task_retries=2, retry_delay_seconds=0)


async def test_the_flow_archives_the_dataset_then_writes_its_manifest(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    result = await build(service)(COMMAND)
    assert (result.shards, result.created, result.recovered, result.verified) == (2, 2, 0, 0)
    manifest = DatasetManifest.from_json(store.data(result.manifest))
    assert len(manifest.entries) == 2
    rerun = DownloadDatasetService(
        catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
    )
    again = await build(rerun)(COMMAND)
    assert (again.created, again.verified, again.manifest) == (0, 2, result.manifest)


async def test_transient_errors_are_retried(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    source.unavailable = ["part-0.gz"]
    result = await build(service)(COMMAND)
    assert result.created == 2
    assert source.opened.count("part-0.gz") == 2


async def test_integrity_errors_are_not_retried(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    source.files["papers"]["part-0.gz"] = PAPERS["part-0.gz"][:-4]
    with pytest.raises(GzipInvalid):
        await build(service)(COMMAND)
    assert source.opened.count("part-0.gz") == 1


async def test_only_the_command_reaches_prefect(service: DownloadDatasetService) -> None:
    await build(service)(COMMAND)
    async with get_client() as client:
        [flow_run] = await client.read_flow_runs(sort=FlowRunSort.START_TIME_DESC, limit=1)
    assert flow_run.parameters == {"command": COMMAND.model_dump(mode="json")}


async def test_a_failing_run_persists_no_signed_link(
    service: DownloadDatasetService, source: FakeSource, tmp_path: Path
) -> None:
    """Prefect writes a failed flow's exception once, and that record must hold no signed link."""
    source.expired_listings = {1}
    source.files["papers"]["part-1.gz"] = PAPERS["part-1.gz"][:-4]
    with temporary_settings(updates={PREFECT_RESULTS_LOCAL_STORAGE_PATH: tmp_path}):
        with pytest.raises(GzipInvalid):
            await build(service)(COMMAND)
    [record] = await asyncio.to_thread(lambda: [path.read_bytes() for path in tmp_path.iterdir()])
    pickled = base64.b64decode(record)
    assert b"GzipInvalid" in pickled  # the record is the pickled exception, so the checks see it
    for blob in (record, pickled):
        assert b"secret" not in blob and b"Signature" not in blob


LOAD = LoadDataset(**COMMAND.model_dump())


def load_flow(store: FakeStore, tables: FakeTables) -> S2LoadFlow:
    loader = LoadDatasetService(store=store, tables=tables, producer=PRODUCER, clock=clock)
    return s2_load_flow(loader, task_retries=2, retry_delay_seconds=0)


async def test_the_load_flow_loads_each_object_once(
    service: DownloadDatasetService, store: FakeStore
) -> None:
    await build(service)(COMMAND)
    tables = FakeTables(store)
    first = await load_flow(store, tables)(LOAD)
    again = await load_flow(store, tables)(LOAD)
    assert (first.loaded, again.already_loaded, len(tables.loads)) == (2, 2, 2)


async def test_only_transient_load_errors_are_retried(
    service: DownloadDatasetService, store: FakeStore
) -> None:
    await build(service)(COMMAND)
    tables = FakeTables(store)
    tables.failures = [TableUnavailable("try again"), ContractViolation("bad record")]
    with pytest.raises(ContractViolation):
        await load_flow(store, tables)(LOAD)
    assert tables.loads == ["part-0.gz", "part-0.gz"]
