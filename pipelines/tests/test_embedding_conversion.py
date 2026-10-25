"""Batch wiring and flow control with fake submission boundaries, without a server."""

from collections.abc import Callable
from typing import Any, Literal

import pytest
from prefect import Task

from openknowledge_adapters.batch import BatchConfig, job_spec
from openknowledge_adapters.parquet import StoredParquetArtifacts
from openknowledge_core.embedding_conversion.models import (
    ConversionJob,
    ConversionPlan,
    ConvertEmbeddings,
)
from openknowledge_core.embedding_conversion.service import (
    EmbeddingConversionService,
    artifact_path,
)
from openknowledge_core.ingestion.archive_progress import ArchiveProgress
from openknowledge_core.ingestion.errors import ArchiveConflict, BatchRejected, BatchTimedOut
from openknowledge_core.ingestion.models import JobOutcome
from openknowledge_core.ingestion.service import DownloadDatasetService
from openknowledge_pipelines.flows import embedding_conversion
from openknowledge_pipelines.settings import ConversionBatchSettings
from tests.fakes.ports import PRODUCER, FakeSource, FakeStore, clock, gz
from tests.test_embedding_conversion import rows


@pytest.mark.parametrize("failure", [None, "missing", "permanent", "timeout"])
async def test_flow_submits_one_task_per_missing_shard_and_requires_receipts(
    monkeypatch: pytest.MonkeyPatch,
    failure: Literal["missing", "permanent", "timeout"] | None,
) -> None:
    store = FakeStore()
    source = FakeSource()
    command = ConvertEmbeddings(release="2026-10-13", bucket="raw")
    source.add(command.dataset, "a.gz", gz(rows(7)))
    source.add(command.dataset, "b.gz", gz(rows(8)))
    downloads = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
    )
    base = await downloads.pin_dataset(command)
    for shard in base.shards:
        await downloads.archive_shard(base, shard)
    await ArchiveProgress(store, downloads, PRODUCER, clock).finalize(command)
    service = EmbeddingConversionService(store, StoredParquetArtifacts(store))
    submitted: list[ConversionJob] = []

    active: list[str] = []
    waited: list[str] = []
    timed_out = False

    class Jobs:
        async def conversion_running(self) -> tuple[str, ...]:
            return tuple(active)

        async def submit(self, job: ConversionJob) -> str:
            assert not active, "new work started before draining owned jobs"
            submitted.append(job)
            if failure == "permanent":
                raise BatchRejected("permission denied")
            active.append("job")
            return "job"

        async def wait(self, name: str) -> JobOutcome:
            nonlocal timed_out
            waited.append(name)
            if failure == "timeout" and not timed_out:
                timed_out = True
                raise BatchTimedOut("still running")
            plan = await service.reader.read(submitted[-1].plan, ConversionPlan)
            if failure != "missing":
                for index in submitted[-1].shards:
                    await service.convert_shard(plan, index)
            active.remove(name)
            # Even a false Batch success must not authorize completion.
            return JobOutcome.SUCCEEDED

    def direct_task(**kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:  # noqa: ANN401
        def unwrap(fn: Callable[..., Any]) -> Callable[..., Any]:
            configured = Task(fn, **kwargs)
            assert configured.retry_condition_fn is embedding_conversion.retry_transient
            return configured.fn

        return unwrap

    monkeypatch.setattr(embedding_conversion, "task", direct_task)
    flow = embedding_conversion.conversion_flow(
        service,
        Jobs(),
        task_retries=1,
        retry_delay_seconds=0,
    )
    plan = await service.prepare(command)
    if failure == "missing":
        with pytest.raises(ArchiveConflict):
            await flow.fn(command)
        assert len(submitted) == 2
        assert await store.stat(artifact_path(plan, "complete.json")) is None
    elif failure == "permanent":
        with pytest.raises(BatchRejected):
            await flow.fn(command)
        assert len(submitted) == 1
        assert await store.stat(artifact_path(plan, "complete.json")) is None
    else:
        if failure == "timeout":
            with pytest.raises(BatchTimedOut):
                await flow.fn(command)
            assert len(submitted) == 1 and active == ["job"]
        result = await flow.fn(command)
        assert result == await service.complete(plan)
        await flow.fn(command)
        assert len(submitted) == 1
        assert waited == (["job", "job"] if failure == "timeout" else ["job"])
    assert submitted[0].shards == (0, 1)
    config = BatchConfig(
        project="p",
        region="r",
        service_account="account",
        network="n",
        subnetwork="s",
        image="repo@sha256:" + "a" * 64,
        machine_type="e2-standard-2",
        parallelism=2,
        max_run_seconds=600,
        poll_seconds=1,
        environment={},
        secrets={"S2_API_KEY": "secret"},
    )
    spec = job_spec(config, submitted[0])
    group = spec["taskGroups"][0]
    assert group["taskEnvironments"] == [
        {"variables": {"OKP_SHARD": "0"}},
        {"variables": {"OKP_SHARD": "1"}},
    ]
    assert group["taskSpec"]["environment"]["secretVariables"] == {}
    assert group["taskSpec"]["runnables"][0]["container"]["commands"][:2] == [
        "embeddings",
        "convert-shard",
    ]


def test_conversion_settings_need_no_source_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S2_API_KEY", raising=False)
    settings = ConversionBatchSettings(
        image_digest="sha256:" + "a" * 64,
        batch_project="p",
        batch_region="r",
        batch_service_account="account",
        batch_network="n",
        batch_subnetwork="s",
        batch_image_repository="repo",
    )
    assert "s2_api_key" not in type(settings).model_fields


@pytest.mark.parametrize("permanent", [False, True])
async def test_task_retry_policy_classifies_batch_errors(permanent: bool) -> None:
    from prefect.client.schemas.objects import TaskRun
    from prefect.states import Failed

    from openknowledge_core.ingestion.errors import BatchUnavailable

    error = BatchRejected("denied") if permanent else BatchUnavailable("HTTP 503")
    assert await embedding_conversion.retry_transient(
        Task(lambda: None),
        TaskRun.model_construct(),
        Failed(data=error),
    ) is (not permanent)
