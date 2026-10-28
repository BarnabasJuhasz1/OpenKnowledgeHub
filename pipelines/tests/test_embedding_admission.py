"""Conversion admission at real CLI/deployment boundaries without a control server."""

import asyncio
import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

import pytest
from click.testing import CliRunner
from prefect import Flow, Task

from openknowledge_core.embedding_conversion.models import ConversionJob, ConvertEmbeddings
from openknowledge_core.embedding_conversion.ports import ConversionJobs
from openknowledge_core.embedding_conversion.service import EmbeddingConversionService
from openknowledge_core.ingestion.models import JobOutcome
from openknowledge_pipelines import admission, bootstrap, cli, deployments
from openknowledge_pipelines.flows import embedding_conversion
from openknowledge_pipelines.settings import CommonSettings, ConversionBatchSettings
from tests.test_embedding_conversion import memory_plan
from tests.test_embedding_parquet_probe import sample


@pytest.mark.parametrize("entry", ["cli", "deployment"])
def test_owned_jobs_drain_before_closed_admission_refuses_new_work(
    monkeypatch: pytest.MonkeyPatch,
    entry: Literal["cli", "deployment"],
) -> None:
    service, _, store = asyncio.run(memory_plan())
    before = dict(store.objects)
    store.reads.clear()
    events: list[str] = []
    for key, value in {
        "IMAGE_DIGEST": "sha256:" + "a" * 64,
        "BATCH_PROJECT": "p",
        "BATCH_REGION": "r",
        "BATCH_SERVICE_ACCOUNT": "a",
        "BATCH_NETWORK": "n",
        "BATCH_SUBNETWORK": "s",
        "BATCH_IMAGE_REPOSITORY": "repo",
    }.items():
        monkeypatch.setenv(f"OKP_{key}", value)
    monkeypatch.delenv("S2_API_KEY", raising=False)

    class Jobs:
        async def conversion_running(self) -> tuple[str, ...]:
            events.append("list")
            return ("earlier-owned-job",)

        async def wait(self, name: str) -> JobOutcome:
            assert name == "earlier-owned-job"
            events.append("wait")
            return JobOutcome.SUCCEEDED

        async def submit(self, job: ConversionJob) -> str:
            pytest.fail("closed admission submitted new Batch work")

    @asynccontextmanager
    async def opened(
        settings: ConversionBatchSettings,
    ) -> AsyncGenerator[tuple[EmbeddingConversionService, ConversionJobs]]:
        yield service, Jobs()

    @asynccontextmanager
    async def closed() -> AsyncGenerator[None]:
        events.append("admission")
        raise admission.AdmissionError("Admission closed")
        yield  # pragma: no cover

    def direct_task(**kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:  # noqa: ANN401
        return lambda fn: Task(fn, **kwargs).fn

    def direct_flow(self: Flow[..., Any], *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        return self.fn(*args, **kwargs)

    def no_source_lock() -> None:
        pytest.fail("conversion must not take the source API lease")

    monkeypatch.setattr(bootstrap, "open_conversion_batch", opened)
    monkeypatch.setattr(admission, "execution", closed)
    monkeypatch.setattr(embedding_conversion, "task", direct_task)
    monkeypatch.setattr(Flow, "__call__", direct_flow)
    monkeypatch.setattr(deployments, "one_s2_user", no_source_lock)

    def configured_server(limits: tuple[str, ...]) -> None:
        pass

    if entry == "cli":
        monkeypatch.setattr(cli, "_require_managed_server", configured_server)
        result = CliRunner().invoke(
            cli.main,
            [
                "embeddings",
                "convert",
                "--release",
                "2026-10-13",
                "--bucket",
                "raw",
            ],
        )
        assert result.exit_code == 1 and "Admission closed" in result.output
    else:
        with pytest.raises(admission.AdmissionError, match="closed"):
            asyncio.run(
                deployments.embedding_conversion.fn(
                    ConvertEmbeddings(release="2026-10-13", bucket="raw"),
                )
            )
    assert events == ["list", "wait", "admission"]
    assert store.objects == before and store.reads == []


@pytest.mark.parametrize("worker", ["managed", "manual", "partial"])
def test_only_complete_batch_worker_context_bypasses_parent_admission(
    monkeypatch: pytest.MonkeyPatch,
    worker: str,
) -> None:
    service, plan, _ = asyncio.run(memory_plan())
    reference = asyncio.run(service.publish_plan(plan))
    opened: list[bool] = []
    for name in ("OKP_BATCH_OPERATION_ID", "BATCH_TASK_INDEX", "BATCH_TASK_COUNT", "OKP_SHARD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("S2_API_KEY", raising=False)
    if worker != "manual":
        monkeypatch.setenv("OKP_BATCH_OPERATION_ID", "a" * 32)
        monkeypatch.setenv("BATCH_TASK_INDEX", "0")
        monkeypatch.setenv("OKP_SHARD", "0")
    if worker == "managed":
        monkeypatch.setenv("BATCH_TASK_COUNT", "1")

    @asynccontextmanager
    async def conversion(settings: CommonSettings) -> AsyncGenerator[EmbeddingConversionService]:
        opened.append(True)
        yield service

    @asynccontextmanager
    async def closed() -> AsyncGenerator[None]:
        if worker == "managed":
            pytest.fail("valid child attempted to take duplicate admission")
        raise admission.AdmissionError("Admission closed")
        yield  # pragma: no cover

    monkeypatch.setattr(bootstrap, "open_embedding_conversion", conversion)
    monkeypatch.setattr(admission, "execution", closed)
    result = CliRunner().invoke(
        cli.main,
        [
            "embeddings",
            "convert-shard",
            "--plan",
            reference.model_dump_json(),
            "--shard",
            "0",
        ],
    )
    if worker == "managed":
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["receipt"]["rows"] == 1
        assert opened == [True]
    else:
        assert result.exit_code == 1 and "Admission closed" in result.output
        assert opened == []


def test_local_converter_remains_offline_with_closed_admission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source, receipt = sample(tmp_path, [0.1] * 768)

    def forbidden() -> None:
        pytest.fail("pure local conversion must not access admission")

    monkeypatch.setattr(admission, "execution", forbidden)
    monkeypatch.delenv("S2_API_KEY", raising=False)
    result = CliRunner().invoke(
        cli.main,
        [
            "embeddings",
            "convert-local",
            str(source),
            str(receipt),
            str(tmp_path / "output"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["rows"] == 3
