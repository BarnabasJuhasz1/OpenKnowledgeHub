"""The s2-download flow, whose tasks close over the service so Prefect records no client or key."""

from __future__ import annotations

from types import CoroutineType
from typing import Any

from prefect import Flow, Task, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.client.schemas.objects import State, TaskRun

from openknowledge_contracts.datasets import ShardRef
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.ingestion import (
    DownloadDataset,
    DownloadDatasetResult,
    DownloadDatasetService,
    PinnedDataset,
    ShardOutcome,
    TransientError,
)

type S2DownloadFlow = Flow[[DownloadDataset], CoroutineType[Any, Any, DownloadDatasetResult]]


async def retry_transient(task: Task[..., Any], task_run: TaskRun, state: State[Any]) -> bool:
    """Async because ``State.result`` in an async task returns an awaitable instead of raising."""
    failure = await state.aresult(raise_on_failure=False)
    return isinstance(failure, TransientError)


def s2_download_flow(
    service: DownloadDatasetService, *, task_retries: int, retry_delay_seconds: float
) -> S2DownloadFlow:
    @task(
        name="pin-dataset",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def pin_dataset(command: DownloadDataset) -> PinnedDataset:
        return await service.pin_dataset(command)

    @task(
        name="archive-shard",
        task_run_name="shard {shard.file_name}",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def archive_shard(pinned: PinnedDataset, shard: ShardRef) -> ShardOutcome:
        outcome = await service.archive_shard(pinned, shard)
        get_run_logger().info("%s was %s", shard.file_name, outcome)
        return outcome

    @task(
        name="write-manifest",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def write_manifest(pinned: PinnedDataset) -> ObjectPath:
        return await service.write_manifest(pinned)

    @flow(name="s2-download", persist_result=False)
    async def s2_download(command: DownloadDataset) -> DownloadDatasetResult:
        pinned = await pin_dataset(command)
        outcomes = [await archive_shard(pinned, shard) for shard in pinned.shards]
        manifest = await write_manifest(pinned)
        return service.result(pinned, outcomes, manifest)

    return s2_download
