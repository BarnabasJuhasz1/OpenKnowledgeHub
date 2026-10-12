"""The s2-batch-download flow: it pins and writes the manifest, and Batch archives the shards."""

from __future__ import annotations

from types import CoroutineType
from typing import Any

from prefect import Flow, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.ingestion import (
    BatchDownloadResult,
    DownloadDataset,
    DownloadDatasetService,
    JobOutcome,
    PinnedDataset,
    ShardJob,
    ShardJobs,
)
from openknowledge_pipelines.flows.s2_download import retry_transient

type S2BatchDownloadFlow = Flow[[DownloadDataset], CoroutineType[Any, Any, BatchDownloadResult]]


def s2_batch_download_flow(
    service: DownloadDatasetService,
    jobs: ShardJobs,
    *,
    rounds: int,
    task_retries: int,
    retry_delay_seconds: float,
) -> S2BatchDownloadFlow:
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
        name="find-unfinished",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def find_unfinished(pinned: PinnedDataset) -> tuple[int, ...]:
        return await service.unfinished(pinned)

    @task(
        name="run-job",
        task_run_name="round {round_}",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def run_job(pinned: PinnedDataset, shards: tuple[int, ...], round_: int) -> str:
        job = ShardJob(
            release_id=pinned.release_id,
            dataset=pinned.dataset,
            bucket=pinned.bucket,
            prefix=pinned.prefix,
            shards=shards,
        )
        name = await jobs.submit(job)
        logger = get_run_logger()
        logger.info("%s archives shards %s", name, ", ".join(map(str, shards)))
        if await jobs.wait(name) is JobOutcome.FAILED:
            logger.warning("%s failed; the next round runs the shards still unfinished", name)
        return name

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

    @flow(name="s2-batch-download", persist_result=False)
    async def s2_batch_download(command: DownloadDataset) -> BatchDownloadResult:
        pinned = await pin_dataset(command)
        names: list[str] = []
        for round_ in range(1, rounds + 1):
            shards = await find_unfinished(pinned)
            if not shards:
                break
            names.append(await run_job(pinned, shards, round_))
        manifest = await write_manifest(pinned)
        return BatchDownloadResult(
            release_id=pinned.release_id,
            dataset=pinned.dataset,
            shards=len(pinned.shards),
            jobs=tuple(names),
            manifest=manifest,
        )

    return s2_batch_download
