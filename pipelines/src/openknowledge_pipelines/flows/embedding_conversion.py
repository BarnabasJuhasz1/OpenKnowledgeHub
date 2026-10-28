from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from types import CoroutineType
from typing import Any

from prefect import Flow, flow, task
from prefect.cache_policies import NO_CACHE

from openknowledge_core.embedding_conversion.models import (
    ConversionJob,
    ConvertEmbeddings,
)
from openknowledge_core.embedding_conversion.ports import ConversionJobs
from openknowledge_core.embedding_conversion.service import (
    EmbeddingConversionService,
    artifact_path,
)
from openknowledge_core.ingestion.archive_progress import HashedRef
from openknowledge_core.ingestion.errors import SharedCapacityError
from openknowledge_pipelines.flows.s2_download import retry_transient

type ConversionFlow = Flow[[ConvertEmbeddings], CoroutineType[Any, Any, HashedRef]]


def conversion_flow(
    service: EmbeddingConversionService,
    jobs: ConversionJobs,
    *,
    task_retries: int,
    retry_delay_seconds: float,
    execution: Callable[[], AbstractAsyncContextManager[None]] = nullcontext,
) -> ConversionFlow:
    @task(
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def run_job(job: ConversionJob) -> None:
        # Retrying submission retains the same operation ID. Completion verifies every
        # shard even when Batch reports success, including receipt replay after failures.
        await jobs.wait(await jobs.submit(job))

    @task(
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def drain_jobs() -> None:
        # Enumeration validates all pages before any owned job is waited on.
        for name in await jobs.conversion_running():
            await jobs.wait(name)

    @flow(name="embedding-conversion", persist_result=False)
    async def convert(command: ConvertEmbeddings) -> HashedRef:
        await drain_jobs()
        async with execution():
            plan = await service.prepare(command)
            reference = await service.publish_plan(plan)
            # Only terminal jobs with missing receipts get a new operation, for bounded rounds.
            # Exceptions propagate; task retries above are limited to transient transport errors.
            for _ in range(task_retries + 1):
                if await jobs.conversion_running():
                    raise SharedCapacityError(
                        "New conversion work appeared; retry to drain it before taking admission"
                    )
                missing: list[int] = []
                for index in range(len(plan.shards)):
                    if await service.store.stat(artifact_path(plan, f"{index}.json")) is None:
                        missing.append(index)
                if not missing:
                    break
                await run_job(
                    ConversionJob(
                        release_id=command.release,
                        dataset=command.dataset,
                        bucket=command.bucket,
                        prefix=command.prefix,
                        shards=tuple(missing),
                        plan=reference,
                    )
                )
            return await service.complete(plan)

    return convert
