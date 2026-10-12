"""The s2-load flow, one task per object so that a retry repeats only that object."""

from __future__ import annotations

from types import CoroutineType
from typing import Any

from prefect import Flow, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from openknowledge_contracts.manifests import ManifestEntry
from openknowledge_core.ingestion import (
    LoadDataset,
    LoadDatasetResult,
    LoadDatasetService,
    LoadOutcome,
    PreparedLoad,
)
from openknowledge_pipelines.flows.s2_download import retry_transient

type S2LoadFlow = Flow[[LoadDataset], CoroutineType[Any, Any, LoadDatasetResult]]


def s2_load_flow(
    service: LoadDatasetService, *, task_retries: int, retry_delay_seconds: float
) -> S2LoadFlow:
    @task(
        name="prepare-load",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def prepare(command: LoadDataset) -> PreparedLoad:
        return await service.prepare(command)

    @task(
        name="load-object",
        task_run_name="object {entry.object_ref.path.name}",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def load_object(prepared: PreparedLoad, entry: ManifestEntry) -> LoadOutcome:
        outcome = await service.load_object(prepared, entry)
        get_run_logger().info("%s was %s", entry.object_ref.path, outcome)
        return outcome

    @task(
        name="check-live-rows",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def finish(prepared: PreparedLoad, outcomes: list[LoadOutcome]) -> LoadDatasetResult:
        return await service.finish(prepared, outcomes)

    @flow(name="s2-load", persist_result=False)
    async def s2_load(command: LoadDataset) -> LoadDatasetResult:
        prepared = await prepare(command)
        outcomes = [await load_object(prepared, entry) for entry in prepared.manifest.entries]
        return await finish(prepared, outcomes)

    return s2_load
