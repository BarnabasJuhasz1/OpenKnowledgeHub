"""The flows the control VM serves, each reading its settings from the run's environment."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager

from prefect import flow
from prefect import serve as prefect_serve
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.asyncio import concurrency
from prefect.deployments.runner import RunnerDeployment
from prefect.exceptions import ObjectNotFound
from prefect.types.entrypoint import EntrypointType

from openknowledge_core.ingestion import (
    BatchDiffDownloadResult,
    BatchDownloadResult,
    DownloadDataset,
    DownloadDiff,
    LoadDataset,
    LoadDatasetResult,
    LoadDiff,
    LoadDiffResult,
)
from openknowledge_pipelines import bootstrap
from openknowledge_pipelines.settings import BatchSettings, LoadSettings

DEPLOYMENT = "control-vm"
RUNS_AT_ONCE = 2
S2_LIMIT = "semantic-scholar"


def one_s2_user() -> AbstractAsyncContextManager[None]:
    # Strict, since Prefect skips a missing limit silently. A failed lease renewal must not
    # cancel a run waiting on its Batch job; the next run waits for that job anyway.
    return concurrency(S2_LIMIT, strict=True, raise_on_lease_renewal_failure=False)


@flow(name="s2-load", persist_result=False)
async def s2_load(command: LoadDataset) -> LoadDatasetResult:
    settings = LoadSettings()  # pyright: ignore[reportCallIssue]  # fields come from the environment
    async with bootstrap.open_load_service(settings) as service:
        # fn runs the body in this flow run, so its tasks do not start a subflow.
        return await bootstrap.load_flows(service, settings).dataset.fn(command)


@flow(name="s2-load-diff", persist_result=False)
async def s2_load_diff(command: LoadDiff) -> LoadDiffResult:
    settings = LoadSettings()  # pyright: ignore[reportCallIssue]
    async with bootstrap.open_load_service(settings) as service:
        return await bootstrap.load_flows(service, settings).diff.fn(command)


@flow(name="s2-batch-download", persist_result=False)
async def s2_batch_download(command: DownloadDataset) -> BatchDownloadResult:
    settings = BatchSettings()  # pyright: ignore[reportCallIssue]
    async with one_s2_user(), bootstrap.open_batch_download(settings) as (service, jobs):
        return await bootstrap.batch_download_flows(service, jobs, settings).dataset.fn(command)


@flow(name="s2-batch-download-diff", persist_result=False)
async def s2_batch_download_diff(command: DownloadDiff) -> BatchDiffDownloadResult:
    settings = BatchSettings()  # pyright: ignore[reportCallIssue]
    async with one_s2_user(), bootstrap.open_batch_download(settings) as (service, jobs):
        return await bootstrap.batch_download_flows(service, jobs, settings).diff.fn(command)


def serve() -> None:
    with get_client(sync_client=True) as client:
        try:
            client.read_global_concurrency_limit_by_name(S2_LIMIT)
        except ObjectNotFound:
            client.create_global_concurrency_limit(
                GlobalConcurrencyLimitCreate(name=S2_LIMIT, limit=1)
            )
    prefect_serve(
        RunnerDeployment.from_flow(s2_load, DEPLOYMENT, entrypoint_type=EntrypointType.MODULE_PATH),
        RunnerDeployment.from_flow(
            s2_load_diff, DEPLOYMENT, entrypoint_type=EntrypointType.MODULE_PATH
        ),
        # One run of each download at a time, held in Prefect's queue; S2_LIMIT keeps the two
        # deployments apart, since their tasks share the S2 key's one request per second.
        *(
            RunnerDeployment.from_flow(
                download,
                DEPLOYMENT,
                entrypoint_type=EntrypointType.MODULE_PATH,
                concurrency_limit=1,
            )
            for download in (s2_batch_download, s2_batch_download_diff)
        ),
        limit=RUNS_AT_ONCE,
    )
