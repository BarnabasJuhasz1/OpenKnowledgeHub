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
    WatchdogResult,
)
from openknowledge_pipelines import bootstrap
from openknowledge_pipelines.settings import BatchSettings, LoadSettings, WatchdogSettings

DEPLOYMENT = "control-vm"
# One load, one download and the watchdog at once. The two global limits below hold the loads and
# the downloads to one each, so this cap no longer guards ClickHouse or the S2 key; a run waiting
# on a limit still holds a slot, so three runs started by hand delay the watchdog's cron run.
RUNS_AT_ONCE = 3
S2_LIMIT = "semantic-scholar"
LOAD_LIMIT = "clickhouse"
WATCHDOG_CRON = "0 */6 * * *"


def one_s2_user() -> AbstractAsyncContextManager[None]:
    # Strict, since Prefect skips a missing limit silently. A failed lease renewal must not
    # cancel a run waiting on its Batch job; the next run waits for that job anyway.
    return concurrency(S2_LIMIT, strict=True, raise_on_lease_renewal_failure=False)


def one_clickhouse_loader() -> AbstractAsyncContextManager[None]:
    # Strict for the same reason. A failed lease renewal must not cancel a load in the middle of
    # an insert of hours.
    return concurrency(LOAD_LIMIT, strict=True, raise_on_lease_renewal_failure=False)


@flow(name="s2-load", persist_result=False)
async def s2_load(command: LoadDataset) -> LoadDatasetResult:
    settings = LoadSettings()  # pyright: ignore[reportCallIssue]  # fields come from the environment
    async with one_clickhouse_loader(), bootstrap.open_load_service(settings) as service:
        # fn runs the body in this flow run, so its tasks do not start a subflow.
        return await bootstrap.load_flows(service, settings).dataset.fn(command)


@flow(name="s2-load-diff", persist_result=False)
async def s2_load_diff(command: LoadDiff) -> LoadDiffResult:
    settings = LoadSettings()  # pyright: ignore[reportCallIssue]
    async with one_clickhouse_loader(), bootstrap.open_load_service(settings) as service:
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


@flow(name="watchdog", persist_result=False)
async def watchdog() -> WatchdogResult:
    settings = WatchdogSettings()  # pyright: ignore[reportCallIssue]
    async with bootstrap.open_watchdog(settings) as service:
        return await bootstrap.watchdog_flow(service, settings).fn()


def ensure_limits() -> None:
    """Create the global limits the flows take, since a strict limit must exist before a run."""
    with get_client(sync_client=True) as client:
        for name in (S2_LIMIT, LOAD_LIMIT):
            try:
                client.read_global_concurrency_limit_by_name(name)
            except ObjectNotFound:
                client.create_global_concurrency_limit(
                    GlobalConcurrencyLimitCreate(name=name, limit=1)
                )


def serve() -> None:
    ensure_limits()
    prefect_serve(
        # One run of each load at a time, held in Prefect's queue, so a second run of the same load
        # holds no serve slot. LOAD_LIMIT keeps the two deployments apart, so no two served loads
        # hit ClickHouse at once; a load waiting on it does hold a slot.
        RunnerDeployment.from_flow(
            s2_load, DEPLOYMENT, entrypoint_type=EntrypointType.MODULE_PATH, concurrency_limit=1
        ),
        RunnerDeployment.from_flow(
            s2_load_diff,
            DEPLOYMENT,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
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
        # Every 6 hours, for a log alert; it needs no S2 key, so it takes no limit.
        RunnerDeployment.from_flow(
            watchdog,
            DEPLOYMENT,
            entrypoint_type=EntrypointType.MODULE_PATH,
            cron=WATCHDOG_CRON,
        ),
        limit=RUNS_AT_ONCE,
    )
