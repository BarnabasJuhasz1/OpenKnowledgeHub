"""The flows the control VM serves, each reading its settings from the run's environment."""

from __future__ import annotations

from prefect import flow
from prefect import serve as prefect_serve
from prefect.deployments.runner import RunnerDeployment
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
from openknowledge_pipelines.download_coordination import exclusive_download
from openknowledge_pipelines.settings import BatchSettings, LoadSettings

DEPLOYMENT = "control-vm"
RUNS_AT_ONCE = 2


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
    async with exclusive_download(), bootstrap.open_batch_download(settings) as (service, jobs):
        return await bootstrap.batch_download_flows(service, jobs, settings).dataset.fn(command)


@flow(name="s2-batch-download-diff", persist_result=False)
async def s2_batch_download_diff(command: DownloadDiff) -> BatchDiffDownloadResult:
    settings = BatchSettings()  # pyright: ignore[reportCallIssue]
    async with exclusive_download(), bootstrap.open_batch_download(settings) as (service, jobs):
        return await bootstrap.batch_download_flows(service, jobs, settings).diff.fn(command)


def serve() -> None:
    prefect_serve(
        RunnerDeployment.from_flow(s2_load, DEPLOYMENT, entrypoint_type=EntrypointType.MODULE_PATH),
        RunnerDeployment.from_flow(
            s2_load_diff, DEPLOYMENT, entrypoint_type=EntrypointType.MODULE_PATH
        ),
        # Each deployment queues its own runs; exclusive_download shares the key's gate
        # across both deployments, leaving load runs independent.
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
