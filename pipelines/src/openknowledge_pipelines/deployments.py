"""The flows the control VM serves, each reading its settings from the run's environment."""

from __future__ import annotations

from prefect import flow
from prefect import serve as prefect_serve
from prefect.deployments.runner import RunnerDeployment
from prefect.types.entrypoint import EntrypointType

from openknowledge_core.ingestion import (
    BatchDownloadResult,
    DownloadDataset,
    LoadDataset,
    LoadDatasetResult,
)
from openknowledge_pipelines import bootstrap
from openknowledge_pipelines.settings import BatchSettings, LoadSettings

DEPLOYMENT = "control-vm"
RUNS_AT_ONCE = 2


@flow(name="s2-load", persist_result=False)
async def s2_load(command: LoadDataset) -> LoadDatasetResult:
    settings = LoadSettings()  # pyright: ignore[reportCallIssue]  # fields come from the environment
    async with bootstrap.open_load_service(settings) as service:
        # fn runs the body in this flow run, so its tasks do not start a subflow.
        return await bootstrap.load_flow(service, settings).fn(command)


@flow(name="s2-batch-download", persist_result=False)
async def s2_batch_download(command: DownloadDataset) -> BatchDownloadResult:
    settings = BatchSettings()  # pyright: ignore[reportCallIssue]
    async with bootstrap.open_batch_download(settings) as (service, jobs):
        return await bootstrap.batch_download_flow(service, jobs, settings).fn(command)


def serve() -> None:
    prefect_serve(
        RunnerDeployment.from_flow(s2_load, DEPLOYMENT, entrypoint_type=EntrypointType.MODULE_PATH),
        # One download at a time, since every run shares the S2 key's one request per second.
        # Prefect holds a second run until the first ends.
        RunnerDeployment.from_flow(
            s2_batch_download,
            DEPLOYMENT,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        limit=RUNS_AT_ONCE,
    )
