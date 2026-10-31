"""The single deployment served on the OpenSearch VM."""

from prefect import flow
from prefect import serve as prefect_serve
from prefect.client.schemas.actions import DeploymentScheduleCreate
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments.runner import RunnerDeployment
from prefect.events import DeploymentEventTrigger
from prefect.types.entrypoint import EntrypointType

from openknowledge_pipelines import admission, bootstrap
from openknowledge_pipelines.flows.search_index import SearchIndexResult
from openknowledge_pipelines.settings import SearchSettings

DEPLOYMENT = "opensearch-vm"
DEPLOYMENT_NAME = f"search-index/{DEPLOYMENT}"


@flow(name="search-index", persist_result=False, retries=0)
async def search_index() -> SearchIndexResult:
    settings = SearchSettings()  # pyright: ignore[reportCallIssue]
    return await bootstrap.run_search_index(settings)


def index_deployment() -> RunnerDeployment:
    return RunnerDeployment.from_flow(
        search_index,
        DEPLOYMENT,
        entrypoint_type=EntrypointType.MODULE_PATH,
        concurrency_limit=1,
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed", "prefect.flow-run.Failed"},
                match_related={
                    "prefect.resource.role": "flow",
                    "prefect.resource.name": ["s2-load", "s2-load-diff", "s2-sync"],
                },
            )
        ],
        schedules=[
            DeploymentScheduleCreate(
                slug="search-index-fallback", schedule=CronSchedule(cron="0 */6 * * *")
            )
        ],
    )


def serve() -> None:
    admission.ensure_closed()
    prefect_serve(index_deployment(), limit=1)
