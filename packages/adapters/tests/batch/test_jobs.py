from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from aiohttp import web
from gcloud.aio.auth import Token

from openknowledge_adapters import batch
from openknowledge_core.ingestion.errors import BatchRejected
from openknowledge_core.ingestion.models import JobOutcome, ShardJob

PARENT = "projects/p/locations/europe-west4"
SECRET = "projects/p/secrets/s2-api-key/versions/latest"
CONFIG = batch.BatchConfig(
    project="p",
    region="europe-west4",
    service_account="download@p.iam.gserviceaccount.com",
    network="projects/p/global/networks/n",
    subnetwork="projects/p/regions/europe-west4/subnetworks/download",
    image="repo/pipelines@sha256:" + "a" * 64,
    machine_type="e2-standard-2",
    max_run_seconds=3600,
    poll_seconds=0.01,
    environment={"OKP_IMAGE_DIGEST": "sha256:" + "a" * 64},
    secrets={"S2_API_KEY": SECRET},
)
JOB = ShardJob(
    release_id="2026-10-06", dataset="s2orc_v2", bucket="raw", prefix="s2", shards=(3, 7)
)


class FakeToken:
    async def get(self) -> str:
        return "token"


@asynccontextmanager
async def batch_api(
    created: web.Response, states: list[int | str]
) -> AsyncGenerator[tuple[str, list[dict[str, Any]]]]:
    posted: list[dict[str, Any]] = []

    async def create(request: web.Request) -> web.Response:
        posted.append({**request.query, **await request.json(), **request.headers})
        return created

    async def get(request: web.Request) -> web.Response:
        state = states.pop(0)
        if isinstance(state, int):
            return web.Response(status=state)
        return web.json_response({"status": {"state": state}})

    app = web.Application()
    app.router.add_post(f"/{PARENT}/jobs", create)
    app.router.add_get(f"/{PARENT}/jobs/j", get)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    try:
        yield f"http://{host}:{port}", posted
    finally:
        await runner.cleanup()


async def test_a_job_runs_one_task_per_shard_and_is_polled_until_it_ends() -> None:
    created = web.json_response({"name": f"{PARENT}/jobs/j"})
    async with (
        batch_api(created, ["QUEUED", 503, "RUNNING", "SUCCEEDED"]) as (root, posted),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        name = await jobs.submit(JOB)
        assert await jobs.wait(name) is JobOutcome.SUCCEEDED
    [request] = posted
    assert request["jobId"].startswith("s2-s2orc-v2-2026-10-06-")
    assert request["Authorization"] == "Bearer token"
    [group] = request["taskGroups"]
    assert [env["variables"] for env in group["taskEnvironments"]] == [
        {"OKP_SHARD": "3"},
        {"OKP_SHARD": "7"},
    ]
    assert group["parallelism"] == 1
    assert group["taskSpec"]["environment"]["secretVariables"] == {"S2_API_KEY": SECRET}
    [runnable] = group["taskSpec"]["runnables"]
    assert runnable["container"]["imageUri"] == CONFIG.image
    assert "--shard" not in runnable["container"]["commands"]
    assert request["allocationPolicy"]["network"]["networkInterfaces"][0]["subnetwork"] == (
        CONFIG.subnetwork
    )


async def test_a_refused_job_says_why() -> None:
    refused = web.json_response({"error": {"message": "actAs denied"}}, status=403)
    async with (
        batch_api(refused, []) as (root, _),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchRejected, match="HTTP 403: actAs denied"):
            await jobs.submit(JOB)
