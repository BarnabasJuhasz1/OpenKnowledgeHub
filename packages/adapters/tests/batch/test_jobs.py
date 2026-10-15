import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Literal, cast
from uuid import UUID

import pytest
from aiohttp import web
from gcloud.aio.auth import Token
from pydantic import ValidationError

from openknowledge_adapters import batch
from openknowledge_adapters.batch.jobs import job_id
from openknowledge_core.ingestion.errors import BatchRejected, BatchTimedOut, BatchUnavailable
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
    created: web.Response, states: list[int | str], *, poll_delay: float = 0
) -> AsyncGenerator[tuple[str, list[dict[str, Any]]]]:
    posted: list[dict[str, Any]] = []

    async def create(request: web.Request) -> web.Response:
        posted.append({**request.query, **await request.json(), **request.headers})
        return created

    async def get(request: web.Request) -> web.Response:
        await asyncio.sleep(poll_delay)
        state = states.pop(0) if len(states) > 1 else states[0]
        if isinstance(state, int):
            return web.Response(status=state)
        return web.json_response({"status": {"state": state}})

    app = web.Application()
    app.router.add_post(f"/{PARENT}/jobs", create)
    app.router.add_get(f"/{PARENT}/jobs/{{job_id}}", get)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    try:
        yield f"http://{host}:{port}", posted
    finally:
        await runner.cleanup()


@pytest.mark.parametrize("provisioning_model", ["SPOT", "STANDARD"])
async def test_a_job_runs_one_task_per_shard_and_is_polled_until_it_ends(
    provisioning_model: Literal["SPOT", "STANDARD"],
) -> None:
    config = CONFIG.model_copy(update={"provisioning_model": provisioning_model})
    created = web.json_response(
        {"name": f"{PARENT}/jobs/{job_id(JOB)}", "labels": batch.job_spec(config, JOB)["labels"]}
    )
    async with (
        batch_api(created, ["QUEUED", 503, "RUNNING", "SUCCEEDED"]) as (root, posted),
        batch.connect(config, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        name = await jobs.submit(JOB)
        assert await jobs.wait(name) is JobOutcome.SUCCEEDED
    [request] = posted
    assert request["jobId"].startswith("s2-s2orc-v2-2026-10-06-")
    assert request["requestId"] == str(JOB.operation_id)
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
    assert request["allocationPolicy"]["instances"][0]["policy"]["provisioningModel"] == (
        provisioning_model
    )


async def test_a_refused_job_says_why() -> None:
    refused = web.json_response({"error": {"message": "actAs denied"}}, status=403)
    async with (
        batch_api(refused, []) as (root, _),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchRejected, match="HTTP 403: actAs denied"):
            await jobs.submit(JOB)


@pytest.mark.parametrize("state", ["QUEUED", "RUNNING", 503])
async def test_queued_running_or_unreachable_jobs_have_a_bounded_wait(state: int | str) -> None:
    config = CONFIG.model_copy(update={"wait_timeout_seconds": 0.05, "poll_seconds": 3600})
    async with (
        batch_api(web.json_response({}), [state]) as (root, _),
        batch.connect(config, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchTimedOut, match=f"{PARENT}/jobs/j.*may still be running"):
            await asyncio.wait_for(jobs.wait(f"{PARENT}/jobs/j"), timeout=1)


async def test_the_wait_deadline_also_covers_an_inflight_poll() -> None:
    config = CONFIG.model_copy(update={"wait_timeout_seconds": 0.05})
    async with (
        batch_api(web.json_response({}), ["SUCCEEDED"], poll_delay=0.2) as (root, _),
        batch.connect(config, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchTimedOut):
            await asyncio.wait_for(jobs.wait(f"{PARENT}/jobs/j"), timeout=1)


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
async def test_failed_or_cancelled_jobs_end_the_wait(state: str) -> None:
    async with (
        batch_api(web.json_response({}), [state]) as (root, _),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        assert await jobs.wait(f"{PARENT}/jobs/j") is JobOutcome.FAILED


def test_a_diff_part_names_the_release_it_starts_from_and_its_part() -> None:
    job = JOB.model_copy(update={"from_release": "2026-09-29", "part": "deletes"})
    spec = batch.job_spec(CONFIG, job)
    command = spec["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["commands"]
    assert command[-4:] == ["--from-release", "2026-09-29", "--part", "deletes"]
    assert spec["labels"]["part"] == "deletes"
    assert job_id(job).startswith("s2-s2orc-v2-2026-10-06-deletes-")


@asynccontextmanager
async def replay_api(
    *, replay: bool, existing: dict[str, Any] | None = None, get_status: int = 200
) -> AsyncGenerator[tuple[str, list[dict[str, str]], dict[str, dict[str, Any]]]]:
    posted: list[dict[str, str]] = []
    remote = {job_id(JOB): existing} if existing is not None else {}

    async def create(request: web.Request) -> web.Response:
        posted.append(dict(request.query))
        id_ = request.query["jobId"]
        payload = cast(dict[str, Any], await request.json())
        if id_ in remote:
            if replay:
                return web.json_response(remote[id_])
            return web.json_response({"error": {"message": "already exists"}}, status=409)
        remote[id_] = {**payload, "name": f"{PARENT}/jobs/{id_}"}
        # The server accepted creation, but its response cannot be recovered by the client.
        return web.Response(text="{", content_type="application/json")

    async def get(request: web.Request) -> web.Response:
        if get_status != 200:
            return web.json_response({"error": {"message": "unavailable"}}, status=get_status)
        return web.json_response(remote[request.match_info["job_id"]])

    app = web.Application()
    app.router.add_post(f"/{PARENT}/jobs", create)
    app.router.add_get(f"/{PARENT}/jobs/{{job_id}}", get)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    try:
        yield f"http://{host}:{port}", posted, remote
    finally:
        await runner.cleanup()


@pytest.mark.parametrize("replay", [False, True])
async def test_a_lost_create_reply_resumes_exactly_one_remote_job(replay: bool) -> None:
    async with (
        replay_api(replay=replay) as (root, posted, remote),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchUnavailable, match=f"{PARENT}/jobs/{job_id(JOB)}.*may be running"):
            await jobs.submit(JOB)
        restored = ShardJob.model_validate_json(JOB.model_dump_json())
        assert await jobs.submit(restored) == f"{PARENT}/jobs/{job_id(JOB)}"
    assert len(remote) == 1
    assert posted == [{"jobId": job_id(JOB), "requestId": str(JOB.operation_id)}] * 2
    labels = remote[job_id(JOB)]["labels"]
    assert labels["operation-id"] == JOB.operation_id.hex
    assert len(labels["payload-hash"]) <= 63


async def test_an_unavailable_replay_names_the_possibly_running_job() -> None:
    existing: dict[str, Any] = {
        **batch.job_spec(CONFIG, JOB),
        "name": f"{PARENT}/jobs/{job_id(JOB)}",
    }
    async with (
        replay_api(replay=False, existing=existing, get_status=503) as (root, posted, remote),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchUnavailable, match=f"{PARENT}/jobs/{job_id(JOB)}.*may be running"):
            await jobs.submit(JOB)
    assert len(remote) == len(posted) == 1


@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("mismatch", ["operation", "payload"])
async def test_an_existing_job_with_another_identity_or_payload_is_refused(
    replay: bool, mismatch: str
) -> None:
    existing: dict[str, Any] = {
        **batch.job_spec(CONFIG, JOB),
        "name": f"{PARENT}/jobs/{job_id(JOB)}",
    }
    requested = JOB
    if mismatch == "operation":
        existing["labels"]["operation-id"] = UUID(int=1).hex
    else:
        requested = JOB.model_copy(update={"prefix": "another-prefix"})
    async with (
        replay_api(replay=replay, existing=existing) as (root, posted, remote),
        batch.connect(CONFIG, api_root=root, token=cast(Token, FakeToken())) as jobs,
    ):
        with pytest.raises(BatchRejected, match="different operation or payload"):
            await jobs.submit(requested)
    assert len(remote) == len(posted) == 1


def test_a_new_operation_has_a_new_id_but_a_serialized_retry_keeps_its_id() -> None:
    retry = ShardJob.model_validate_json(JOB.model_dump_json())
    new = ShardJob.model_validate(JOB.model_dump(exclude={"operation_id"}))
    assert job_id(JOB) == job_id(retry)
    assert job_id(JOB) != job_id(new)
    assert len(job_id(JOB)) <= 63


def test_zero_is_not_an_operation_identity() -> None:
    with pytest.raises(ValidationError, match="operation id must not be zero"):
        ShardJob.model_validate({**JOB.model_dump(), "operation_id": UUID(int=0)})
