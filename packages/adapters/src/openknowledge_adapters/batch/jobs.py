"""Cloud Batch jobs over its REST API, one task per shard running okp s2 download-shard."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

import aiohttp
from gcloud.aio.auth import Token

from openknowledge_adapters.batch.config import BatchConfig
from openknowledge_core.ingestion.errors import (
    TRANSIENT_STATUSES,
    BatchRejected,
    BatchTimedOut,
    BatchUnavailable,
)
from openknowledge_core.ingestion.models import JobOutcome, ShardJob

ENDED = {
    "SUCCEEDED": JobOutcome.SUCCEEDED,
    "FAILED": JobOutcome.FAILED,
    "CANCELLED": JobOutcome.FAILED,
}


class _JobExists(Exception):
    """The job ID holds the operation's UUID, so an earlier attempt of it created the job."""


class CloudBatchJobs:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        token: Token,
        config: BatchConfig,
        *,
        api_root: str,
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._session = session
        self._token = token
        self._config = config
        self._root = api_root
        self._sleep = sleep
        self._parent = f"projects/{config.project}/locations/{config.region}"

    async def submit(self, job: ShardJob) -> str:
        name = f"{self._parent}/jobs/{job_id(job)}"
        try:
            await self._call(
                "POST",
                f"{self._parent}/jobs",
                params={"jobId": job_id(job), "requestId": str(job.operation_id)},
                json=job_spec(self._config, job),
            )
        except _JobExists:
            pass
        except BatchUnavailable as exc:
            raise BatchUnavailable(
                f"Cloud Batch job {name} submission is unconfirmed; it may be running. {exc}"
            ) from None
        return name

    async def running(self) -> list[str]:
        """Download jobs, told apart from other Batch jobs by their operation-id label."""
        names: list[str] = []
        params: dict[str, str] = {}
        while True:
            body = await self._call("GET", f"{self._parent}/jobs", params=params)
            names += [
                str(job["name"])
                for job in body.get("jobs", [])
                if "operation-id" in job.get("labels", {})
                and job.get("status", {}).get("state") not in ENDED
            ]
            if not body.get("nextPageToken"):
                return names
            params = {"pageToken": body["nextPageToken"]}

    async def wait(self, name: str) -> JobOutcome:
        """Bound the whole wait, including failed or stalled polls."""
        try:
            async with asyncio.timeout(self._config.wait_timeout_seconds):
                while True:
                    try:
                        state = (await self._call("GET", name)).get("status", {}).get("state")
                    except BatchUnavailable:
                        state = None
                    if state in ENDED:
                        return ENDED[state]
                    await self._sleep(self._config.poll_seconds)
        except TimeoutError:
            raise BatchTimedOut(
                f"Cloud Batch job {name} did not finish within "
                f"{self._config.wait_timeout_seconds:g}s and may still be running; "
                "every download waits for it, so cancel it if it hangs"
            ) from None

    async def _call(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        try:
            token = await self._token.get()
            headers = {"Authorization": f"Bearer {token}"}
            async with self._session.request(
                method, f"{self._root}/{path}", headers=headers, **kwargs
            ) as response:
                status = response.status
                body = cast(dict[str, Any], await response.json(content_type=None) or {})
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            raise BatchUnavailable(
                f"Cloud Batch did not answer ({type(exc).__name__}); rerun later"
            ) from None
        if status < 300:
            return body
        if status in TRANSIENT_STATUSES:
            raise BatchUnavailable(f"Cloud Batch answered HTTP {status}; rerun later")
        if status == 409 and method == "POST":
            raise _JobExists
        message = str(body.get("error", {}).get("message", "no message"))
        raise BatchRejected(f"Cloud Batch refused {method} {path} with HTTP {status}: {message}")


def job_id(job: ShardJob) -> str:
    """At most 63 lowercase characters, stable for retries of the same operation."""
    part = f"-{job.part}" if job.part else ""
    name = f"s2-{job.dataset.replace('_', '-')}-{job.release_id}{part}"[:30].rstrip("-")
    return f"{name}-{job.operation_id.hex}"


def job_spec(config: BatchConfig, job: ShardJob) -> dict[str, Any]:
    command = ["s2", "download-shard", "--release", job.release_id, "--dataset", job.dataset]
    command += ["--bucket", job.bucket, "--prefix", job.prefix]
    if job.from_release is not None and job.part is not None:
        command += ["--from-release", job.from_release, "--part", job.part]
    return {
        "taskGroups": [
            {
                # One task per entry; okp reads its shard from OKP_SHARD.
                "taskEnvironments": [{"variables": {"OKP_SHARD": str(s)}} for s in job.shards],
                # One task at a time: each lists the dataset with the S2 key, which allows
                # one request per second.
                "parallelism": 1,
                "taskSpec": {
                    "runnables": [{"container": {"imageUri": config.image, "commands": command}}],
                    "environment": {
                        "variables": config.environment,
                        "secretVariables": config.secrets,
                    },
                    "maxRunDuration": f"{config.max_run_seconds}s",
                },
            }
        ],
        "allocationPolicy": {
            "serviceAccount": {"email": config.service_account},
            "instances": [
                {
                    "policy": {
                        "machineType": config.machine_type,
                        "provisioningModel": config.provisioning_model,
                    }
                }
            ],
            "network": {
                "networkInterfaces": [{"network": config.network, "subnetwork": config.subnetwork}]
            },
        },
        "labels": {
            "dataset": job.dataset.replace("_", "-"),
            "release": job.release_id,
            "part": job.part or "base",
            "operation-id": job.operation_id.hex,
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
    }
