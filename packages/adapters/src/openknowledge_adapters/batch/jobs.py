"""Cloud Batch jobs over its REST API, one task per shard running okp s2 download-shard."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

import aiohttp
from gcloud.aio.auth import Token

from openknowledge_adapters.batch.config import BatchConfig
from openknowledge_core.ingestion.errors import TRANSIENT_STATUSES, BatchRejected, BatchUnavailable
from openknowledge_core.ingestion.models import JobOutcome, ShardJob

ENDED = {
    "SUCCEEDED": JobOutcome.SUCCEEDED,
    "FAILED": JobOutcome.FAILED,
    "CANCELLED": JobOutcome.FAILED,
}


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

    async def submit(self, job: ShardJob) -> str:
        parent = f"projects/{self._config.project}/locations/{self._config.region}"
        body = await self._call(
            "POST",
            f"{parent}/jobs",
            params={"jobId": job_id(job)},
            json=job_spec(self._config, job),
        )
        return str(body["name"])

    async def wait(self, name: str) -> JobOutcome:
        """Polls until the job ends, through any number of failed polls."""
        while True:
            try:
                state = (await self._call("GET", name)).get("status", {}).get("state")
            except BatchUnavailable:
                state = None
            if state in ENDED:
                return ENDED[state]
            await self._sleep(self._config.poll_seconds)

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
        message = str(body.get("error", {}).get("message", "no message"))
        raise BatchRejected(f"Cloud Batch refused {method} {path} with HTTP {status}: {message}")


def job_id(job: ShardJob) -> str:
    """Lowercase letters, digits and hyphens, at most 63, and new for every submission."""
    name = f"s2-{job.dataset.replace('_', '-')}-{job.release_id}"[:54]
    return f"{name}-{uuid.uuid4().hex[:8]}"


def job_spec(config: BatchConfig, job: ShardJob) -> dict[str, Any]:
    command = ["s2", "download-shard", "--release", job.release_id, "--dataset", job.dataset]
    command += ["--bucket", job.bucket, "--prefix", job.prefix]
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
            "instances": [{"policy": {"machineType": config.machine_type}}],
            "network": {
                "networkInterfaces": [{"network": config.network, "subnetwork": config.subnetwork}]
            },
        },
        "labels": {"dataset": job.dataset.replace("_", "-"), "release": job.release_id},
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
    }
