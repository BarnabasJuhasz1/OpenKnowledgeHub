"""Google Cloud Batch, which archives each shard on its own short-lived machine."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

import aiohttp
from gcloud.aio.auth import Token

from openknowledge_adapters.batch.config import BatchConfig
from openknowledge_adapters.batch.jobs import CloudBatchJobs, job_spec

BATCH_API = "https://batch.googleapis.com/v1"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


@asynccontextmanager
async def connect(
    config: BatchConfig,
    *,
    api_root: str = BATCH_API,
    token: Token | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> AsyncGenerator[CloudBatchJobs, None]:
    async with aiohttp.ClientSession() as session:
        yield CloudBatchJobs(
            session,
            token or Token(session=session, scopes=SCOPES),
            config,
            api_root=api_root,
            sleep=sleep,
        )


__all__ = ["BatchConfig", "CloudBatchJobs", "connect", "job_spec"]
