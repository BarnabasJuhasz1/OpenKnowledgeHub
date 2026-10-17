"""Semantic Scholar Datasets API: release metadata, dataset listings and signed shard links."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import NamedTuple

import httpx

from openknowledge_adapters.semantic_scholar.catalog import SemanticScholarCatalog
from openknowledge_adapters.semantic_scholar.config import SemanticScholarConfig
from openknowledge_adapters.semantic_scholar.downloads import (
    SignedLinkDownloads,
    install_log_redaction,
)
from openknowledge_adapters.semantic_scholar.rate_limit import AsyncRateLimiter


class SemanticScholar(NamedTuple):
    catalog: SemanticScholarCatalog
    downloads: SignedLinkDownloads


@asynccontextmanager
async def connect(
    config: SemanticScholarConfig,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    uniform: Callable[[float, float], float] = random.uniform,
) -> AsyncGenerator[SemanticScholar, None]:
    """Open the API and file-host clients for one run, with keyword arguments as test seams."""
    install_log_redaction()
    # httpx keeps x-api-key on a redirect to another host, so the API client follows none.
    api = httpx.AsyncClient(
        base_url=str(config.api_url),
        timeout=config.timeout_seconds,
        follow_redirects=False,
        transport=transport,
    )
    files = httpx.AsyncClient(
        timeout=httpx.Timeout(
            config.download_read_timeout_seconds, connect=config.download_connect_timeout_seconds
        ),
        follow_redirects=False,
        transport=transport,
    )
    async with api, files:
        limiter = AsyncRateLimiter(config.request_interval_seconds, clock=clock, sleep=sleep)
        catalog = SemanticScholarCatalog(
            api,
            limiter,
            api_key=config.api_key,
            attempts=config.attempts,
            backoff=config.backoff_seconds,
            sleep=sleep,
            uniform=uniform,
        )
        yield SemanticScholar(catalog=catalog, downloads=SignedLinkDownloads(files))


__all__ = ["SemanticScholar", "SemanticScholarCatalog", "SemanticScholarConfig", "connect"]
