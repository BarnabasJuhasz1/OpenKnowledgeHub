"""OpenSearch over opensearch-py's asyncio client, the only package that speaks its API."""

from __future__ import annotations

from base64 import b64encode
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from opensearchpy import AsyncConnection, AsyncHttpConnection, AsyncOpenSearch

from openknowledge_adapters.opensearch.config import OpenSearchConfig
from openknowledge_adapters.opensearch.sink import INITIAL_BACKOFF, OpenSearchSink
from openknowledge_adapters.opensearch.vectors import OpenSearchVectors


@asynccontextmanager
async def _client(
    config: OpenSearchConfig, connection_class: type[AsyncConnection]
) -> AsyncGenerator[AsyncOpenSearch]:
    """One per-run client; shared auth header avoids deprecated aiohttp auth.

    pydantic retains IPv6 brackets which opensearch-py adds itself.
    """
    url = config.url
    path = url.path or ""
    credentials = f"{config.user}:{config.password.get_secret_value()}".encode()
    client = AsyncOpenSearch(
        hosts=[
            {
                "host": (url.host or "").strip("[]"),
                "port": url.port,
                "use_ssl": url.scheme == "https",
                "url_prefix": "" if path == "/" else path,
            }
        ],
        connection_class=connection_class,
        headers={"Authorization": "Basic " + b64encode(credentials).decode()},
        verify_certs=True,
        http_compress=True,
        timeout=config.timeout_seconds,
    )
    try:
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def connect(
    config: OpenSearchConfig,
    *,
    connection_class: type[AsyncConnection] = AsyncHttpConnection,
    initial_backoff: float = INITIAL_BACKOFF,
) -> AsyncGenerator[OpenSearchSink]:
    async with _client(config, connection_class) as client:
        yield OpenSearchSink(client, initial_backoff=initial_backoff)


@asynccontextmanager
async def connect_vectors(
    config: OpenSearchConfig,
    *,
    connection_class: type[AsyncConnection] = AsyncHttpConnection,
) -> AsyncGenerator[OpenSearchVectors]:
    async with _client(config, connection_class) as client:
        yield OpenSearchVectors(client)


__all__ = ["OpenSearchConfig", "OpenSearchSink", "OpenSearchVectors", "connect", "connect_vectors"]
