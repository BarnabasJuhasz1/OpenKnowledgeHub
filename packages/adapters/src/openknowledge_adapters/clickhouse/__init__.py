"""ClickHouse over HTTP, the only package that sends SQL."""

from __future__ import annotations

import socket
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx

from openknowledge_adapters.clickhouse.config import ClickHouseConfig
from openknowledge_adapters.clickhouse.tables import ClickHouseTables

# An insert sends nothing back until it ends, and GCP drops a connection idle for 10 minutes.
KEEPALIVE = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),
]


@asynccontextmanager
async def connect(
    config: ClickHouseConfig, *, transport: httpx.AsyncBaseTransport | None = None
) -> AsyncGenerator[ClickHouseTables, None]:
    client = httpx.AsyncClient(
        base_url=str(config.url),
        headers={
            "X-ClickHouse-User": config.user,
            "X-ClickHouse-Key": config.password.get_secret_value(),
        },
        timeout=httpx.Timeout(config.timeout_seconds, connect=10.0),
        follow_redirects=False,
        transport=transport or httpx.AsyncHTTPTransport(socket_options=KEEPALIVE),
    )
    async with client:
        yield ClickHouseTables(client, database=config.database, raw_bucket=config.raw_bucket)


__all__ = ["ClickHouseConfig", "ClickHouseTables", "connect"]
