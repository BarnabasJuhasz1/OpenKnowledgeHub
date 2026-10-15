"""ClickHouse over HTTP, the only package that sends SQL."""

from __future__ import annotations

import socket
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import NamedTuple

import httpx

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.config import ClickHouseConfig
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables

# An insert sends nothing back until it ends, and GCP drops a connection idle for 10 minutes.
KEEPALIVE = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),
]


class ClickHouse(NamedTuple):
    tables: ClickHouseTables
    applied: ClickHouseAppliedReleases


@asynccontextmanager
async def connect(
    config: ClickHouseConfig, *, transport: httpx.AsyncBaseTransport | None = None
) -> AsyncGenerator[ClickHouse, None]:
    http = httpx.AsyncClient(
        base_url=str(config.url),
        headers={
            "X-ClickHouse-User": config.user,
            "X-ClickHouse-Key": config.password.get_secret_value(),
        },
        timeout=httpx.Timeout(config.timeout_seconds, connect=10.0),
        follow_redirects=False,
        transport=transport or httpx.AsyncHTTPTransport(socket_options=KEEPALIVE),
    )
    async with http:
        client = ClickHouseClient(http, database=config.database)
        yield ClickHouse(
            tables=ClickHouseTables(client, raw_bucket=config.raw_bucket),
            applied=ClickHouseAppliedReleases(client),
        )


__all__ = [
    "ClickHouse",
    "ClickHouseAppliedReleases",
    "ClickHouseConfig",
    "ClickHouseTables",
    "connect",
]
