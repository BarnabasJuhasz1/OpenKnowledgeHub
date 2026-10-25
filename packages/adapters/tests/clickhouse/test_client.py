import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_core.ingestion.errors import TableUnavailable

TAG = "gdnpmnofgbbteqgn"
EXCEPTION = (
    "Code: 241. DB::Exception: Query memory limit exceeded: would use 66.67 MiB, maximum: "
    "9.54 MiB. (MEMORY_LIMIT_EXCEEDED) (version 26.8.22.13 (official build))"
)
# Two rows as the e2e server sends them: U+0085 raw, and a stored byte that is not UTF-8.
ROWS = b'{"number":4,"title":"a\xc2\x85b"}\n{"number":5,"title":"caf\xe9"}\n'
# After a 200, the exception comes framed; before the headers, it follows the rows held back.
FRAMED = f"\r\n__exception__\r\n{TAG}\r\n{EXCEPTION}\n{len(EXCEPTION)} {TAG}\r\n__exception__\r\n"
STREAMED = [{"number": 4, "title": "a\x85b"}, {"number": 5, "title": "caf\ufffd"}]
MEMORY = "failed with code 241: Query memory limit exceeded: would use 66.67 MiB, maximum: 9.54 MiB"


@pytest.mark.parametrize(
    ("status", "body", "streamed", "message"),
    [
        (200, ROWS + FRAMED.encode(), STREAMED, MEMORY),
        (500, ROWS + f"{EXCEPTION}\n".encode(), [], MEMORY),
        # An answer from the wrong host that echoes the request is named, never repeated.
        (
            500,
            b'{"echo":1}\nX-ClickHouse-Key: hunter2\n',
            [],
            "failed with code 241: an answer that is not ClickHouse's",
        ),
    ],
    ids=["after the 200", "before the headers", "an echo"],
)
async def test_a_stream_yields_each_row_and_maps_the_failure_that_ends_it(
    status: int, body: bytes, streamed: list[dict[str, Any]], message: str
) -> None:
    asked: list[httpx.Request] = []

    async def pieces() -> AsyncIterator[bytes]:
        for start in range(len(body)):  # a byte at a time, so rows and U+0085 are cut apart
            yield body[start : start + 1]

    def handle(request: httpx.Request) -> httpx.Response:
        asked.append(request)
        headers = {"X-ClickHouse-Exception-Tag": TAG}
        if status != 200:
            headers["X-ClickHouse-Exception-Code"] = "241"
        return httpx.Response(status, headers=headers, content=pieces())

    rows: list[dict[str, Any]] = []
    transport = httpx.MockTransport(handle)
    async with httpx.AsyncClient(base_url="http://clickhouse:8123", transport=transport) as http:
        with pytest.raises(TableUnavailable) as raised:
            async for row in ClickHouseClient(http, database="openknowledge").stream(
                "SELECT {low:UInt64}", low="4"
            ):
                rows.append(row)
    assert rows == streamed
    params = asked[0].url.params
    assert params["wait_end_of_query"] == params["http_write_exception_in_output_format"] == "0"
    assert params["param_low"] == "4"
    assert asked[0].content == b"SELECT {low:UInt64} FORMAT JSONEachRow"
    assert str(raised.value) == f"ClickHouse query {params['query_id']} {message}; rerun"


async def test_a_stream_yields_a_row_before_the_body_has_ended() -> None:
    first_row_out = asyncio.Event()

    async def body() -> AsyncIterator[bytes]:
        yield b'{"number":1}\n'
        # The second row exists only once the first has reached the caller, so a client that reads
        # the body whole before it yields times out here instead of hanging.
        await asyncio.wait_for(first_row_out.wait(), timeout=1)
        yield b'{"number":2}\n'

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body())

    rows: list[dict[str, Any]] = []
    transport = httpx.MockTransport(handle)
    async with httpx.AsyncClient(base_url="http://clickhouse:8123", transport=transport) as http:
        async for row in ClickHouseClient(http, database="openknowledge").stream("SELECT 1"):
            rows.append(row)
            first_row_out.set()
    assert rows == [{"number": 1}, {"number": 2}]
