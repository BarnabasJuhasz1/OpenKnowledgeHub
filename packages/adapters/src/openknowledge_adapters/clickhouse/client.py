"""Queries over ClickHouse's HTTP interface, with its failures mapped to ingestion errors."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import aclosing
from typing import Any

import httpx

from openknowledge_core.ingestion.errors import (
    TRANSIENT_STATUSES,
    ContractViolation,
    CredentialRejected,
    IngestionError,
    TableAccessDenied,
    TableRejected,
    TableUnavailable,
)

SETTINGS = {
    "wait_end_of_query": "1",
    "output_format_json_quote_64bit_integers": "0",
    "date_time_input_format": "best_effort",
    "date_time_output_format": "iso",
}
# What a stream sends on top of SETTINGS: an answer that begins before the query ends, and a
# later failure written as framed text, not as a row, whatever the server's default.
STREAM_SETTINGS = {"wait_end_of_query": "0", "http_write_exception_in_output_format": "0"}
# How ClickHouse's exception text starts, with its code and the class of the exception.
_EXCEPTION = re.compile(r"Code: (\d{1,5})\. DB::\w*Exception: ")
# The lines before the exception text in a body that ClickHouse began with a 200: the line that
# broke off, a line of __exception__ and one with the tag of X-ClickHouse-Exception-Tag.
_FRAMING = 3
NOT_CLICKHOUSE = "an answer that is not ClickHouse's"


class ClickHouseClient:
    def __init__(self, http: httpx.AsyncClient, *, database: str) -> None:
        self._http = http
        self._database = database

    async def rows(self, query: str, **parameters: str) -> list[dict[str, Any]]:
        response = await self.post(f"{query} FORMAT JSONEachRow", **parameters)
        return [json.loads(line) for line in response.text.splitlines()]

    async def stream(self, query: str, **parameters: str) -> AsyncGenerator[dict[str, Any]]:
        """The query's rows one at a time, as ClickHouse sends them, never the whole result.

        With ``wait_end_of_query`` off, ClickHouse answers while the query runs. A failure
        before its headers are out gets an error status, its text after the rows held back,
        and one after a 200 takes the place of the next row, framed by ``__exception__`` lines
        and a tag; either is mapped as ``rows`` maps a failed query, the second as if its
        status were 500, repeating only a line that is ClickHouse's exception text. The body
        is split at newlines alone, since a JSON row escapes every other line break but
        U+0085, which ``str.splitlines`` also splits at, and decoded as ``rows`` decodes it,
        a byte that is not UTF-8 replaced.
        """
        query_id = str(uuid.uuid4())
        params = {**self._params(query_id, parameters), **STREAM_SETTINGS}
        content = f"{query} FORMAT JSONEachRow"
        text: str | None = None
        try:
            async with self._http.stream("POST", "/", params=params, content=content) as response:
                async with aclosing(_lines(response)) as lines:
                    broken: str | None = None
                    async for line in lines:
                        decoded = line.decode(errors="replace")
                        try:
                            row = json.loads(decoded)
                        except (ValueError, RecursionError):
                            broken = decoded
                            break
                        if response.is_success:
                            yield row
                    if broken is not None:  # read on outside that handler, so nothing chains to it
                        text = await _exception(broken, lines)
        except httpx.RequestError as exc:  # a transport error, or a body that does not decode
            error: IngestionError = TableUnavailable(
                f"ClickHouse query {query_id} failed ({type(exc).__name__}); rerun"
            )
        else:
            if not response.is_success:
                code = response.headers.get("X-ClickHouse-Exception-Code", "")
                error = _failure(response.status_code, code, text or NOT_CLICKHOUSE, query_id)
            elif text is not None:
                # As a 500, the status of a failure that comes while the query runs, out of memory
                # or cancelled; one that ClickHouse answers 400 before the headers, a parse or
                # argument error, is permanent in rows and transient here.
                found = _EXCEPTION.match(text)
                error = _failure(500, found[1] if found else "", text, query_id)
            else:
                return
        raise error  # outside the handler, so the request with its key is not the context

    async def written(self, query: str, *, query_id: str | None = None, **parameters: str) -> int:
        """Run an insert and return the rows it wrote."""
        response = await self.post(query, query_id=query_id, **parameters)
        return int(json.loads(response.headers["X-ClickHouse-Summary"])["written_rows"])

    async def post(
        self,
        query: str,
        *,
        data: str | None = None,
        query_id: str | None = None,
        **parameters: str,
    ) -> httpx.Response:
        query_id = query_id or str(uuid.uuid4())
        params = self._params(query_id, parameters)
        if data is not None:
            params["query"] = query
        try:
            response = await self._http.post(
                "/", params=params, content=query if data is None else data
            )
        except httpx.TransportError as exc:
            raise TableUnavailable(
                f"ClickHouse query {query_id} failed ({type(exc).__name__}); rerun"
            ) from None
        if response.is_success:
            return response
        raise _error(response, query_id)

    def _params(self, query_id: str, parameters: Mapping[str, str]) -> dict[str, str]:
        return {
            "database": self._database,
            "query_id": query_id,
            **SETTINGS,
            **{f"param_{name}": value for name, value in parameters.items()},
        }


async def _lines(response: httpx.Response) -> AsyncGenerator[bytes]:
    """The body's lines, split at the newline byte alone, each joined once from its chunks."""
    pending: list[bytes] = []
    async for chunk in response.aiter_bytes():
        *lines, rest = chunk.split(b"\n")
        for line in lines:
            yield b"".join([*pending, line])
            pending = []
        pending.append(rest)
    if tail := b"".join(pending):
        yield tail


async def _exception(line: str, lines: AsyncIterator[bytes]) -> str:
    """The first line of ClickHouse's exception text, from ``line`` on, which is not a row.

    Any other line is named, never repeated, since an answer from the wrong host could echo
    the request.
    """
    for _ in range(_FRAMING):
        if _EXCEPTION.match(line):
            return line
        found = await anext(lines, None)
        if found is None:
            break
        line = found.decode(errors="replace")
    return line if _EXCEPTION.match(line) else NOT_CLICKHOUSE


def _error(response: httpx.Response, query_id: str) -> IngestionError:
    code = response.headers.get("X-ClickHouse-Exception-Code", "")
    return _failure(response.status_code, code, response.text, query_id)


def _failure(status: int, code: str, text: str, query_id: str) -> IngestionError:
    message = _message(text)
    if code == "395":
        return ContractViolation(f"{message}; the object stays unloaded")
    if code == "497":
        return TableAccessDenied("Database query denied with code 497; check account permissions")
    if code == "516":
        return CredentialRejected("ClickHouse refused the user or password")
    failure = f"ClickHouse query {query_id} failed with code {code or status}"
    if status in TRANSIENT_STATUSES:
        return TableUnavailable(f"{failure}: {message}; rerun")
    return TableRejected(f"{failure}: {message}")


def _message(text: str) -> str:
    """The first line of ClickHouse's error, without its code, query, stack or version."""
    first = (text.strip().splitlines() or [""])[0]
    message = first.partition("DB::Exception: ")[2] or first
    return message.partition(": while executing")[0].partition(". (")[0][:300]
