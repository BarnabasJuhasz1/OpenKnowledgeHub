"""Queries over ClickHouse's HTTP interface, with its failures mapped to ingestion errors."""

from __future__ import annotations

import json
import uuid
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


class ClickHouseClient:
    def __init__(self, http: httpx.AsyncClient, *, database: str) -> None:
        self._http = http
        self._database = database

    async def rows(self, query: str, **parameters: str) -> list[dict[str, Any]]:
        response = await self.post(f"{query} FORMAT JSONEachRow", **parameters)
        return [json.loads(line) for line in response.text.splitlines()]

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
        params = {
            "database": self._database,
            "query_id": query_id,
            **SETTINGS,
            **{f"param_{name}": value for name, value in parameters.items()},
        }
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


def _error(response: httpx.Response, query_id: str) -> IngestionError:
    code = response.headers.get("X-ClickHouse-Exception-Code", "")
    message = _message(response.text)
    if code == "395":
        return ContractViolation(f"{message}; the object stays unloaded")
    if code == "497":
        return TableAccessDenied("Database query denied with code 497; check account permissions")
    if code == "516":
        return CredentialRejected("ClickHouse refused the user or password")
    failure = f"ClickHouse query {query_id} failed with code {code or response.status_code}"
    if response.status_code in TRANSIENT_STATUSES:
        return TableUnavailable(f"{failure}: {message}; rerun")
    return TableRejected(f"{failure}: {message}")


def _message(text: str) -> str:
    """The first line of ClickHouse's error, without its code, query, stack or version."""
    first = (text.strip().splitlines() or [""])[0]
    message = first.partition("DB::Exception: ")[2] or first
    return message.partition(": while executing")[0].partition(". (")[0][:300]
