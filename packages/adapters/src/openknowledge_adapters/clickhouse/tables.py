"""The loader's side of ClickHouse, over its HTTP interface."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from typing import Any

import httpx

from openknowledge_adapters.clickhouse.load_sql import (
    duplicates_parameters,
    duplicates_query,
    query_parameters,
    sql_sha256,
    upsert_query,
)
from openknowledge_contracts.datasets import DatasetName, ReleaseId
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion.errors import (
    TRANSIENT_STATUSES,
    ContractViolation,
    CredentialRejected,
    IngestionError,
    TableRejected,
    TableUnavailable,
)
from openknowledge_core.ingestion.models import Duplicates, LoadObject, LoadRecord, TableState

SETTINGS = {
    "wait_end_of_query": "1",
    "output_format_json_quote_64bit_integers": "0",
    "date_time_input_format": "best_effort",
    "date_time_output_format": "iso",
}
RECORD_COLUMNS = ", ".join(LoadRecord.model_fields)


class ClickHouseTables:
    def __init__(self, client: httpx.AsyncClient, *, database: str, raw_bucket: str) -> None:
        self._client = client
        self._database = database
        self._raw_bucket = raw_bucket

    async def table(self, dataset: DatasetName) -> TableState:
        contract = _contract(dataset)
        rows = await self._rows(
            "SELECT uuid, total_rows FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=contract.table,
        )
        if not rows:
            raise TableRejected(f"table {contract.table} does not exist; run the migrations")
        return TableState(
            name=contract.table,
            uuid=rows[0]["uuid"],
            has_rows=bool(rows[0]["total_rows"]),
            load_sql_sha256=sql_sha256(upsert_query(contract)),
        )

    async def loaded(self, dataset: DatasetName, source_object: ObjectPath) -> LoadRecord | None:
        rows = await self._rows(
            f"SELECT {RECORD_COLUMNS} FROM loads FINAL "
            "WHERE target = {target:String} AND source_object = {source_object:String}",
            target=_contract(dataset).table,
            source_object=str(source_object),
        )
        return LoadRecord.model_validate(rows[0]) if rows else None

    async def load(self, request: LoadObject) -> int:
        path = self._readable(request.object_ref.path)
        parameters = query_parameters(
            source_file=path.name, release=request.release, source_object=str(path)
        )
        response = await self._post(upsert_query(_contract(request.dataset)), **parameters)
        return int(json.loads(response.headers["X-ClickHouse-Summary"])["written_rows"])

    async def record(self, record: LoadRecord) -> None:
        await self._post("INSERT INTO loads FORMAT JSONEachRow", data=record.model_dump_json())

    async def live_rows(self, dataset: DatasetName, release: ReleaseId) -> int:
        rows = await self._rows(
            "SELECT count() AS rows FROM {table:Identifier} FINAL "
            "WHERE source_release = {release:Date}",
            table=_contract(dataset).table,
            release=release,
        )
        return int(rows[0]["rows"])

    async def duplicates(self, dataset: DatasetName, objects: Sequence[ObjectPath]) -> Duplicates:
        names = [self._readable(path).name for path in objects]
        rows = await self._rows(
            duplicates_query(_contract(dataset), len(names)), **duplicates_parameters(names)
        )
        return Duplicates.model_validate(rows[0])

    def _readable(self, path: ObjectPath) -> ObjectPath:
        if path.bucket != self._raw_bucket:
            raise TableRejected(f"ClickHouse reads only {self._raw_bucket}, not {path}")
        return path

    async def _rows(self, query: str, **parameters: str) -> list[dict[str, Any]]:
        response = await self._post(f"{query} FORMAT JSONEachRow", **parameters)
        return [json.loads(line) for line in response.text.splitlines()]

    async def _post(
        self, query: str, *, data: str | None = None, **parameters: str
    ) -> httpx.Response:
        query_id = str(uuid.uuid4())
        params = {
            "database": self._database,
            "query_id": query_id,
            **SETTINGS,
            **{f"param_{name}": value for name, value in parameters.items()},
        }
        if data is not None:
            params["query"] = query
        try:
            response = await self._client.post(
                "/", params=params, content=query if data is None else data
            )
        except httpx.TransportError as exc:
            raise TableUnavailable(
                f"ClickHouse query {query_id} failed ({type(exc).__name__}); rerun"
            ) from None
        if response.is_success:
            return response
        raise _error(response, query_id)


def _contract(dataset: DatasetName) -> DatasetContract:
    try:
        return CONTRACTS[dataset]
    except KeyError:
        raise TableRejected(f"no table contract for dataset {dataset}") from None


def _error(response: httpx.Response, query_id: str) -> IngestionError:
    code = response.headers.get("X-ClickHouse-Exception-Code", "")
    message = _message(response.text)
    if code == "395":
        return ContractViolation(f"{message}; the object stays unloaded")
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
