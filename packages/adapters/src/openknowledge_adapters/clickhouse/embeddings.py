"""Private bounded attempts, atomic immutable publication, and exact ID lookup.

A transport failure never causes an INSERT retry. Retry the whole job with a fresh
iterator/attempt and a finite caller-owned budget. RENAME is the publication point;
ClickHouse refuses an existing destination rather than replacing it. Published tables
must not be modified by other writers. Process loss can leave invisible attempts for
operator cleanup; this lane deliberately has no lease or garbage collection framework.
"""

import json
import struct
from collections.abc import AsyncGenerator, AsyncIterable
from contextlib import aclosing
from typing import Any
from uuid import uuid4

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_contracts.embeddings import (
    EmbeddingLookupResult,
    EmbeddingMutation,
    EmbeddingSnapshot,
    LookupEmbeddings,
    ResolvedVectors,
)
from openknowledge_core.embeddings.models import EmbeddingPublication
from openknowledge_core.embeddings.resolve import resolve_lookup
from openknowledge_core.embeddings.service import validated_records
from openknowledge_core.ingestion.errors import IngestionError, TableRejected

_COLUMNS = "corpusid, sequence, provenance, is_deleted, vector"
_BATCH_SIZE = 1000
_BATCH_BYTES = 4 * 1024 * 1024
_READ_COLUMNS = (
    "corpusid, sequence, provenance, is_deleted, "
    "arrayMap(x -> reinterpretAsUInt32(x), vector) AS vector_bits"
)


def publication_table(source: EmbeddingSnapshot) -> str:
    """Only validated fixed-width hex identity becomes an SQL identifier."""
    return "paper_embeddings_" + source.snapshot.removeprefix("sha256:")


class ClickHouseEmbeddings:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def publish(
        self, descriptor: ResolvedVectors, rows: AsyncIterable[EmbeddingMutation]
    ) -> EmbeddingPublication:
        attempt = "paper_embeddings_attempt_" + uuid4().hex
        target = publication_table(descriptor.source)
        try:
            await self._client.post(f"CREATE TABLE {attempt} AS paper_embeddings_template")
            batch: list[str] = []
            batch_bytes = 0
            async with aclosing(validated_records(descriptor, rows)) as checked:
                async for row in checked:
                    encoded = row.model_dump_json(exclude={"source"})
                    size = len(encoded.encode("utf-8")) + 1
                    if size > _BATCH_BYTES:
                        raise TableRejected("embedding record exceeds the 4 MiB insert budget")
                    if batch and (len(batch) == _BATCH_SIZE or batch_bytes + size > _BATCH_BYTES):
                        await self._insert(attempt, batch)
                        batch = []
                        batch_bytes = 0
                    batch.append(encoded)
                    batch_bytes += size
            if batch:
                await self._insert(attempt, batch)
            # Check the stored Float32 bytes, not just what the input iterator supplied.
            async with aclosing(self._records(attempt, descriptor.source)) as stored:
                async with aclosing(validated_records(descriptor, stored)) as checked:
                    async for _ in checked:
                        pass
            await self._client.post(
                f"ALTER TABLE {attempt} MODIFY COMMENT "
                + _string_literal(descriptor.model_dump_json())
            )
            try:
                await self._client.post(f"RENAME TABLE {attempt} TO {target}")
            except IngestionError:
                existing = await self._descriptor(target)
                if existing is None:
                    raise
                if existing != descriptor:
                    raise TableRejected(
                        "embedding publication already exists with another descriptor"
                    ) from None
            return EmbeddingPublication(descriptor=descriptor, table=target)
        finally:
            # Best effort: preserve the original error; no unbounded cleanup retries.
            try:
                await self._client.post(f"DROP TABLE IF EXISTS {attempt} SYNC")
            except IngestionError:
                pass

    async def lookup(self, command: LookupEmbeddings) -> EmbeddingLookupResult:
        table = publication_table(command.source)
        descriptor = await self._descriptor(table)
        if descriptor is None:
            raise TableRejected("embedding snapshot is not published")
        if descriptor.source != command.source:
            raise TableRejected("embedding publication snapshot metadata mismatch")
        ids = tuple(dict.fromkeys(command.corpusids))
        if not ids:
            return resolve_lookup(command, ())
        rows = await self._client.rows(
            f"SELECT {_READ_COLUMNS} FROM {table} WHERE corpusid IN {{ids:Array(UInt64)}} "
            "ORDER BY corpusid LIMIT 3001",
            ids=json.dumps(ids),
        )
        if len({int(row["corpusid"]) for row in rows}) != len(rows) or len(rows) > len(ids):
            raise TableRejected("published embeddings contain duplicate IDs")
        candidates = tuple(_mutation(row, command.source) for row in rows)
        return resolve_lookup(command, candidates)

    async def _insert(self, table: str, batch: list[str]) -> None:
        await self._client.post(
            f"INSERT INTO {table} ({_COLUMNS}) FORMAT JSONEachRow",
            data="\n".join(batch) + "\n",
        )

    async def _records(
        self, table: str, source: EmbeddingSnapshot
    ) -> AsyncGenerator[EmbeddingMutation]:
        async with aclosing(
            self._client.stream(f"SELECT {_READ_COLUMNS} FROM {table} ORDER BY corpusid")
        ) as rows:
            async for row in rows:
                yield _mutation(row, source)

    async def _descriptor(self, table: str) -> ResolvedVectors | None:
        rows = await self._client.rows(
            "SELECT comment FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=table,
        )
        if not rows:
            return None
        try:
            return ResolvedVectors.model_validate_json(rows[0]["comment"])
        except ValueError:
            raise TableRejected("embedding publication has no valid resolved descriptor") from None


def _mutation(row: dict[str, Any], source: EmbeddingSnapshot) -> EmbeddingMutation:
    # Read IEEE bits: decimal JSON output can round Float32 or lose signed zero.
    values = dict(row)
    values["vector"] = tuple(
        struct.unpack("<f", struct.pack("<I", int(bits)))[0] for bits in values.pop("vector_bits")
    )
    return EmbeddingMutation.model_validate({**values, "source": source})


def _string_literal(value: str) -> str:
    # ClickHouse 26.8 MODIFY COMMENT requires a literal, not a bound parameter.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
