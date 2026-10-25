"""Opt-in real HTTP server boundary; never starts a server or uses cloud resources.

Run with CLICKHOUSE_EMBEDDINGS_TEST_URL=http://127.0.0.1:18124 and optional
CLICKHOUSE_EMBEDDINGS_TEST_USER / CLICKHOUSE_EMBEDDINGS_TEST_PASSWORD.
The account must create/drop a tiny isolated database and read system.tables.
"""

import os
import struct
from collections.abc import AsyncGenerator
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from openknowledge_adapters.clickhouse import ClickHouseConfig, connect
from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.embeddings import ClickHouseEmbeddings
from openknowledge_contracts.embeddings import (
    EmbeddingMutation,
    EmbeddingSnapshot,
    LookupEmbeddings,
    ResolvedVectors,
)
from openknowledge_core.embeddings.serialization import vector_record_bytes
from openknowledge_core.embeddings.service import publish_embeddings
from openknowledge_core.ingestion.errors import ContractViolation, TableRejected, TableUnavailable

MIGRATION = (
    Path(__file__).resolve().parents[4]
    / "migrations/clickhouse/00016_create_embedding_template.sql"
)
MIGRATION_UP = MIGRATION.read_text().split("-- +goose Up\n")[1].split("-- +goose Down")[0]

SOURCE = EmbeddingSnapshot(
    snapshot="sha256:" + "a" * 64, release="2026-10-06", model_revision="fixture:encoder-v1"
)


def mutation(corpusid: int, *, deleted: bool = False) -> EmbeddingMutation:
    # Signed zero, subnormal, rounding and largest finite Float32 are all retained.
    vector = (0.1, -0.0, 2**-149, -(2**-149), (2 - 2**-23) * 2**127) + (0.0,) * 763
    return EmbeddingMutation(
        source=SOURCE,
        corpusid=corpusid,
        sequence=2**64 - 1,
        provenance="fixture:雪\nrow",
        is_deleted=deleted,
        vector=() if deleted else vector,
    )


def descriptor(rows: tuple[EmbeddingMutation, ...]) -> ResolvedVectors:
    return ResolvedVectors(
        source=rows[0].source if rows else SOURCE,
        records=len(rows),
        live_records=sum(not row.is_deleted for row in rows),
        sha256="sha256:" + sha256(b"".join(vector_record_bytes(row) for row in rows)).hexdigest(),
        resolution_evidence="fixture:resolved",
        completeness_evidence="fixture:complete",
        space_evidence="fixture:space",
    )


async def stream(rows: tuple[EmbeddingMutation, ...]) -> AsyncGenerator[EmbeddingMutation]:
    for row in rows:
        yield row


class _FailureClient(ClickHouseClient):
    fail_after: str | None = None

    async def post(
        self,
        query: str,
        *,
        data: str | None = None,
        query_id: str | None = None,
        **parameters: str,
    ) -> httpx.Response:
        response = await super().post(query, data=data, query_id=query_id, **parameters)
        if self.fail_after is not None and query.startswith(self.fail_after):
            self.fail_after = None
            raise TableUnavailable("fixture: response lost after successful server write")
        return response


@pytest.fixture
async def server() -> AsyncGenerator[tuple[ClickHouseEmbeddings, _FailureClient]]:
    url = os.environ.get("CLICKHOUSE_EMBEDDINGS_TEST_URL")
    if url is None:
        pytest.skip("set CLICKHOUSE_EMBEDDINGS_TEST_URL to the isolated local HTTP server")
    parsed = httpx.URL(url)
    if parsed.host != "127.0.0.1" or parsed.port != 18124:
        pytest.fail("embedding integration tests require isolated 127.0.0.1:18124")
    database = "embedding_lookup_test_" + uuid4().hex
    config = ClickHouseConfig.model_validate(
        dict(
            url=url,
            user=os.environ.get("CLICKHOUSE_EMBEDDINGS_TEST_USER", "default"),
            password=os.environ.get("CLICKHOUSE_EMBEDDINGS_TEST_PASSWORD", ""),
            database=database,
            raw_bucket="fixture-raw",
            timeout_seconds=30,
        )
    )
    async with httpx.AsyncClient(
        base_url=url,
        headers={
            "X-ClickHouse-User": config.user,
            "X-ClickHouse-Key": config.password.get_secret_value(),
        },
        timeout=30,
        trust_env=False,
    ) as http:
        admin = ClickHouseClient(http, database="default")
        await admin.post(f"CREATE DATABASE {database} ENGINE = Atomic")
        try:
            client = _FailureClient(http, database=database)
            # Idempotent migration, then the packaged connection entrypoint.
            await client.post(MIGRATION_UP)
            await client.post(MIGRATION_UP)
            async with connect(config) as connection:
                yield connection.embeddings, client
        finally:
            await admin.post(f"DROP DATABASE {database} SYNC")


async def test_publish_replay_delete_lookup_and_identity(
    server: tuple[ClickHouseEmbeddings, _FailureClient],
) -> None:
    lane, _ = server
    rows = (mutation(0), mutation(7, deleted=True), mutation(2**64 - 1))
    expected = descriptor(rows)
    receipt = await publish_embeddings(lane, expected, stream(rows))
    assert await publish_embeddings(lane, expected, stream(rows)) == receipt
    result = await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(2**64 - 1, 7, 100, 0, 0)))
    assert result.missing == (7, 100)
    assert result.found == (rows[2], rows[0])
    assert struct.pack("<768f", *result.found[0].vector) == struct.pack("<768f", *rows[2].vector)
    changed = SOURCE.model_copy(update={"model_revision": "fixture:other"})
    with pytest.raises(TableRejected, match="metadata mismatch"):
        await lane.lookup(LookupEmbeddings(source=changed, corpusids=(0,)))
    changed_rows = (rows[0].model_copy(update={"provenance": "different"}), *rows[1:])
    with pytest.raises(TableRejected, match="another descriptor"):
        await lane.publish(descriptor(changed_rows), stream(changed_rows))
    assert (await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(0,)))).found == (rows[0],)


async def test_incomplete_wrong_digest_and_duplicates_never_publish(
    server: tuple[ClickHouseEmbeddings, _FailureClient],
) -> None:
    lane, client = server
    rows = tuple(mutation(i, deleted=True) for i in range(1002))
    expected = descriptor(rows)
    with pytest.raises(ContractViolation, match="count or digest"):
        await lane.publish(expected, stream(rows[:-1]))
    with pytest.raises(ContractViolation, match="count or digest"):
        await lane.publish(
            expected.model_copy(update={"sha256": "sha256:" + "0" * 64}), stream(rows)
        )
    with pytest.raises(ContractViolation, match="unique and strictly increasing"):
        await lane.publish(expected, stream((rows[0], rows[0])))
    with pytest.raises(TableRejected, match="not published"):
        await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(0,)))
    assert (
        await client.rows(
            "SELECT name FROM system.tables WHERE database = currentDatabase() "
            "AND name LIKE 'paper_embeddings_attempt_%'"
        )
        == []
    )
    await lane.publish(expected, stream(rows))
    assert (await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(0,)))).missing == (0,)


async def test_empty_publication(server: tuple[ClickHouseEmbeddings, _FailureClient]) -> None:
    lane, _ = server
    await lane.publish(descriptor(()), stream(()))
    assert (await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(0,)))).missing == (0,)


async def test_descriptor_literal_roundtrip(
    server: tuple[ClickHouseEmbeddings, _FailureClient],
) -> None:
    lane, client = server
    special = "fixture:雪 'quoted' \\path\nnext\r\t; DROP TABLE ignored; --"
    source = EmbeddingSnapshot.model_validate({**SOURCE.model_dump(), "model_revision": special})
    row = EmbeddingMutation.model_validate({**mutation(1).model_dump(), "source": source})
    expected = ResolvedVectors.model_validate(
        {
            **descriptor((row,)).model_dump(),
            "resolution_evidence": special,
            "completeness_evidence": special,
            "space_evidence": special,
        }
    )
    receipt = await lane.publish(expected, stream((row,)))
    stored = await client.rows(
        "SELECT comment FROM system.tables "
        "WHERE database = currentDatabase() AND name = {table:String}",
        table=receipt.table,
    )
    assert ResolvedVectors.model_validate_json(stored[0]["comment"]) == expected
    assert await lane.publish(expected, stream((row,))) == receipt
    assert (await lane.lookup(LookupEmbeddings(source=source, corpusids=(1,)))).found == (row,)


async def test_ambiguous_writes_are_invisible_or_equal_publications(
    server: tuple[ClickHouseEmbeddings, _FailureClient],
) -> None:
    _, client = server
    lane = ClickHouseEmbeddings(client)
    rows = tuple(mutation(i, deleted=True) for i in range(1002))
    expected = descriptor(rows)
    # Execute a real insert, then simulate losing its successful HTTP response.
    client.fail_after = "INSERT INTO"
    with pytest.raises(TableUnavailable, match="response lost"):
        await lane.publish(expected, stream(rows))
    with pytest.raises(TableRejected, match="not published"):
        await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(0,)))
    # The caller retries the complete job. A successful rename with a lost response
    # is recognized from the immutable publication rather than creating a new identity.
    client.fail_after = "RENAME TABLE"
    receipt = await lane.publish(expected, stream(rows))
    assert await lane.publish(expected, stream(rows)) == receipt
    assert (await lane.lookup(LookupEmbeddings(source=SOURCE, corpusids=(0,)))).missing == (0,)
