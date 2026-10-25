"""Opt-in synthetic archive through conversion/resolution into both actual stores.

Only manager-owned loopback engines are used. This test owns one fresh database
and physical vector index; it neither manages services nor selects production data.
"""

from __future__ import annotations

import os
import struct
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from opensearchpy import AsyncOpenSearch

from openknowledge_adapters.clickhouse import ClickHouseConfig, connect
from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.opensearch.vectors import OpenSearchVectors
from openknowledge_adapters.resolved.parquet import resolve_parquet_vectors
from openknowledge_adapters.resolved.store import iter_resolved_vectors, stream_resolved_vectors
from openknowledge_contracts.embeddings import EmbeddingMutation, LookupEmbeddings, ResolvedVectors
from openknowledge_core.embeddings.service import publish_embeddings
from openknowledge_core.search.vector_models import SearchVectorNeighbors, VectorPublication
from openknowledge_core.search.vectors import VectorPublicationService
from tests.test_resolved_bridge import fixture_archive

pytestmark = pytest.mark.skipif(
    os.environ.get("OKP_EMBEDDING_STORES_INTEGRATION") != "1",
    reason="requires explicit opt-in to manager-owned ClickHouse18124 and OpenSearch19203",
)

MIGRATION = Path(__file__).resolve().parents[1] / (
    "migrations/clickhouse/00016_create_embedding_template.sql"
)
MIGRATION_UP = MIGRATION.read_text().split("-- +goose Up\n")[1].split("-- +goose Down")[0]


async def test_archive_resolved_artifact_publishes_identically_to_both_stores(
    tmp_path: Path,
) -> None:
    # The reused fixture executes real archive/download/completion and packaged
    # conversion against tiny in-memory source/store ports, then reads real Parquet.
    store, plan, selections, parquet = await fixture_archive(tmp_path)
    directory = tmp_path / "resolved"
    artifact = await resolve_parquet_vectors(plan, selections, store, parquet, directory)
    rows = tuple(iter_resolved_vectors(directory, artifact))
    maximum = 2**64 - 1
    assert artifact.plan.scope == "bounded-sample"
    assert artifact.input_verification == "receipt-backed"
    assert artifact.input_records == 8  # Base4 (duplicate7), updates2, deletes2 (duplicate7).
    assert artifact.resolved.records == 4 and artifact.resolved.live_records == 3
    assert [(row.corpusid, row.sequence, row.is_deleted) for row in rows] == [
        (0, 0, False),
        (7, 1, True),
        (8, 1, False),
        (maximum, 1, False),
    ]
    base_float32 = struct.unpack("<f", struct.pack("<f", 0.1))[0]
    updated_float32 = struct.unpack("<f", struct.pack("<f", 0.2))[0]
    assert base_float32 != 0.1 and updated_float32 != 0.2
    assert rows[0].vector == (base_float32,) * 768
    assert rows[1].vector == ()
    assert rows[2].vector == rows[3].vector == (updated_float32,) * 768

    database = "embedding_stores_test_" + uuid4().hex
    publication = VectorPublication(
        index="paper-vectors-" + uuid4().hex, resolved=artifact.resolved
    )
    config = ClickHouseConfig.model_validate(
        dict(
            url="http://127.0.0.1:18124",
            user="default",
            password="",
            database=database,
            raw_bucket="fixture-raw",
            timeout_seconds=30,
        )
    )
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:18124",
        headers={"X-ClickHouse-User": "default"},
        timeout=30,
        trust_env=False,
    ) as http:
        admin = ClickHouseClient(http, database="default")
        await admin.post(f"CREATE DATABASE {database} ENGINE = Atomic")
        try:
            sql = ClickHouseClient(http, database=database)
            await sql.post(MIGRATION_UP)
            async with (
                connect(config) as clickhouse,
                AsyncOpenSearch(
                    hosts=["http://127.0.0.1:19203"],
                    timeout=3,
                    max_retries=0,
                    retry_on_timeout=False,
                ) as opensearch,
            ):
                # Fail before creating an index if the reserved endpoint is unavailable.
                await opensearch.info()
                try:
                    receipt = await publish_embeddings(
                        clickhouse.embeddings,
                        artifact.resolved,
                        stream_resolved_vectors(directory, artifact),
                    )
                    service = VectorPublicationService(OpenSearchVectors(opensearch))
                    assert (
                        await service.publish(
                            publication, stream_resolved_vectors(directory, artifact)
                        )
                        == publication
                    )
                    assert receipt.descriptor == publication.resolved == artifact.resolved
                    assert receipt.descriptor.source == plan.source

                    # A whole-job CH replay reads the same artifact and recognizes
                    # the immutable descriptor instead of duplicating stored records.
                    assert (
                        await publish_embeddings(
                            clickhouse.embeddings,
                            artifact.resolved,
                            stream_resolved_vectors(directory, artifact),
                        )
                        == receipt
                    )
                    lookup = await clickhouse.embeddings.lookup(
                        LookupEmbeddings(
                            source=plan.source, corpusids=(maximum, 7, 99, 0, 8, maximum)
                        )
                    )
                    assert lookup.source == plan.source
                    assert lookup.found == (rows[3], rows[0], rows[2])
                    assert lookup.missing == (7, 99)
                    ch_rows = await sql.rows(
                        "SELECT corpusid, sequence, is_deleted, length(vector) AS dimension "
                        f"FROM {receipt.table} ORDER BY corpusid"
                    )
                    assert [
                        (
                            int(row["corpusid"]),
                            int(row["sequence"]),
                            bool(row["is_deleted"]),
                            int(row["dimension"]),
                        )
                        for row in ch_rows
                    ] == [
                        (row.corpusid, row.sequence, row.is_deleted, len(row.vector))
                        for row in rows
                    ]
                    comment = await sql.rows(
                        "SELECT comment FROM system.tables WHERE database=currentDatabase() "
                        "AND name={table:String}",
                        table=receipt.table,
                    )
                    assert (
                        ResolvedVectors.model_validate_json(comment[0]["comment"])
                        == artifact.resolved
                    )

                    assert (
                        (await opensearch.count(index=publication.index))["count"]
                        == len(ch_rows)
                        == 4
                    )
                    assert (
                        (
                            await opensearch.count(
                                index=publication.index,
                                body={"query": {"term": {"is_deleted": False}}},
                            )
                        )["count"]
                        == len(lookup.found)
                        == 3
                    )
                    os_rows: list[EmbeddingMutation] = []
                    for expected in rows:
                        document = (
                            await opensearch.get(index=publication.index, id=str(expected.corpusid))
                        )["_source"]
                        actual = EmbeddingMutation.model_validate(document)
                        assert actual == expected
                        if actual.is_deleted:
                            assert "vector" not in document
                        os_rows.append(actual)
                    assert {row.corpusid for row in os_rows if row.is_deleted} == {7}
                    for found in lookup.found:
                        os_row = next(row for row in os_rows if row.corpusid == found.corpusid)
                        assert found.source == os_row.source == plan.source
                        assert struct.pack("<768f", *found.vector) == struct.pack(
                            "<768f", *os_row.vector
                        )
                    neighbors = await service.neighbors(
                        SearchVectorNeighbors(
                            publication=publication,
                            expected_source=lookup.source,
                            seed=next(row for row in lookup.found if row.corpusid == 0),
                            k=3,
                        )
                    )
                    assert neighbors.publication.resolved == receipt.descriptor
                    assert {hit.corpusid for hit in neighbors.hits} == {8, maximum}
                    assert all(hit.score == pytest.approx(1.0, abs=1e-5) for hit in neighbors.hits)
                finally:
                    if await opensearch.indices.exists(index=publication.index):
                        await opensearch.indices.delete(index=publication.index)
        finally:
            await admin.post(f"DROP DATABASE {database} SYNC")
