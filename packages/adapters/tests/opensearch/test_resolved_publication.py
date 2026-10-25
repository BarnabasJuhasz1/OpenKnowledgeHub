"""OpenSearch-specific publication checks for neutral resolved artifacts."""

import os
from pathlib import Path
from uuid import uuid4

import pytest
from opensearchpy import AsyncOpenSearch

from openknowledge_adapters.opensearch.vectors import OpenSearchVectors
from openknowledge_adapters.resolved.store import (
    iter_resolved_vectors,
    resolve_vectors,
    stream_resolved_vectors,
)
from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_core.search.vector_models import SearchVectorNeighbors, VectorPublication
from openknowledge_core.search.vectors import VectorPublicationService
from packages.adapters.tests.test_resolved_vectors import plan, row


async def test_artifact_is_executable_publication_input(tmp_path: Path) -> None:
    from packages.core.tests.test_vector_publication import MemoryVectors

    p = plan()
    artifact = resolve_vectors(p, [row(p, 1), row(p, 2, deleted=True)], tmp_path / "result")
    publication = VectorPublication(index="paper-vectors-" + "c" * 32, resolved=artifact.resolved)
    store = MemoryVectors()
    await VectorPublicationService(store).publish(
        publication, stream_resolved_vectors(tmp_path / "result", artifact)
    )
    assert store.complete_called
    assert len(store.rows) == 2


@pytest.mark.skipif(os.environ.get("OKP_VECTOR_INTEGRATION") != "1", reason="local engine opt-in")
async def test_resolved_parquet_publication_opensearch(tmp_path: Path) -> None:
    p = plan()
    artifact = resolve_vectors(p, [row(p, 2**64 - 1), row(p, 0, deleted=True)], tmp_path / "result")
    publication = VectorPublication(
        index="paper-vectors-" + uuid4().hex, resolved=artifact.resolved
    )
    async with AsyncOpenSearch(
        hosts=["http://127.0.0.1:19203"], timeout=3, max_retries=0, retry_on_timeout=False
    ) as client:
        await client.info()
        try:
            service = VectorPublicationService(OpenSearchVectors(client))
            await service.publish(
                publication, stream_resolved_vectors(tmp_path / "result", artifact)
            )
            assert (await client.count(index=publication.index))["count"] == 2
            tombstone = (await client.get(index=publication.index, id="0"))["_source"]
            assert tombstone["is_deleted"] and "vector" not in tombstone
            stored = (await client.get(index=publication.index, id=str(2**64 - 1)))["_source"]
            expected = list(iter_resolved_vectors(tmp_path / "result", artifact))[-1]
            assert EmbeddingMutation.model_validate(stored) == expected
        finally:
            if await client.indices.exists(index=publication.index):
                await client.indices.delete(index=publication.index)


@pytest.mark.skipif(os.environ.get("OKP_VECTOR_INTEGRATION") != "1", reason="local engine opt-in")
async def test_archive_conversion_resolution_opensearch(tmp_path: Path) -> None:
    from openknowledge_adapters.resolved.parquet import resolve_parquet_vectors
    from tests.test_resolved_bridge import fixture_archive

    store, plan, selected, parquet = await fixture_archive(tmp_path)
    artifact = await resolve_parquet_vectors(plan, selected, store, parquet, tmp_path / "resolved")
    publication = VectorPublication(
        index="paper-vectors-" + uuid4().hex, resolved=artifact.resolved
    )
    async with AsyncOpenSearch(
        hosts=["http://127.0.0.1:19203"], timeout=3, max_retries=0, retry_on_timeout=False
    ) as client:
        await client.info()
        try:
            service = VectorPublicationService(OpenSearchVectors(client))
            await service.publish(
                publication, stream_resolved_vectors(tmp_path / "resolved", artifact)
            )
            rows = list(iter_resolved_vectors(tmp_path / "resolved", artifact))
            assert (await client.count(index=publication.index))["count"] == 4
            assert (await client.get(index=publication.index, id="7"))["_source"]["is_deleted"]
            neighbors = await service.neighbors(
                SearchVectorNeighbors(
                    publication=publication,
                    expected_source=plan.source,
                    seed=rows[0],
                    k=3,
                )
            )
            assert set(hit.corpusid for hit in neighbors.hits) == {8, 2**64 - 1}
        finally:
            if await client.indices.exists(index=publication.index):
                await client.indices.delete(index=publication.index)
