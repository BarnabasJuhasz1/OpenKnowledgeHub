"""Opt-in checks against the manager-owned engine; never start or stop a service.

Only http://127.0.0.1:19203 is used. Fresh prefixed fixture indices remain for the
manager's container cleanup. No real-corpus quality/capacity evidence follows.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

import pytest
from opensearchpy import AsyncOpenSearch
from pydantic import ValidationError

import openknowledge_adapters.opensearch.vectors as vector_adapter
import openknowledge_contracts.embeddings as embedding_contract
import openknowledge_core.search.vectors as vector_service
from openknowledge_adapters.opensearch.sink import OpenSearchSink
from openknowledge_adapters.opensearch.vectors import OpenSearchVectors
from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_contracts.search import PAPERS, index_body
from openknowledge_core.search.errors import SearchRejected
from openknowledge_core.search.vector_models import SearchVectorNeighbors, VectorPublication
from openknowledge_core.search.vectors import VectorPublicationService, hybrid_ids
from tests.fakes.ports import paper_row, search_document
from tests.fakes.vector_publication import SOURCE, publication, record

pytestmark = pytest.mark.skipif(
    os.environ.get("OKP_VECTOR_INTEGRATION") != "1",
    reason="requires explicitly reserved local OpenSearch at 127.0.0.1:19203",
)
ROOT = Path(__file__).resolve().parents[4]


def fixture_rows() -> tuple[EmbeddingMutation, ...]:
    # All vectors are synthetic; 0.1 demonstrates canonical Float32 source round trip.
    def live(corpusid: int, a: float, b: float) -> EmbeddingMutation:
        return EmbeddingMutation(
            source=SOURCE,
            corpusid=corpusid,
            sequence=corpusid,
            provenance=f"fixture:row:{corpusid}",
            vector=(a, b) + (0.0,) * 766,
        )

    return (
        live(1, 0.1, 0.2),
        live(2, 0.2, 0.4),
        record(3, deleted=True),
        live(2**64 - 1, 0.2, -0.1),
    )


async def stream(rows: tuple[EmbeddingMutation, ...]) -> AsyncGenerator[EmbeddingMutation]:
    for row in rows:
        yield row


def fresh_publication(rows: tuple[EmbeddingMutation, ...]) -> VectorPublication:
    name = "paper-vectors-5ea7" + uuid4().hex[4:]
    print(f"fixture index: {name}")
    return VectorPublication(index=name, resolved=publication(rows).resolved)


@pytest.fixture
def own_sources() -> None:
    for module in (vector_adapter, embedding_contract, vector_service):
        assert module.__file__ is not None
        assert Path(module.__file__).resolve().is_relative_to(ROOT), module.__name__
    assert version("opensearch-py") == "3.2.0"


@pytest.fixture
async def client(own_sources: None) -> AsyncGenerator[AsyncOpenSearch]:
    async with AsyncOpenSearch(
        hosts=["http://127.0.0.1:19203"], timeout=3, max_retries=0, retry_on_timeout=False
    ) as connection:
        assert (await connection.info())["version"]["number"] == "3.9.0"
        yield connection


async def test_complete_publication_and_lexical_index_stay_separate(
    client: AsyncOpenSearch,
) -> None:
    rows = fixture_rows()
    selected = fresh_publication(rows)
    lexical = "text-vector-check-" + selected.index.removeprefix("paper-vectors-")
    print(f"fixture index: {lexical}")
    sink = OpenSearchSink(client)
    body = index_body(PAPERS, shards=1)
    await sink.create_index(lexical, body.mappings, body.settings)
    await sink.bulk(lexical, [search_document(paper_row(1, 1, title="Original lexical title"))])
    await sink.refresh(lexical)
    text_mapping = await client.indices.get_mapping(index=lexical)

    service = VectorPublicationService(OpenSearchVectors(client))
    assert await service.publish(selected, stream(rows)) == selected
    mapping = (await client.indices.get_mapping(index=selected.index))[selected.index]["mappings"]
    assert mapping["_meta"] == {
        "vector_publication": selected.model_dump(mode="json"),
        "state": "complete",
    }
    assert mapping["properties"]["vector"]["method"]["engine"] == "lucene"
    assert mapping["properties"]["vector"]["dimension"] == 768
    assert mapping.get("_source", {}).get("enabled", True) is True
    settings = (
        await client.indices.get_settings(index=selected.index, params={"flat_settings": "true"})
    )[selected.index]["settings"]
    assert settings["index.knn.derived_source.enabled"] == "false"
    assert settings["index.blocks.write"] == "true"
    assert settings["index.refresh_interval"] == "-1"
    assert (await client.count(index=selected.index))["count"] == 4
    assert (
        await client.count(index=selected.index, body={"query": {"term": {"is_deleted": False}}})
    )["count"] == 3
    assert rows[0].vector[0] != 0.1
    for row in rows:
        stored = (await client.get(index=selected.index, id=str(row.corpusid)))["_source"]
        assert EmbeddingMutation.model_validate(stored) == row
        if row.is_deleted:
            assert "vector" not in stored

    result = await service.neighbors(
        SearchVectorNeighbors(publication=selected, expected_source=SOURCE, seed=rows[0], k=2)
    )
    assert tuple(hit.corpusid for hit in result.hits) == (2, 2**64 - 1)
    assert result.hits[0].score == pytest.approx(1.0, abs=1e-5)
    assert result.hits[1].score == pytest.approx(0.5, abs=1e-5)
    assert 99 in hybrid_ids((99, 2), result)  # A lexical paper need not have a vector.

    await sink.bulk(lexical, [search_document(paper_row(1, 2, title="Updated lexical title"))])
    await sink.refresh(lexical)
    assert await client.indices.get_mapping(index=lexical) == text_mapping
    assert "vector" not in text_mapping[lexical]["mappings"]["properties"]
    assert "vector" not in (await client.get(index=lexical, id="1"))["_source"]
    assert (
        await service.neighbors(
            SearchVectorNeighbors(publication=selected, expected_source=SOURCE, seed=rows[0], k=2)
        )
    ) == result


@pytest.mark.parametrize("corpusid", [3, 99])
async def test_missing_or_tombstoned_seed_keeps_lexical_results(
    client: AsyncOpenSearch, corpusid: int
) -> None:
    rows = fixture_rows()
    selected = fresh_publication(rows)
    service = VectorPublicationService(OpenSearchVectors(client))
    await service.publish(selected, stream(rows))
    result = await service.neighbors(
        SearchVectorNeighbors(
            publication=selected, expected_source=SOURCE, seed=record(corpusid), k=2
        )
    )
    assert result.hits == ()
    assert hybrid_ids((corpusid, 100), result) == (corpusid, 100)


async def test_identity_provenance_and_late_replay_fail_closed(client: AsyncOpenSearch) -> None:
    rows = fixture_rows()
    selected = fresh_publication(rows)
    adapter = OpenSearchVectors(client)
    service = VectorPublicationService(adapter)
    await service.publish(selected, stream(rows))
    other = SOURCE.model_copy(update={"snapshot": "sha256:" + "b" * 64})
    with pytest.raises(ValidationError, match="expected snapshot"):
        SearchVectorNeighbors(publication=selected, expected_source=other, seed=rows[0])
    altered = VectorPublication(
        index=selected.index,
        resolved=selected.resolved.model_copy(update={"completeness_evidence": "fixture:other"}),
    )
    with pytest.raises(SearchRejected, match="identity, evidence or completion"):
        await service.neighbors(
            SearchVectorNeighbors(publication=altered, expected_source=SOURCE, seed=rows[0])
        )
    with pytest.raises(SearchRejected, match="provenance"):
        await service.neighbors(
            SearchVectorNeighbors(
                publication=selected,
                expected_source=SOURCE,
                seed=rows[0].model_copy(update={"provenance": "fixture:wrong"}),
            )
        )
    with pytest.raises(SearchRejected):
        await service.publish(selected, stream(rows))  # Cannot adopt an existing index.
    with pytest.raises(SearchRejected):
        await adapter.write(selected, (record(3),))  # Cannot resurrect the tombstone.
    assert (await client.get(index=selected.index, id="3"))["_source"]["is_deleted"] is True
    assert (await client.count(index=selected.index))["count"] == 4


async def test_failed_and_partial_publications_cannot_be_read(client: AsyncOpenSearch) -> None:
    rows = fixture_rows()
    selected = fresh_publication(rows)
    adapter = OpenSearchVectors(client)
    service = VectorPublicationService(adapter)
    with pytest.raises(SearchRejected, match="counts or canonical digest"):
        await service.publish(selected, stream(rows[:2]))
    with pytest.raises(SearchRejected, match="completion"):
        await service.neighbors(
            SearchVectorNeighbors(publication=selected, expected_source=SOURCE, seed=rows[0])
        )
    # Even a direct partial port write cannot pass the engine's seal count checks.
    await adapter.write(selected, rows[:2])
    with pytest.raises(SearchRejected, match="stored count"):
        await adapter.complete(selected)
    metadata = (await client.indices.get_mapping(index=selected.index))[selected.index]["mappings"][
        "_meta"
    ]
    assert metadata["state"] == "building"
    with pytest.raises(SearchRejected, match="completion"):
        await service.neighbors(
            SearchVectorNeighbors(publication=selected, expected_source=SOURCE, seed=rows[0])
        )


@pytest.mark.parametrize("mismatch", ["total", "live"])
async def test_reader_rechecks_counts_even_if_completion_metadata_was_forged(
    client: AsyncOpenSearch, mismatch: str
) -> None:
    rows = fixture_rows()
    selected = fresh_publication(rows)
    adapter = OpenSearchVectors(client)
    await adapter.begin(selected)
    actual = rows[:2] if mismatch == "total" else (*rows[:3], record(2**64 - 1, deleted=True))
    await adapter.write(selected, actual)
    await client.indices.add_block(index=selected.index, block="write")
    await client.indices.refresh(index=selected.index)
    # Adversarial fixture: bypass service to model an incorrect administrative marker.
    await client.indices.put_mapping(
        index=selected.index,
        body={
            "_meta": {"vector_publication": selected.model_dump(mode="json"), "state": "complete"}
        },
    )
    with pytest.raises(SearchRejected, match="stored .*count"):
        await adapter.neighbors(
            SearchVectorNeighbors(publication=selected, expected_source=SOURCE, seed=rows[0])
        )
