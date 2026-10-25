from collections.abc import AsyncGenerator

import pytest
from openknowledge_contracts.embeddings import EmbeddingMutation
from pydantic import ValidationError

from openknowledge_core.search.errors import SearchRejected
from openknowledge_core.search.vector_models import (
    SearchVectorNeighbors,
    VectorHit,
    VectorNeighbors,
    VectorPublication,
)
from openknowledge_core.search.vectors import VectorPublicationService, hybrid_ids
from tests.fakes.vector_publication import SOURCE, publication, record


class MemoryVectors:
    def __init__(self) -> None:
        self.begun = False
        self.complete_called = False
        self.rows: list[EmbeddingMutation] = []

    async def begin(self, publication: VectorPublication) -> None:
        self.begun = True

    async def write(
        self, publication: VectorPublication, records: tuple[EmbeddingMutation, ...]
    ) -> None:
        assert 1 <= len(records) <= 128
        self.rows.extend(records)

    async def complete(self, publication: VectorPublication) -> None:
        self.complete_called = True

    async def neighbors(self, command: SearchVectorNeighbors) -> VectorNeighbors:
        return VectorNeighbors(publication=command.publication, hits=())


async def stream(rows: tuple[EmbeddingMutation, ...]) -> AsyncGenerator[EmbeddingMutation]:
    for row in rows:
        yield row


async def test_resolved_snapshot_retains_tombstones_in_bounded_batches() -> None:
    rows = tuple(record(id_, deleted=id_ == 129) for id_ in range(130))
    store = MemoryVectors()
    expected = publication(rows)
    assert await VectorPublicationService(store).publish(expected, stream(rows)) == expected
    assert store.rows == list(rows)
    assert store.complete_called


@pytest.mark.parametrize("failure", ["duplicate", "older", "digest", "short", "source"])
async def test_invalid_or_incomplete_snapshot_never_completes(failure: str) -> None:
    rows = (record(1), record(2, deleted=True), record(3))
    expected = publication(rows)
    if failure == "duplicate":
        rows = (rows[0], rows[0], rows[2])
    elif failure == "older":
        rows = (record(3), record(2, deleted=True), record(1))
    elif failure == "digest":
        # An altered provenance is not interchangeable even when the vector is equal.
        rows = (rows[0].model_copy(update={"provenance": "fixture:conflict"}), *rows[1:])
    elif failure == "short":
        rows = rows[:1]
    else:
        other = SOURCE.model_copy(update={"model_revision": "other"})
        rows = (rows[0].model_copy(update={"source": other}), *rows[1:])
    store = MemoryVectors()
    with pytest.raises(SearchRejected):
        await VectorPublicationService(store).publish(expected, stream(rows))
    assert store.begun and not store.complete_called


async def test_interrupted_source_leaves_unselected_attempt() -> None:
    rows = tuple(record(id_) for id_ in range(129))

    async def failing() -> AsyncGenerator[EmbeddingMutation]:
        async for row in stream(rows[:128]):
            yield row
        raise OSError("fixture source failed")

    store = MemoryVectors()
    with pytest.raises(OSError):
        await VectorPublicationService(store).publish(publication(rows), failing())
    assert len(store.rows) == 128
    assert not store.complete_called


def test_selection_rejects_old_snapshot_and_missing_evidence() -> None:
    selected = publication((record(1),))
    with pytest.raises(ValidationError, match="expected snapshot"):
        SearchVectorNeighbors(
            publication=selected,
            expected_source=SOURCE.model_copy(update={"snapshot": "sha256:" + "b" * 64}),
            seed=record(1),
        )
    raw = selected.model_dump()
    del raw["resolved"]["completeness_evidence"]
    with pytest.raises(ValidationError):
        VectorPublication.model_validate(raw)
    raw = selected.model_dump()
    raw["resolved"]["source"]["model_revision"] = "unknown"
    with pytest.raises(ValidationError):
        VectorPublication.model_validate(raw)


def test_missing_vectors_preserve_lexical_results() -> None:
    selected = publication((record(1),))
    assert hybrid_ids((9, 10), VectorNeighbors(publication=selected, hits=())) == (9, 10)
    hits = VectorNeighbors(publication=selected, hits=(VectorHit(corpusid=10, score=0.9),))
    assert hybrid_ids((9, 10), hits) == (10, 9)
