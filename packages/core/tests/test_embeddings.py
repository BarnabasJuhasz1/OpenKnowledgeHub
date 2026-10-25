from itertools import permutations

import pytest

from openknowledge_contracts.embeddings import (
    EmbeddingMutation,
    EmbeddingSnapshot,
    LookupEmbeddings,
)
from openknowledge_core.embeddings.resolve import resolve_lookup, validate_batch

SOURCE = EmbeddingSnapshot(
    snapshot="sha256:" + "a" * 64, release="2026-10-06", model_revision="unknown"
)


def row(sequence: int, deleted: bool = False, provenance: str = "fixture:1") -> EmbeddingMutation:
    return EmbeddingMutation(
        source=SOURCE,
        corpusid=2**64 - 1,
        sequence=sequence,
        provenance=provenance,
        is_deleted=deleted,
        vector=() if deleted else (0.1,) * 768,
    )


def test_order_duplicates_and_tombstones() -> None:
    command = LookupEmbeddings(source=SOURCE, corpusids=(2**64 - 1, 7, 7))
    for rows in permutations((row(0), row(1, True), row(0))):
        result = resolve_lookup(command, rows)
        assert result.found == ()
        assert result.missing == (2**64 - 1, 7)
    result = resolve_lookup(command, (row(2), row(1, True)))
    assert result.found == (row(2),)
    assert result.missing == (7,)


def test_conflicts_fail_even_if_superseded() -> None:
    with pytest.raises(ValueError, match="conflicting"):
        validate_batch(SOURCE, (row(0), row(2), row(0, provenance="other:1")))
    with pytest.raises(ValueError, match="conflicting"):
        validate_batch(SOURCE, (row(0), row(0, True)))


def test_snapshot_metadata_cannot_change() -> None:
    other = EmbeddingSnapshot(
        snapshot=SOURCE.snapshot, release="2026-10-07", model_revision="unknown"
    )
    with pytest.raises(ValueError, match="metadata mismatch"):
        validate_batch(other, (row(0),))


def test_bounded_candidates_and_wrong_ids() -> None:
    with pytest.raises(ValueError, match="3000"):
        validate_batch(SOURCE, (row(0),) * 3001)
    with pytest.raises(ValueError, match="unrequested"):
        resolve_lookup(LookupEmbeddings(source=SOURCE, corpusids=(7,)), (row(0),))
