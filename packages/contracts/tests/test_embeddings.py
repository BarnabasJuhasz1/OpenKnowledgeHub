import pytest
from pydantic import ValidationError

from openknowledge_contracts.embeddings import (
    EmbeddingMutation,
    EmbeddingSnapshot,
    LookupEmbeddings,
)

SOURCE = EmbeddingSnapshot(
    snapshot="sha256:" + "a" * 64, release="2026-10-06", model_revision="unknown"
)


def test_live_and_delete_have_distinct_payloads() -> None:
    row = EmbeddingMutation(
        source=SOURCE, corpusid=2**64 - 1, sequence=1, provenance="object:1", vector=(0.5,) * 768
    )
    assert row.vector[0] == 0.5
    deleted = EmbeddingMutation(
        source=SOURCE, corpusid=1, sequence=2, provenance="deletes:1", is_deleted=True
    )
    assert deleted.vector == ()
    with pytest.raises(ValidationError, match="empty vector"):
        EmbeddingMutation(
            source=SOURCE,
            corpusid=1,
            sequence=2,
            provenance="deletes:1",
            is_deleted=True,
            vector=(1.0,) * 768,
        )


@pytest.mark.parametrize("vector", [(), (0.0,) * 768, (1.0,) * 767, (float("nan"),) * 768])
def test_reject_unusable_vectors(vector: tuple[float, ...]) -> None:
    with pytest.raises(ValidationError):
        EmbeddingMutation(
            source=SOURCE, corpusid=1, sequence=0, provenance="object:1", vector=vector
        )


@pytest.mark.parametrize("corpusid", [-1, 2**64, True, "1"])
def test_lookup_validates_ids(corpusid: object) -> None:
    with pytest.raises(ValidationError):
        LookupEmbeddings.model_validate({"source": SOURCE, "corpusids": [corpusid]})


def test_lookup_budget() -> None:
    with pytest.raises(ValidationError):
        LookupEmbeddings(source=SOURCE, corpusids=tuple(range(3001)))


@pytest.mark.parametrize("vector", [(1e39,) * 768, (1e-50,) * 768])
def test_float32_overflow_and_underflow_to_zero(vector: tuple[float, ...]) -> None:
    with pytest.raises(ValidationError):
        EmbeddingMutation(source=SOURCE, corpusid=1, sequence=0, provenance="f:1", vector=vector)


def test_float32_is_canonical_and_json_roundtrips() -> None:
    import struct

    row = EmbeddingMutation(
        source=SOURCE, corpusid=2**64 - 1, sequence=2**64 - 1, provenance="f:1", vector=(0.1,) * 768
    )
    assert row.vector[0] == struct.unpack("!f", struct.pack("!f", 0.1))[0]
    assert EmbeddingMutation.model_validate_json(row.model_dump_json()) == row


@pytest.mark.parametrize("snapshot", ["latest", " ", "sha256:" + "A" * 64])
def test_snapshot_requires_explicit_content_digest(snapshot: str) -> None:
    with pytest.raises(ValidationError):
        EmbeddingSnapshot(snapshot=snapshot, release="2026-10-06", model_revision="unknown")


def test_model_revision_must_be_explicit() -> None:
    with pytest.raises(ValidationError):
        EmbeddingSnapshot.model_validate({"snapshot": SOURCE.snapshot, "release": SOURCE.release})


def test_result_enforces_source_live_rows_and_disjoint_ids() -> None:
    from openknowledge_contracts.embeddings import EmbeddingLookupResult

    live = EmbeddingMutation(
        source=SOURCE, corpusid=1, sequence=0, provenance="f:1", vector=(1.0,) * 768
    )
    deleted = EmbeddingMutation(
        source=SOURCE, corpusid=1, sequence=1, provenance="f:1", is_deleted=True
    )
    other = EmbeddingSnapshot(
        snapshot=SOURCE.snapshot, release="2026-10-07", model_revision="unknown"
    )
    for source, found, missing in [
        (SOURCE, (live,), (1,)),
        (SOURCE, (live, live), ()),
        (SOURCE, (deleted,), ()),
        (other, (live,), ()),
    ]:
        with pytest.raises(ValidationError):
            EmbeddingLookupResult(source=source, found=found, missing=missing)
