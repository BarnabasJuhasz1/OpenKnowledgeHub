"""Search-specific resolved publication and explicit selection metadata."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator

from openknowledge_contracts.embeddings import CorpusId, EmbeddingMutation, EmbeddingSnapshot

VectorIndex = Annotated[str, Field(pattern=r"^paper-vectors-[0-9a-f]{32}$")]
Digest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
Evidence = Annotated[str, Field(min_length=1, max_length=4096, pattern=r"\S")]
Count = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]


class VectorModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class ResolvedVectors(VectorModel):
    """Expected canonical content, approved by the upstream resolver, not raw base shards.

    Digest is SHA256 over vector_record_bytes in strictly increasing corpusid order,
    including tombstones. Evidence references must be reviewed by the caller; this
    model does not itself verify an external manifest or establish encoder compatibility.
    """

    source: EmbeddingSnapshot
    records: Count
    live_records: Count
    sha256: Digest
    resolution_evidence: Evidence
    completeness_evidence: Evidence
    space_evidence: Evidence
    format: Literal["resolved-vectors-v1"] = "resolved-vectors-v1"

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if len(self.source.model_revision) > 256:
            raise ValueError("model revision must be a bounded reference")
        if self.live_records > self.records:
            raise ValueError("live count exceeds total count")
        if self.source.model_revision.strip().lower() in ("", "unknown", "unverified"):
            raise ValueError("publication requires an evidenced model revision")
        return self


class VectorPublication(VectorModel):
    """An explicit physical index; never a mutable alias or an implicit latest release."""

    index: VectorIndex
    resolved: ResolvedVectors


class VectorHit(VectorModel):
    corpusid: CorpusId
    score: Annotated[FiniteFloat, Field(strict=True)]


class VectorNeighbors(VectorModel):
    publication: VectorPublication
    hits: tuple[VectorHit, ...]


class SearchVectorNeighbors(VectorModel):
    publication: VectorPublication
    expected_source: EmbeddingSnapshot
    seed: EmbeddingMutation
    k: Annotated[int, Field(strict=True, ge=1, le=100)] = 10

    @model_validator(mode="after")
    def _compatible(self) -> Self:
        if self.expected_source != self.publication.resolved.source:
            raise ValueError("selected publication differs from the expected snapshot")
        if self.seed.source != self.expected_source or self.seed.is_deleted:
            raise ValueError("seed must be live and match the selected snapshot")
        return self
