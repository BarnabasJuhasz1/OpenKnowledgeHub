"""Immutable embedding identity and Float32 mutations shared by ingest and lookup."""

import math
import struct
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator, model_validator

from openknowledge_contracts.datasets import ReleaseId

CorpusId = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]


class EmbeddingSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    # Immutable content/manifest identity, not an alias such as "latest".
    snapshot: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    dataset: Literal["embeddings-specter_v2"] = "embeddings-specter_v2"
    release: ReleaseId
    model: Literal["specter_v2"] = "specter_v2"
    dimension: Literal[768] = 768
    dtype: Literal["float32"] = "float32"
    # Unknown is explicit: the upstream model name does not establish its adapter.
    model_revision: Annotated[str, Field(min_length=1)]


class EmbeddingMutation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    source: EmbeddingSnapshot
    corpusid: CorpusId
    sequence: Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]
    provenance: Annotated[str, Field(min_length=1)]
    is_deleted: Annotated[bool, Field(strict=True)] = False
    vector: Annotated[tuple[FiniteFloat, ...], Field(max_length=768)] = ()

    @field_validator("vector")
    @classmethod
    def canonical_float32(cls, values: tuple[float, ...]) -> tuple[float, ...]:
        try:
            canonical = tuple(struct.unpack("!f", struct.pack("!f", x))[0] for x in values)
        except (OverflowError, struct.error) as exc:
            raise ValueError("vector values must fit finite Float32") from exc
        if not all(math.isfinite(x) for x in canonical):
            raise ValueError("vector values must fit finite Float32")
        return canonical

    @model_validator(mode="after")
    def validate_vector(self) -> "EmbeddingMutation":
        if self.is_deleted:
            if self.vector:
                raise ValueError("deleted embeddings must have an empty vector")
        elif len(self.vector) != self.source.dimension or not any(self.vector):
            raise ValueError("live embeddings require 768 finite values and nonzero norm")
        return self


class LookupEmbeddings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: EmbeddingSnapshot
    corpusids: Annotated[tuple[CorpusId, ...], Field(max_length=3000)]


class EmbeddingLookupResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    source: EmbeddingSnapshot
    found: tuple[EmbeddingMutation, ...]
    missing: tuple[CorpusId, ...]

    @model_validator(mode="after")
    def validate_result(self) -> "EmbeddingLookupResult":
        ids = [row.corpusid for row in self.found] + list(self.missing)
        if len(ids) > 3000 or len(ids) != len(set(ids)):
            raise ValueError("lookup results require at most 3000 unique, disjoint IDs")
        if any(row.source != self.source or row.is_deleted for row in self.found):
            raise ValueError("found embeddings must be live and match all snapshot metadata")
        return self


Digest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
Evidence = Annotated[str, Field(min_length=1, max_length=4096, pattern=r"\S")]
Count = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]


class ResolvedVectors(BaseModel):
    """Expected canonical content, approved by the upstream resolver, not raw base shards.

    Digest is SHA256 over vector_record_bytes in strictly increasing corpusid order,
    including tombstones. Evidence references must be reviewed by the caller; this
    model does not itself verify an external manifest or establish encoder compatibility.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

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
