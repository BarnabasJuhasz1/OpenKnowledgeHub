"""Search-specific resolved publication and explicit selection metadata."""

from __future__ import annotations

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator

from openknowledge_contracts.embeddings import CorpusId, EmbeddingMutation, EmbeddingSnapshot
from openknowledge_contracts.embeddings import Count as Count
from openknowledge_contracts.embeddings import Digest as Digest
from openknowledge_contracts.embeddings import Evidence as Evidence
from openknowledge_contracts.embeddings import ResolvedVectors as ResolvedVectors

VectorIndex = Annotated[str, Field(pattern=r"^paper-vectors-[0-9a-f]{32}$")]


class VectorModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


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
