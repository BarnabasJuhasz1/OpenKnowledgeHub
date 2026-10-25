"""Offline exact cosine baseline for a bounded, explicitly identified sample.

This is a pilot input format, not the shared ingestion or serving contract. Candidate
rankings must come from the same sample; this module performs no ANN approximation.
"""

from __future__ import annotations

import math
import struct
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator, model_validator

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_core.search.ranking import reciprocal_rank_fusion

Float32Value = Annotated[FiniteFloat, Field(ge=-3.4028234663852886e38, le=3.4028234663852886e38)]
Vector = Annotated[tuple[Float32Value, ...], Field(min_length=768, max_length=768)]
CorpusId = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]


class PilotModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class PilotVector(PilotModel):
    corpusid: CorpusId
    vector: Vector

    @field_validator("vector")
    @classmethod
    def _float32(cls, values: tuple[float, ...]) -> tuple[float, ...]:
        return tuple(struct.unpack("!f", struct.pack("!f", value))[0] for value in values)

    @model_validator(mode="after")
    def _nonzero(self) -> Self:
        if not any(self.vector):
            raise ValueError("cosine requires a nonzero vector")
        return self


class PilotSample(PilotModel):
    """Operator export: provenance and all vectors in this bounded evaluation universe."""

    dataset: Literal["embeddings-specter_v2"]
    release: ReleaseId
    model: str = Field(min_length=1)
    document_adapter: str = Field(min_length=1)
    model_revision: str = Field(min_length=1)
    provenance: str = Field(min_length=1)
    dtype: Literal["float32"]
    vectors: Annotated[tuple[PilotVector, ...], Field(min_length=1, max_length=512)]

    @model_validator(mode="after")
    def _distinct(self) -> Self:
        ids = [row.corpusid for row in self.vectors]
        if len(ids) != len(set(ids)):
            raise ValueError("sample corpusids repeat")
        return self


class PilotQuery(PilotVector):
    """Paper queries use the archived vector; text queries require external space evidence."""

    kind: Literal["paper", "text"]
    compatibility_evidence: str = Field(min_length=1)
    candidates: Annotated[tuple[CorpusId, ...], Field(max_length=512)]
    bm25_candidates: Annotated[tuple[CorpusId, ...], Field(max_length=512)] = ()


class PilotInput(PilotModel):
    sample: PilotSample
    k: Annotated[int, Field(strict=True, ge=1, le=100)] = 10
    queries: Annotated[tuple[PilotQuery, ...], Field(min_length=1, max_length=32)]

    @model_validator(mode="after")
    def _universe(self) -> Self:
        vectors = {row.corpusid: row.vector for row in self.sample.vectors}
        ids = [query.corpusid for query in self.queries]
        if len(ids) != len(set(ids)):
            raise ValueError("query ids repeat")
        for query in self.queries:
            if len(query.bm25_candidates) != len(set(query.bm25_candidates)):
                raise ValueError("BM25 candidate ids repeat")
            if query.kind == "paper" and query.corpusid in query.bm25_candidates:
                raise ValueError("paper BM25 candidates must exclude self")
            if len(query.candidates) != len(set(query.candidates)):
                raise ValueError("candidate ids repeat")
            if any(id_ not in vectors for id_ in query.candidates):
                raise ValueError("candidate outside evaluation sample")
            if query.kind == "paper":
                if vectors.get(query.corpusid) != query.vector:
                    raise ValueError("paper query must use its archived vector")
                if query.corpusid in query.candidates:
                    raise ValueError("paper candidates must exclude self")
            eligible = len(vectors) - (query.kind == "paper")
            if eligible < self.k:
                raise ValueError("sample has fewer eligible vectors than k")
        return self


class PilotResult(PilotModel):
    sample_count: int
    dimension: Literal[768] = 768
    raw_float32_bytes: int
    binary_code_bytes_lower_bound: int
    recall_at_k: tuple[float, ...]
    mean_recall_at_k: float
    hybrid_rankings: tuple[tuple[int, ...], ...]


def _unit(vector: Vector) -> tuple[float, ...]:
    # Scale first to avoid overflow/underflow on finite but extreme inputs.
    scale = max(abs(value) for value in vector)
    scaled = tuple(value / scale for value in vector)
    norm = math.sqrt(math.fsum(value * value for value in scaled))
    return tuple(value / norm for value in scaled)


def evaluate(pilot: PilotInput) -> PilotResult:
    """Compare supplied rankings against exact cosine, with corpusid breaking ties.

    Short/empty candidate lists lose recall. No missing vector becomes a zero vector.
    Byte counts describe raw payload and ideal one-bit codes only, never index size/RAM.
    """
    vectors = {row.corpusid: _unit(row.vector) for row in pilot.sample.vectors}
    recalls: list[float] = []
    for query in pilot.queries:
        unit = _unit(query.vector)
        scores = [
            (id_, math.fsum(a * b for a, b in zip(unit, vector, strict=True)))
            for id_, vector in vectors.items()
            if query.kind != "paper" or id_ != query.corpusid
        ]
        exact = {id_ for id_, _ in sorted(scores, key=lambda pair: (-pair[1], pair[0]))[: pilot.k]}
        recalls.append(len(exact.intersection(query.candidates[: pilot.k])) / pilot.k)
    count = len(vectors)
    return PilotResult(
        sample_count=count,
        raw_float32_bytes=count * 768 * 4,
        binary_code_bytes_lower_bound=count * 96,
        recall_at_k=tuple(recalls),
        mean_recall_at_k=math.fsum(recalls) / len(recalls),
        hybrid_rankings=tuple(
            reciprocal_rank_fusion(query.bm25_candidates, query.candidates)[: pilot.k]
            if query.bm25_candidates
            else ()
            for query in pilot.queries
        ),
    )
