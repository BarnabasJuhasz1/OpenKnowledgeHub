"""Explicit lineage and immutable identities for common vector resolution."""

from __future__ import annotations

import hashlib
import json
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.embeddings import (
    Count,
    Digest,
    EmbeddingSnapshot,
    Evidence,
    ResolvedVectors,
)


class ResolutionModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class ResolutionRelease(ResolutionModel):
    """One complete base or complete update/delete pair, explicitly sequenced.

    ``identity`` binds the adapter's verified inventory and all artifact receipts,
    including empty diff parts. Completeness evidence must describe its scope;
    a bounded sample is never evidence of global corpus completeness.
    """

    release: ReleaseId
    from_release: ReleaseId | None = None
    sequence: Count
    identity: Digest


class ResolutionPlan(ResolutionModel):
    source: EmbeddingSnapshot
    releases: tuple[ResolutionRelease, ...] = Field(min_length=1)
    completeness_evidence: Evidence
    space_evidence: Evidence
    scope: Literal["corpus", "bounded-sample"]
    format: Literal["vector-resolution-plan-v1"] = "vector-resolution-plan-v1"

    @model_validator(mode="after")
    def _lineage(self) -> Self:
        previous: ResolutionRelease | None = None
        for release in self.releases:
            if previous is None:
                if release.from_release is not None or release.sequence != 0:
                    raise ValueError(
                        "resolution requires an explicit complete base at sequence zero"
                    )
            elif (
                release.from_release != previous.release
                or release.release <= previous.release
                or release.sequence != previous.sequence + 1
            ):
                raise ValueError("resolution requires contiguous lineage and contiguous sequences")
            previous = release
        if self.releases[-1].release != self.source.release:
            raise ValueError("target snapshot release differs from final declared release")
        if self.source.snapshot != self.identity:
            raise ValueError("snapshot identity must bind the resolution plan")
        return self

    @property
    def identity(self) -> str:
        """Hash every declaration except the resulting snapshot digest itself."""
        document = self.model_dump(mode="json")
        del document["source"]["snapshot"]
        return (
            "sha256:"
            + hashlib.sha256(
                json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )


class ResolvedPart(ResolutionModel):
    """One bounded Parquet part; its filename derives from its ordinal."""

    ordinal: Count
    records: Count
    live_records: Count
    parquet_bytes: Count
    sha256: Digest


class ResolvedArtifact(ResolutionModel):
    """Finalized manifest; files live alongside it under fixed, non-input names."""

    plan: ResolutionPlan
    resolved: ResolvedVectors
    input_records: Count
    input_parquet_bytes: Count | None = None
    parquet_bytes: Count
    parts: tuple[ResolvedPart, ...] = Field(min_length=1)
    input_verification: Literal["unverified-iterable", "receipt-backed"] = "unverified-iterable"
    format: Literal["resolved-vector-artifact-v1"] = "resolved-vector-artifact-v1"

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if (
            self.resolved.source != self.plan.source
            or self.resolved.resolution_evidence != self.plan.identity
            or self.resolved.completeness_evidence != self.plan.completeness_evidence
            or self.resolved.space_evidence != self.plan.space_evidence
        ):
            raise ValueError("resolved manifest differs from its bound plan")
        if (
            any(
                part.ordinal != index or part.live_records > part.records
                for index, part in enumerate(self.parts)
            )
            or sum(part.records for part in self.parts) != self.resolved.records
            or sum(part.live_records for part in self.parts) != self.resolved.live_records
            or sum(part.parquet_bytes for part in self.parts) != self.parquet_bytes
        ):
            raise ValueError("resolved part ordinals or totals differ from manifest")
        return self


def resolution_identity(
    source: EmbeddingSnapshot,
    releases: tuple[ResolutionRelease, ...],
    completeness_evidence: str,
    space_evidence: str,
    scope: Literal["corpus", "bounded-sample"],
) -> str:
    """Compute the snapshot ID before constructing the validated plan."""
    # Construction bypasses validation only to calculate the self-excluding hash.
    return ResolutionPlan.model_construct(
        source=source,
        releases=releases,
        completeness_evidence=completeness_evidence,
        space_evidence=space_evidence,
        scope=scope,
    ).identity
