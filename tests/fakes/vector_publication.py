"""Synthetic resolved snapshot; no archive completeness or encoder claim."""

import hashlib

from openknowledge_contracts.embeddings import EmbeddingMutation, EmbeddingSnapshot

from openknowledge_core.search.vector_models import ResolvedVectors, VectorPublication
from openknowledge_core.search.vectors import vector_record_bytes

SOURCE = EmbeddingSnapshot(
    snapshot="sha256:" + "a" * 64, release="2026-10-06", model_revision="fixture-revision"
)


def record(corpusid: int, *, deleted: bool = False) -> EmbeddingMutation:
    return EmbeddingMutation(
        source=SOURCE,
        corpusid=corpusid,
        sequence=corpusid,
        provenance=f"fixture:row:{corpusid}",
        is_deleted=deleted,
        vector=() if deleted else (1.0,) + (0.0,) * 767,
    )


def publication(records: tuple[EmbeddingMutation, ...]) -> VectorPublication:
    digest = hashlib.sha256(b"".join(vector_record_bytes(row) for row in records)).hexdigest()
    return VectorPublication(
        index="paper-vectors-" + "1" * 32,
        resolved=ResolvedVectors(
            source=SOURCE,
            records=len(records),
            live_records=sum(not row.is_deleted for row in records),
            sha256="sha256:" + digest,
            resolution_evidence="fixture:resolved-manifest",
            completeness_evidence="fixture:complete-manifest",
            space_evidence="fixture:encoder-space",
        ),
    )
