"""Canonical record encoding shared by resolved embedding consumers."""

import json

from openknowledge_contracts.embeddings import EmbeddingMutation


def vector_record_bytes(record: EmbeddingMutation) -> bytes:
    """Canonical v1 JSONL: validated Float32 values, sorted keys, no optional whitespace."""
    return (
        json.dumps(record.model_dump(mode="json"), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
