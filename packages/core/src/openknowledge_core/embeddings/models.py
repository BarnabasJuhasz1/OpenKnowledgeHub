"""Publication receipts for immutable resolved embedding snapshots."""

from pydantic import BaseModel, ConfigDict

from openknowledge_contracts.embeddings import ResolvedVectors


class EmbeddingPublication(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    descriptor: ResolvedVectors
    table: str
