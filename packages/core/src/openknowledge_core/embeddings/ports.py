from typing import Protocol

from openknowledge_contracts.embeddings import (
    EmbeddingLookupResult,
    EmbeddingMutation,
    EmbeddingSnapshot,
    LookupEmbeddings,
)


class EmbeddingLookup(Protocol):
    async def lookup(self, command: LookupEmbeddings) -> EmbeddingLookupResult:
        """Read one published immutable snapshot; report absent/deleted IDs as missing."""
        ...


class EmbeddingIngest(Protocol):
    async def ingest(self, source: EmbeddingSnapshot, rows: tuple[EmbeddingMutation, ...]) -> None:
        """Stage at most 3000 mutations with matching source metadata; never publish.

        A snapshot/corpusid/sequence replay must have identical contents; conflicting
        replays fail. Transport failure may leave a partial batch: replay unchanged.
        Cancellation propagates. The caller owns bounded retries and checkpointing.
        """
        ...
