from collections.abc import AsyncIterable
from typing import Protocol

from openknowledge_contracts.embeddings import (
    EmbeddingLookupResult,
    EmbeddingMutation,
    LookupEmbeddings,
    ResolvedVectors,
)
from openknowledge_core.embeddings.models import EmbeddingPublication


class EmbeddingLookup(Protocol):
    async def lookup(self, command: LookupEmbeddings) -> EmbeddingLookupResult:
        """Read one published immutable snapshot; report absent/deleted IDs as missing."""
        ...


class EmbeddingPublisher(Protocol):
    async def publish(
        self, descriptor: ResolvedVectors, rows: AsyncIterable[EmbeddingMutation]
    ) -> EmbeddingPublication:
        """Validate and publish an immutable, ordered resolved stream, including tombstones.

        Retry a failed call with a fresh stream; never retry individual inserts. An equal
        descriptor and canonical stream replay succeeds; a changed publication fails.
        The caller owns the source iterator's lifetime and closes it on early failure.
        """
        ...
