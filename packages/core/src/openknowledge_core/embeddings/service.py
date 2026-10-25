"""Bounded validation at the resolved stream's publication boundary."""

from collections.abc import AsyncGenerator, AsyncIterable
from hashlib import sha256

from openknowledge_contracts.embeddings import EmbeddingMutation, ResolvedVectors
from openknowledge_core.embeddings.models import EmbeddingPublication
from openknowledge_core.embeddings.ports import EmbeddingPublisher
from openknowledge_core.embeddings.serialization import vector_record_bytes
from openknowledge_core.ingestion.errors import ContractViolation


async def validated_records(
    descriptor: ResolvedVectors, rows: AsyncIterable[EmbeddingMutation]
) -> AsyncGenerator[EmbeddingMutation]:
    """Check canonical source order, exact counts and digest without buffering the corpus."""
    digest = sha256()
    count = live = 0
    previous = -1
    async for row in rows:
        if row.source != descriptor.source:
            raise ContractViolation("embedding snapshot identity or metadata mismatch")
        if row.corpusid <= previous:
            raise ContractViolation("resolved embedding IDs must be unique and strictly increasing")
        previous = row.corpusid
        digest.update(vector_record_bytes(row))
        count += 1
        live += not row.is_deleted
        if count > descriptor.records or live > descriptor.live_records:
            raise ContractViolation("resolved embedding counts exceed the pinned descriptor")
        yield row
    if (
        count != descriptor.records
        or live != descriptor.live_records
        or "sha256:" + digest.hexdigest() != descriptor.sha256
    ):
        raise ContractViolation(
            "resolved embedding count or digest differs from the pinned descriptor"
        )


async def publish_embeddings(
    publisher: EmbeddingPublisher,
    descriptor: ResolvedVectors,
    rows: AsyncIterable[EmbeddingMutation],
) -> EmbeddingPublication:
    """Publish a pinned resolved stream with caller-owned lifetime, timeout and retry budget.

    The caller owns closing its source iterator (for example with aclosing around an
    async generator), including when validation or publication stops consumption early.
    """
    return await publisher.publish(descriptor, rows)
