"""Publication of resolved snapshots, independent of the lexical projection."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterable

from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_core.embeddings.serialization import vector_record_bytes as vector_record_bytes
from openknowledge_core.search.errors import SearchRejected
from openknowledge_core.search.ports import VectorStore
from openknowledge_core.search.ranking import reciprocal_rank_fusion
from openknowledge_core.search.vector_models import (
    SearchVectorNeighbors,
    VectorNeighbors,
    VectorPublication,
)


class VectorPublicationService:
    def __init__(self, store: VectorStore) -> None:
        self._store = store

    async def publish(
        self, publication: VectorPublication, records: AsyncIterable[EmbeddingMutation]
    ) -> VectorPublication:
        """One fresh attempt; this service never selects an index for application use.

        Failures before sealing leave an incomplete index. A lost final acknowledgement
        or failed readback can leave a complete but unselected index; do not infer that
        publication failed from the response alone. No in-place replay/resume: retry
        with a new physical name and the same resolved identity.

        The index is created exclusively; create-only records retain tombstones.
        Memory is one batch, not all corpus IDs. Strict ordering rejects duplicate/conflicting
        records rather than guessing how to resolve raw shards or sequence histories.
        """
        await self._store.begin(publication)
        digest = hashlib.sha256()
        count = live = 0
        previous = -1
        batch: list[EmbeddingMutation] = []
        async for record in records:
            if record.source != publication.resolved.source:
                raise SearchRejected("vector record source does not match the resolved snapshot")
            if not record.provenance.strip():
                raise SearchRejected("vector record requires provenance")
            if record.corpusid <= previous:
                raise SearchRejected(
                    "resolved records must have strictly increasing unique corpusids"
                )
            previous = record.corpusid
            count += 1
            live += not record.is_deleted
            if count > publication.resolved.records or live > publication.resolved.live_records:
                raise SearchRejected("vector records exceed declared counts")
            encoded = vector_record_bytes(record)
            if len(encoded) > 64 * 1024:
                raise SearchRejected("canonical vector record exceeds 64 KiB")
            digest.update(encoded)
            batch.append(record)
            if len(batch) == 128:
                await self._store.write(publication, tuple(batch))
                batch.clear()
        if (
            count != publication.resolved.records
            or live != publication.resolved.live_records
            or "sha256:" + digest.hexdigest() != publication.resolved.sha256
        ):
            raise SearchRejected("vector snapshot counts or canonical digest differ from evidence")
        if batch:
            await self._store.write(publication, tuple(batch))
        await self._store.complete(publication)
        return publication

    async def neighbors(self, command: SearchVectorNeighbors) -> VectorNeighbors:
        """Read only an explicitly expected, complete and write-blocked publication."""
        return await self._store.neighbors(command)


def hybrid_ids(lexical: tuple[int, ...], neighbors: VectorNeighbors) -> tuple[int, ...]:
    """Opt-in rank fusion preserves lexical papers without vectors; no lexical writes."""
    return reciprocal_rank_fusion(lexical, tuple(hit.corpusid for hit in neighbors.hits))
