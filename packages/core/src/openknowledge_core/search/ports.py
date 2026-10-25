"""What the search use cases need from outside: the current rows, and an index to write."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from typing import Any, Protocol

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_core.search.models import BulkOperation, IndexName, SourceRows
from openknowledge_core.search.vector_models import (
    SearchVectorNeighbors,
    VectorNeighbors,
    VectorPublication,
)


class SearchSource(Protocol):
    async def bounds(self, release: ReleaseId) -> tuple[int, int]:
        """The smallest and the largest corpus ID at ``release``, which every part splits."""
        ...

    async def base(self, release: ReleaseId, low: int, high: int) -> AsyncGenerator[SourceRows]:
        """The live rows of both tables at ``release``, one item per corpus ID from low to high.

        The tables keep one row per key, so ``release`` must be the one they hold: an older
        state cannot be read back once a later release replaced a row. Items come in no set
        order.
        """
        ...

    async def diff(
        self, from_release: ReleaseId, release: ReleaseId, low: int, high: int
    ) -> AsyncGenerator[SourceRows]:
        """The latest row per key in both tables, tombstones included, for the changed keys.

        A key counts as changed when either table has a row for it at a release after
        ``from_release`` and up to ``release``.
        """
        ...


class SearchSink(Protocol):
    async def exists(self, index: IndexName) -> bool: ...

    async def create_index(
        self, index: IndexName, mapping: Mapping[str, Any], settings: Mapping[str, Any]
    ) -> None: ...

    async def bulk(self, index: IndexName, operations: Sequence[BulkOperation]) -> None:
        """Apply every operation, or raise BulkRefused naming a refused corpus ID and why."""
        ...

    async def set_refresh_interval(self, index: IndexName, interval: str | None) -> None:
        """Set how often the index refreshes: ``1s``, ``-1`` for never, or None for the default."""
        ...

    async def refresh(self, index: IndexName) -> None: ...

    async def count(self, index: IndexName) -> int: ...


class VectorStore(Protocol):
    """Separate physical vector indices; implementations must enforce publication gates."""

    async def begin(self, publication: VectorPublication) -> None:
        """Exclusively create a fresh, incomplete index. Never overwrite or adopt one."""
        ...

    async def write(
        self, publication: VectorPublication, records: tuple[EmbeddingMutation, ...]
    ) -> None:
        """Create every record (including retained tombstones), or fail the attempt."""
        ...

    async def complete(self, publication: VectorPublication) -> None:
        """Block writes, refresh, verify counts, then persist completion. No alias changes."""
        ...

    async def neighbors(self, command: SearchVectorNeighbors) -> VectorNeighbors:
        """Verify selected identity, completion, block and exact seed provenance before ANN.

        Missing/deleted seeds return no vector hits. Malformed, partial or mismatched
        responses fail closed; they do not silently become a missing vector.
        """
        ...
