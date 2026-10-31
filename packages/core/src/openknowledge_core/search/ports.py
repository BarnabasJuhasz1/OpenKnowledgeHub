"""What the search use cases need from outside: the current rows, and an index to write."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from typing import Any, Protocol

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_core.search.models import BulkOperation, IndexName, SourceRows
from openknowledge_core.search.progress import Indexed, SearchState


class SearchSource(Protocol):
    async def bounds(self, release: ReleaseId) -> tuple[int, int]:
        """The smallest and the largest corpus ID at ``release``, which every part splits."""
        ...

    async def base(self, release: ReleaseId, low: int, high: int) -> AsyncGenerator[SourceRows]:
        """Latest live papers and text, one item per corpus ID from low to high.

        ``release`` identifies the pass whose bounds supplied the range, not a historical
        snapshot. Items come in no set order.
        """
        ...

    async def diff(
        self, from_release: ReleaseId, release: ReleaseId, low: int, high: int
    ) -> AsyncGenerator[SourceRows]:
        """The latest row per key in all three tables, tombstones included, for the changed keys.

        A key counts as changed when any table has a row after ``from_release``.
        ``release`` identifies the pass whose bounds supplied the range; rows have no upper
        release bound.
        """
        ...


class IndexedProgress(Protocol):
    async def state(self) -> SearchState: ...

    async def receipts(self) -> list[Indexed]: ...

    async def record(self, receipt: Indexed) -> None: ...


class SearchSink(Protocol):
    async def alias_target(self, alias: IndexName) -> IndexName | None: ...

    async def switch_alias(self, alias: IndexName, index: IndexName) -> None: ...

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
