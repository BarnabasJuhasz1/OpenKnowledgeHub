"""Report each table whose last load was applied longer ago than the watchdog allows.

Stale tables are listed in the result rather than raised, so the caller can write one alert line
per table and still complete.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from openknowledge_core.ingestion.commands import WatchdogResult
from openknowledge_core.ingestion.models import TableFreshness
from openknowledge_core.ingestion.ports import AppliedReleases


class WatchdogService:
    """Freshness comes from the applied log, not from the table contents."""

    def __init__(self, *, applied: AppliedReleases, clock: Callable[[], datetime]) -> None:
        self._applied = applied
        self._clock = clock

    async def freshness(self, tables: Sequence[str], max_age: timedelta) -> WatchdogResult:
        """Each table's last applied release, stale when applied more than ``max_age`` ago.

        A table never loaded is not stale: the heartbeat covers a system that never started.
        """
        now = self._clock()
        checked: list[TableFreshness] = []
        for table in tables:
            latest = await self._applied.latest(table)
            checked.append(
                TableFreshness(
                    table=table,
                    release=None if latest is None else latest.release,
                    applied_at=None if latest is None else latest.applied_at,
                    stale=latest is not None and now - latest.applied_at > max_age,
                )
            )
        return WatchdogResult(
            checked_at=now,
            checked=tuple(checked),
            stale=tuple(found.table for found in checked if found.stale),
        )
