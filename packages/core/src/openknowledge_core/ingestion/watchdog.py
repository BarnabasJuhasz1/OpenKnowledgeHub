"""Report each table whose last load was applied longer ago than the watchdog allows.

Stale tables are listed in the result rather than raised, so the caller can write one alert line
per table and still complete.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from openknowledge_core.ingestion.commands import WatchdogResult
from openknowledge_core.ingestion.models import AppliedRelease
from openknowledge_core.ingestion.ports import AppliedReleases


class WatchdogService:
    """Freshness comes from the applied log, not from the table contents."""

    def __init__(self, *, applied: AppliedReleases, clock: Callable[[], datetime]) -> None:
        self._applied = applied
        self._clock = clock

    async def freshness(self, tables: Sequence[str], max_age: timedelta) -> WatchdogResult:
        """The last record of each table loaded over ``max_age`` ago, and the tables never loaded.

        A table never loaded is not stale: the heartbeat covers a system that never started.
        """
        now = self._clock()
        stale: list[AppliedRelease] = []
        never_loaded: list[str] = []
        for table in tables:
            latest = await self._applied.latest(table)
            if latest is None:
                never_loaded.append(table)
            elif now - latest.applied_at > max_age:
                stale.append(latest)
        return WatchdogResult(checked_at=now, stale=tuple(stale), never_loaded=tuple(never_loaded))
