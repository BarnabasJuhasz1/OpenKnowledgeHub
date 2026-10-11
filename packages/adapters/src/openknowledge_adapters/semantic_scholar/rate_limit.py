from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


class AsyncRateLimiter:
    """Keeps calls at least ``interval`` seconds apart, across the tasks of one event loop."""

    def __init__(
        self,
        interval: float,
        *,
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._interval = interval
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._next_allowed = clock()

    async def wait(self) -> None:
        async with self._lock:
            delay = self._next_allowed - self._clock()
            if delay > 0:
                await self._sleep(delay)
            self._next_allowed = self._clock() + self._interval
