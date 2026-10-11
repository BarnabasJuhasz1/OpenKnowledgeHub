import asyncio

from openknowledge_adapters.semantic_scholar.rate_limit import AsyncRateLimiter
from tests.fakes.s2_http import FakeClock


async def test_concurrent_callers_are_spaced_one_interval_apart() -> None:
    clock = FakeClock()
    limiter = AsyncRateLimiter(1.0, clock=clock, sleep=clock.sleep)
    await asyncio.gather(limiter.wait(), limiter.wait(), limiter.wait())
    assert clock.sleeps == [1.0, 1.0]


async def test_a_caller_after_a_long_pause_goes_straight_through() -> None:
    clock = FakeClock()
    limiter = AsyncRateLimiter(1.0, clock=clock, sleep=clock.sleep)
    await limiter.wait()
    clock.now += 5
    await limiter.wait()
    assert clock.sleeps == []
