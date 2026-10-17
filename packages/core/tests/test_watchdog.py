from datetime import timedelta
from typing import Literal

import pytest

from openknowledge_core.ingestion import WatchdogService
from openknowledge_core.ingestion.models import AppliedRelease
from tests.fakes.ports import RELEASE, FakeApplied, applied_release, clock

MAX_AGE = timedelta(days=14)
OLDER = "2026-08-01"


def watchdog(*records: AppliedRelease) -> WatchdogService:
    fake = FakeApplied()
    fake.records.extend(records)
    return WatchdogService(applied=fake, clock=clock)


@pytest.mark.parametrize(
    ("age", "outcome"),
    [
        (timedelta(days=4), "fresh"),
        (MAX_AGE, "fresh"),
        (MAX_AGE + timedelta(seconds=1), "stale"),
        (timedelta(days=40), "stale"),
        (None, "never loaded"),
    ],
    ids=["fresh", "on-the-boundary", "just-past-it", "stale", "never-loaded"],
)
async def test_a_table_is_stale_only_when_applied_more_than_max_age_ago(
    age: timedelta | None, outcome: Literal["fresh", "stale", "never loaded"]
) -> None:
    records = () if age is None else (applied_release("papers", RELEASE, age),)
    result = await watchdog(*records).freshness(["papers"], MAX_AGE)
    assert result.stale == (records if outcome == "stale" else ())
    assert result.never_loaded == (("papers",) if outcome == "never loaded" else ())
    assert result.checked_at == clock()


async def test_only_the_latest_release_of_each_table_counts() -> None:
    old_authors = applied_release("authors", OLDER, timedelta(days=70))
    service = watchdog(
        applied_release("papers", OLDER, timedelta(days=70)),
        applied_release("papers", RELEASE, timedelta(days=4)),
        old_authors,
    )
    result = await service.freshness(["papers", "authors", "tldrs"], MAX_AGE)
    assert (result.stale, result.never_loaded) == ((old_authors,), ("tldrs",))
