from datetime import timedelta

import pytest
from pydantic import ValidationError

from openknowledge_core.ingestion import TableFreshness, WatchdogService
from openknowledge_core.ingestion.models import AppliedRelease
from tests.fakes.ports import RELEASE, FakeApplied, applied_release, clock

MAX_AGE = timedelta(days=10)
OLDER = "2026-08-01"


def watchdog(*records: AppliedRelease) -> WatchdogService:
    fake = FakeApplied()
    fake.records.extend(records)
    return WatchdogService(applied=fake, clock=clock)


@pytest.mark.parametrize(
    ("age", "stale"),
    [
        (timedelta(days=4), False),
        (MAX_AGE, False),
        (MAX_AGE + timedelta(seconds=1), True),
        (timedelta(days=40), True),
    ],
    ids=["fresh", "on-the-boundary", "just-past-it", "stale"],
)
async def test_a_table_is_stale_only_when_applied_more_than_max_age_ago(
    age: timedelta, stale: bool
) -> None:
    service = watchdog(applied_release("papers", RELEASE, age))
    result = await service.freshness(["papers"], MAX_AGE)
    assert result.checked == (
        TableFreshness(table="papers", release=RELEASE, applied_at=clock() - age, stale=stale),
    )
    assert result.stale == (("papers",) if stale else ())
    assert result.checked_at == clock()


async def test_a_table_never_loaded_is_not_stale() -> None:
    result = await watchdog().freshness(["papers"], MAX_AGE)
    assert result.checked == (
        TableFreshness(table="papers", release=None, applied_at=None, stale=False),
    )
    assert result.stale == ()


async def test_only_the_latest_release_of_each_table_counts() -> None:
    service = watchdog(
        applied_release("papers", OLDER, timedelta(days=70)),
        applied_release("papers", RELEASE, timedelta(days=4)),
        applied_release("authors", OLDER, timedelta(days=70)),
    )
    result = await service.freshness(["papers", "authors", "tldrs"], MAX_AGE)
    assert [(found.table, found.release, found.stale) for found in result.checked] == [
        ("papers", RELEASE, False),
        ("authors", OLDER, True),
        ("tldrs", None, False),
    ]
    assert result.stale == ("authors",)


def test_a_stale_table_without_a_release_is_refused() -> None:
    with pytest.raises(ValidationError, match="a stale table has a release"):
        TableFreshness(table="papers", release=None, applied_at=None, stale=True)
