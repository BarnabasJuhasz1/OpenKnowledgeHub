import logging
from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from typing import cast

import pytest
from prefect.client.schemas.objects import State

from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion import WatchdogResult, WatchdogService
from openknowledge_core.ingestion.errors import TableRejected, TableUnavailable
from openknowledge_core.ingestion.models import AppliedRelease
from openknowledge_pipelines.flows.watchdog import WatchdogFlow, watchdog_flow
from tests.fakes.ports import RELEASE, FakeApplied, applied_release, clock

pytestmark = pytest.mark.usefixtures("prefect_server")

TABLES = [contract.table for contract in CONTRACTS.values() if contract.updates == "diffs"]
HEARTBEAT = "watchdog heartbeat"
# 12:30:59 UTC in another zone, so the stale line shows the conversion and the minute precision.
STALE_AT = datetime(2026, 9, 5, 14, 30, 59, tzinfo=timezone(timedelta(hours=2)))


class WatchedApplied(FakeApplied):
    """Notes what had been logged when the first table was read."""

    def __init__(self, caplog: pytest.LogCaptureFixture) -> None:
        super().__init__()
        self.caplog = caplog
        self.logged_before_first_read: list[str] | None = None

    async def latest(self, target: str) -> AppliedRelease | None:
        if self.logged_before_first_read is None:
            self.logged_before_first_read = [record.getMessage() for record in self.caplog.records]
        return await super().latest(target)


class UnavailableApplied(WatchedApplied):
    """Fails every read, as the adapter does while ClickHouse cannot be reached."""

    def __init__(self, caplog: pytest.LogCaptureFixture) -> None:
        super().__init__(caplog)
        self.reads = 0

    async def latest(self, target: str) -> AppliedRelease | None:
        await super().latest(target)
        self.reads += 1
        raise TableUnavailable("ClickHouse query failed (ConnectError); rerun")


class RejectedApplied(FakeApplied):
    """Fails every read, as the adapter does when ClickHouse refuses the query for good."""

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    async def latest(self, target: str) -> AppliedRelease | None:
        self.reads += 1
        raise TableRejected(
            "ClickHouse query failed with code 60: Table openknowledge.applied does not exist"
        )


def build(fake: FakeApplied, *, task_retries: int = 2) -> WatchdogFlow:
    service = WatchdogService(applied=fake, clock=clock)
    return watchdog_flow(
        service, max_age=timedelta(days=10), task_retries=task_retries, retry_delay_seconds=0
    )


def run_state(flow: WatchdogFlow) -> Awaitable[State[WatchdogResult]]:
    # Flow is invariant in its return type, so pyright cannot pick the awaitable overload here.
    return cast("Awaitable[State[WatchdogResult]]", flow(return_state=True))


def lines(caplog: pytest.LogCaptureFixture, prefix: str) -> list[tuple[int, str]]:
    return [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.getMessage().startswith(prefix)
    ]


async def test_the_heartbeat_is_logged_before_any_table_is_read(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    fake = WatchedApplied(caplog)
    result = await build(fake)()
    assert fake.logged_before_first_read is not None
    assert HEARTBEAT in fake.logged_before_first_read
    assert lines(caplog, "watchdog") == [
        (logging.INFO, HEARTBEAT),
        (logging.INFO, "watchdog checked 6 tables: 0 older than 10 days, 6 never loaded"),
    ]
    assert [(found.table, found.release, found.stale) for found in result.checked] == [
        (table, None, False) for table in TABLES
    ]
    assert "publication_venues" not in [found.table for found in result.checked]
    assert (result.stale, result.checked_at) == ((), clock())


async def test_a_stale_table_is_reported_and_the_flow_still_completes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    fake = FakeApplied()
    stale = applied_release("papers", "2026-09-01", clock() - STALE_AT)
    fake.records += [
        stale.model_copy(update={"applied_at": STALE_AT}),
        applied_release("authors", RELEASE, timedelta(days=4)),
    ]
    state = await run_state(build(fake))
    assert state.is_completed()
    result = await state.aresult()
    assert result.stale == ("papers",)
    assert lines(caplog, "watchdog stale") == [
        (logging.ERROR, "watchdog stale: papers holds 2026-09-01, applied 2026-09-05 12:30 UTC")
    ]
    assert lines(caplog, "watchdog checked") == [
        (logging.INFO, "watchdog checked 6 tables: 1 older than 10 days, 4 never loaded")
    ]


async def test_an_unreachable_clickhouse_fails_the_run_after_the_heartbeat(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    fake = UnavailableApplied(caplog)
    state = await run_state(build(fake, task_retries=1))
    assert state.is_failed()
    with pytest.raises(TableUnavailable):
        await state.aresult()
    assert fake.logged_before_first_read is not None
    assert HEARTBEAT in fake.logged_before_first_read
    assert fake.reads == 2
    assert lines(caplog, "watchdog") == [(logging.INFO, HEARTBEAT)]


async def test_a_refused_read_fails_the_run_on_the_first_try(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    fake = RejectedApplied()
    state = await run_state(build(fake))
    assert state.is_failed()
    with pytest.raises(TableRejected):
        await state.aresult()
    assert fake.reads == 1
    assert lines(caplog, "watchdog") == [(logging.INFO, HEARTBEAT)]
