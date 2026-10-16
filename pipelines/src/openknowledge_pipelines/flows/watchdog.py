"""The watchdog flow: a heartbeat line, then one stale line per table whose last load is too old.

Both texts are fixed so that a log alert can match them: ``HEARTBEAT`` and ``STALE`` never change,
and no other line of this flow may contain either of them.
"""

from __future__ import annotations

from datetime import UTC, timedelta
from types import CoroutineType
from typing import Any

from prefect import Flow, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion import WatchdogResult, WatchdogService
from openknowledge_pipelines.flows.s2_download import retry_transient

type WatchdogFlow = Flow[[], CoroutineType[Any, Any, WatchdogResult]]

HEARTBEAT = "watchdog heartbeat"
STALE = "watchdog stale"
# Only tables that follow diffs; a snapshot table is loaded whole by hand, at its own pace.
TABLES = tuple(contract.table for contract in CONTRACTS.values() if contract.updates == "diffs")


def watchdog_flow(
    service: WatchdogService,
    *,
    max_age: timedelta,
    task_retries: int,
    retry_delay_seconds: float,
) -> WatchdogFlow:
    @task(name="heartbeat", cache_policy=NO_CACHE, persist_result=False)
    async def heartbeat() -> None:
        """Logged before any ClickHouse query, so it proves only that ``okp serve`` schedules."""
        get_run_logger().info(HEARTBEAT)

    @task(
        name="check-freshness",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def check_freshness() -> WatchdogResult:
        logger = get_run_logger()
        result = await service.freshness(TABLES, max_age)
        for found in result.checked:
            # TableFreshness refuses a stale table without these, so the checks only narrow types.
            if found.stale and found.release is not None and found.applied_at is not None:
                logger.error(
                    "%s: %s holds %s, applied %s",
                    STALE,
                    found.table,
                    found.release,
                    found.applied_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC"),
                )
        never_loaded = sum(found.release is None for found in result.checked)
        logger.info(
            "watchdog checked %d tables: %d older than %d days, %d never loaded",
            len(result.checked),
            len(result.stale),
            max_age.days,
            never_loaded,
        )
        return result

    @flow(name="watchdog", persist_result=False)
    async def watchdog() -> WatchdogResult:
        await heartbeat()
        return await check_freshness()

    return watchdog
