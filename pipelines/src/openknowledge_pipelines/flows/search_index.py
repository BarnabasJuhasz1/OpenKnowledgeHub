"""Managed paper indexing with durable part receipts and bounded task concurrency."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime

from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pydantic import BaseModel

from openknowledge_core.search import IndexPapers, SearchIndexService, SettleIndex
from openknowledge_core.search.commands import IndexPapersResult
from openknowledge_core.search.errors import SearchRejected
from openknowledge_core.search.ports import IndexedProgress, SearchSink, SearchSource
from openknowledge_core.search.progress import Indexed, IndexPass, plan_index


class SearchIndexResult(BaseModel):
    outcome: str
    index: str | None = None


async def run_index(
    service: SearchIndexService,
    source: SearchSource,
    sink: SearchSink,
    progress: IndexedProgress,
    *,
    execution: Callable[[], AbstractAsyncContextManager[None]],
    base_parts: int,
    diff_parts: int,
    parallelism: int,
    index: Callable[[IndexPapers], Awaitable[IndexPapersResult]] | None = None,
) -> SearchIndexResult:
    receipts = await progress.receipts()
    decision = plan_index(
        await progress.state(), receipts, base_parts=base_parts, diff_parts=diff_parts
    )
    get_run_logger().info("Search index: %s", decision.reason)
    plan = decision.plan
    if plan is None:
        return SearchIndexResult(outcome=decision.reason)
    if decision.reason == "current":
        async with execution():
            if not await sink.exists(plan.index):
                raise SearchRejected(
                    f"index {plan.index} is missing; receipts in indexed point at it"
                )
            await sink.switch_alias("papers", plan.index)
        return SearchIndexResult(outcome="current", index=plan.index)
    if not any(r.plan == plan and r.kind == "started" for r in receipts):
        async with execution():
            low, high = await source.bounds(plan.release)
            plan = plan.model_copy(update={"low": low, "high": high})
            await progress.record(Indexed(plan=plan, kind="started", recorded_at=datetime.now(UTC)))
    if (plan.from_release is not None or decision.finished) and not await sink.exists(plan.index):
        raise SearchRejected(f"index {plan.index} is missing; receipts in indexed point at it")
    preparation = asyncio.Lock()
    prepared = False

    @task(
        name="index-part", task_run_name="part {part}", cache_policy=NO_CACHE, persist_result=False
    )
    async def index_part(part: int, plan: IndexPass) -> None:
        nonlocal prepared
        async with execution():
            if plan.from_release is None:
                async with preparation:
                    if not prepared:
                        await service.prepare(plan.index)
                        prepared = True
            await (index or service.index)(
                IndexPapers(
                    index=plan.index,
                    release=plan.release,
                    from_release=plan.from_release,
                    part=part,
                    parts=plan.parts,
                    bounds=(plan.low, plan.high),
                )
            )
            await progress.record(
                Indexed(plan=plan, kind="part", part=part, recorded_at=datetime.now(UTC))
            )

    pending = iter(part for part in range(1, plan.parts + 1) if part not in decision.finished)
    running: set[asyncio.Task[None]] = set()
    try:
        for _ in range(parallelism):
            if (part := next(pending, None)) is not None:
                running.add(asyncio.create_task(index_part(part, plan)))
        while running:
            done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            failures = [
                result
                for result in await asyncio.gather(*done, return_exceptions=True)
                if isinstance(result, BaseException)
            ]
            if failures:
                raise failures[0]
            for _ in done:
                if (part := next(pending, None)) is not None:
                    running.add(asyncio.create_task(index_part(part, plan)))
    finally:
        # Drain already started work before releasing clients or reporting failure.
        if running:
            drain = asyncio.gather(*running, return_exceptions=True)
            interrupted = False
            while not drain.done():
                try:
                    await asyncio.shield(drain)
                except asyncio.CancelledError:
                    interrupted = True
            if interrupted:
                raise asyncio.CancelledError
    async with execution():
        if (await progress.state()).sources != plan.sources:
            raise SearchRejected("source tables changed during indexing; rerun for a new base")
        if plan.from_release is None:
            await service.settle(SettleIndex(index=plan.index))
        else:
            await sink.refresh(plan.index)
        await sink.switch_alias("papers", plan.index)
        await progress.record(Indexed(plan=plan, kind="complete", recorded_at=datetime.now(UTC)))
    return SearchIndexResult(outcome=decision.reason, index=plan.index)
