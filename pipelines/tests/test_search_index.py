from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from uuid import UUID

import pytest
from prefect import flow
from prefect.events import DeploymentEventTrigger

from openknowledge_core.ingestion.errors import ContractViolation
from openknowledge_core.search import SearchIndexService, SourceRows
from openknowledge_core.search.commands import IndexPapers, IndexPapersResult
from openknowledge_core.search.errors import BulkRefused, SearchRejected
from openknowledge_core.search.progress import Indexed, SearchState
from openknowledge_pipelines import admission
from openknowledge_pipelines.flows.search_index import SearchIndexResult, run_index
from openknowledge_pipelines.index_deployment import index_deployment
from tests.fakes.ports import FakeSearchSink, FakeSearchSource, paper_row


class RangedSource(FakeSearchSource):
    async def base(self, release: str, low: int, high: int) -> AsyncGenerator[SourceRows]:
        self.asked.append((None, release, low, high))
        return self._stream([r for r in self.base_rows if low <= r.corpusid <= high])


class SettledSink(FakeSearchSink):
    async def switch_alias(self, alias: str, index: str) -> None:
        assert admission.status().active_slots == 1
        assert self.intervals[index] is None and index in self.refreshed
        await super().switch_alias(alias, index)


class Progress:
    def __init__(self) -> None:
        self.rows: list[Indexed] = []

    async def state(self) -> SearchState:
        return SearchState(
            releases=dict.fromkeys(("papers", "abstracts", "tldrs"), "2026-10-06"),
            sources={
                name: UUID(int=i) for i, name in enumerate(("papers", "abstracts", "tldrs"), 1)
            },
        )

    async def receipts(self) -> list[Indexed]:
        return self.rows

    async def record(self, receipt: Indexed) -> None:
        self.rows.append(receipt)


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize("changed_sources", [False, True])
async def test_failed_part_resumes_and_alias_switch_follows_settle(changed_sources: bool) -> None:
    class RunProgress(Progress):
        async def state(self) -> SearchState:
            state = await super().state()
            if changed_sources and sum(r.kind == "part" for r in self.rows) == 3:
                return state.model_copy(
                    update={"sources": {**state.sources, "papers": UUID(int=99)}}
                )
            return state

    source, sink, progress = RangedSource(), SettledSink(), RunProgress()
    source.base_rows = [SourceRows(corpusid=i, paper=paper_row(i, 10)) for i in (1, 2, 3)]
    sink.refused = "2"
    service = SearchIndexService(source=source, sink=sink, shards=1)

    @flow(persist_result=False)
    async def run() -> SearchIndexResult:
        return await run_index(
            service,
            source,
            sink,
            progress,
            execution=lambda: admission.execution(release_on_exception=True),
            base_parts=3,
            diff_parts=3,
            parallelism=1,
        )

    with pytest.raises(BulkRefused):
        await run()
    assert [r.part for r in progress.rows if r.kind == "part"] == [1]
    assert admission.status().active_slots == 0
    assert sink.target is None and not sink.refreshed
    original_bounds = progress.rows[0].plan
    sink.refused = None
    if changed_sources:
        with pytest.raises(SearchRejected, match="source tables changed during indexing"):
            await run()
        assert sink.target is None and not sink.refreshed
        assert not any(r.kind == "complete" for r in progress.rows)
        assert admission.status().active_slots == 0
        return
    result = await run()
    assert result.outcome == "base"
    assert [r.part for r in progress.rows if r.kind == "part"] == [1, 2, 3]
    assert all(r.plan == original_bounds for r in progress.rows)
    assert sink.intervals == {result.index: None}
    assert sink.refreshed == [result.index] and sink.target == result.index
    before = list(sink.requests)
    assert (await run()).outcome == "current" and sink.requests == before


def test_index_deployment_queues_and_triggers_from_loads() -> None:
    deployment = index_deployment()
    assert deployment.name == "opensearch-vm"
    assert deployment.concurrency_limit == 1
    assert deployment.schedules is not None and len(deployment.schedules) == 1
    trigger = deployment.triggers[0]
    assert isinstance(trigger, DeploymentEventTrigger)
    assert trigger.expect == {"prefect.flow-run.Completed", "prefect.flow-run.Failed"}
    assert trigger.model_dump()["match_related"] == {
        "prefect.resource.role": "flow",
        "prefect.resource.name": ["s2-load", "s2-load-diff", "s2-sync"],
    }


def test_index_serve_initializes_only_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    from openknowledge_pipelines import admission
    from openknowledge_pipelines import index_deployment as module

    initialized: list[str] = []
    monkeypatch.setattr(admission, "ensure_closed", lambda: initialized.append("admission"))

    def served(*deployments: object, limit: int) -> None:
        assert len(deployments) == limit == 1
        assert initialized == ["admission"]

    monkeypatch.setattr(module, "prefect_serve", served)
    module.serve()


@pytest.mark.usefixtures("prefect_server")
async def test_deploy_closes_admission_running_parts_drain_and_rerun_resumes() -> None:
    class ClosingProgress(Progress):
        async def record(self, receipt: Indexed) -> None:
            await super().record(receipt)
            if receipt.kind == "part" and receipt.part == 1:
                status = admission.status("close")
                assert status.active_slots == 1

    source, sink, progress = RangedSource(), SettledSink(), ClosingProgress()
    source.base_rows = [SourceRows(corpusid=i, paper=paper_row(i, 10)) for i in range(1, 5)]
    service = SearchIndexService(source=source, sink=sink, shards=1)

    @flow(persist_result=False)
    async def run() -> SearchIndexResult:
        return await run_index(
            service,
            source,
            sink,
            progress,
            execution=lambda: admission.execution(release_on_exception=True),
            base_parts=4,
            diff_parts=4,
            parallelism=1,
        )

    try:
        with pytest.raises(admission.AdmissionError, match="execution refused"):
            await run()
        status = admission.status()
        assert status.closed and status.active_slots == 0
        assert sorted(r.part for r in progress.rows if r.kind == "part") == [1]
        assert sink.target is None
    finally:
        admission.status("open")
    await run()
    assert sorted(r.part for r in progress.rows if r.kind == "part") == [1, 2, 3, 4]
    assert len(source.asked) == 4 and admission.status().active_slots == 0


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize("mode", ["exit", "sigkill"])
async def test_lost_worker_joins_sibling_retains_admission_and_never_publishes(
    tmp_path: Path, mode: str
) -> None:
    import asyncio
    import os
    from contextlib import AsyncExitStack, asynccontextmanager
    from functools import partial

    from openknowledge_pipelines.flows.process_parts import PartProcesses, PartWorkerLost
    from tests.fakes.search_parts import crash_with_sibling

    source, sink, progress = RangedSource(), SettledSink(), Progress()
    source.base_rows = [SourceRows(corpusid=i, paper=paper_row(i, 10)) for i in (1, 2)]
    workers = PartProcesses(partial(crash_with_sibling, str(tmp_path), mode), 2)
    service = SearchIndexService(source=source, sink=sink, shards=1)

    @asynccontextmanager
    async def execution() -> AsyncGenerator[None]:
        # Admission is reserved for the whole run below, before task/flow state writes
        # can contend with counter mutations in the temporary SQLite server.
        yield

    @flow(persist_result=False)
    async def run() -> SearchIndexResult:
        return await run_index(
            service,
            source,
            sink,
            progress,
            execution=execution,
            base_parts=2,
            diff_parts=2,
            parallelism=2,
            index=workers.index,
        )

    try:
        with pytest.raises(PartWorkerLost, match="admission retained"):
            async with AsyncExitStack() as reserved:
                for _ in range(2):
                    await reserved.enter_async_context(
                        admission.execution(release_on_exception=True)
                    )
                assert admission.status().active_slots == 2
                # The sibling sleeps for 30s: natural completion must not satisfy join.
                async with asyncio.timeout(20):
                    await run()
        # Observe the admission boundary BEFORE the outer bootstrap closes its pool.
        assert admission.status().active_slots == 2
        for part in (1, 2):
            pid = int((tmp_path / f"{part}.started").read_text())
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        assert sink.target is None and not sink.refreshed
        assert [r.kind for r in progress.rows] == ["started"]
    finally:
        await workers.close()
        admission.status("close")
        held = admission.status().active_slots
        if held:
            admission.release(held)
        admission.status("open")


@flow(persist_result=False)
async def run_retry_parts(
    service: SearchIndexService,
    source: RangedSource,
    sink: SettledSink,
    progress: Progress,
    index: Callable[[IndexPapers], Awaitable[IndexPapersResult]],
    parts: int = 1,
) -> SearchIndexResult:
    return await run_index(
        service,
        source,
        sink,
        progress,
        execution=lambda: admission.execution(release_on_exception=True),
        base_parts=parts,
        diff_parts=parts,
        parallelism=1,
        index=index,
    )


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize("failure_count", [1, 4])
async def test_transient_part_retries_preserve_plan_and_receipts(
    monkeypatch: pytest.MonkeyPatch, failure_count: int
) -> None:
    from openknowledge_core.ingestion.errors import TableUnavailable
    from openknowledge_pipelines.flows import search_index

    assert search_index.PART_RETRY_DELAYS == [15, 30, 60]
    monkeypatch.setattr(search_index, "PART_RETRY_DELAYS", [0.01, 0.02, 0.03])
    source, sink, progress = RangedSource(), SettledSink(), Progress()
    source.base_rows = [SourceRows(corpusid=i, paper=paper_row(i, 10)) for i in (1, 2)]
    service = SearchIndexService(source=source, sink=sink, shards=1)
    attempts: list[IndexPapers] = []

    async def index(command: IndexPapers) -> IndexPapersResult:
        assert admission.status().active_slots == 1
        if command.part == 2:
            attempts.append(command)
            assert [r.part for r in progress.rows if r.kind == "part"] == [1]
            if len(attempts) <= failure_count:
                # Exercise idempotent replay after writes, rather than failure before any work.
                await service.index(command)
                raise TableUnavailable("ClickHouse query failed (RemoteProtocolError); rerun")
        return await service.index(command)

    if failure_count == 4:
        with pytest.raises(TableUnavailable):
            await run_retry_parts(service, source, sink, progress, index, parts=2)
        assert [r.part for r in progress.rows if r.kind == "part"] == [1]
        assert sink.target is None and not sink.refreshed
    else:
        await run_retry_parts(service, source, sink, progress, index, parts=2)
        assert [r.part for r in progress.rows if r.kind == "part"] == [1, 2]
        assert sink.target is not None
    assert len(attempts) == min(failure_count + 1, 4)
    assert all(command == attempts[0] for command in attempts)
    assert all(r.plan == progress.rows[0].plan for r in progress.rows)
    assert admission.status().active_slots == 0


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize(
    "failure",
    [ValueError("bug"), ContractViolation("invalid row"), SearchRejected("source changed")],
)
async def test_terminal_part_failure_is_not_retried(failure: Exception) -> None:
    source, sink, progress = RangedSource(), SettledSink(), Progress()
    service = SearchIndexService(source=source, sink=sink, shards=1)
    attempts = 0

    async def index(command: IndexPapers) -> IndexPapersResult:
        nonlocal attempts
        attempts += 1
        raise failure

    with pytest.raises(type(failure), match=str(failure)):
        await run_retry_parts(service, source, sink, progress, index)
    assert attempts == 1
    assert [r.kind for r in progress.rows] == ["started"]
    assert sink.target is None and admission.status().active_slots == 0


@pytest.mark.usefixtures("prefect_server")
async def test_retry_rechecks_admission_before_replaying_part(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openknowledge_core.ingestion.errors import TableUnavailable
    from openknowledge_pipelines.flows import search_index

    monkeypatch.setattr(search_index, "PART_RETRY_DELAYS", [0.01])
    source, sink, progress = RangedSource(), SettledSink(), Progress()
    service = SearchIndexService(source=source, sink=sink, shards=1)
    attempts = 0

    async def index(command: IndexPapers) -> IndexPapersResult:
        nonlocal attempts
        attempts += 1
        admission.status("close")
        raise TableUnavailable("temporary failure")

    try:
        with pytest.raises(admission.AdmissionError, match="execution refused"):
            await run_retry_parts(service, source, sink, progress, index)
        assert attempts == 1
        assert [r.kind for r in progress.rows] == ["started"]
        assert sink.target is None
        assert admission.status().closed and admission.status().active_slots == 0
    finally:
        admission.status("open")
