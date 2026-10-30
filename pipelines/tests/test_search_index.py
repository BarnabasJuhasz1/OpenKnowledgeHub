from collections.abc import AsyncGenerator
from uuid import UUID

import pytest
from prefect import flow
from prefect.events import DeploymentEventTrigger

from openknowledge_core.search import SearchIndexService, SourceRows
from openknowledge_core.search.errors import BulkRefused
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
async def test_failed_part_resumes_and_alias_switch_follows_settle() -> None:
    source, sink, progress = RangedSource(), SettledSink(), Progress()
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
