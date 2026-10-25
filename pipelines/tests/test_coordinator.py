"""Coordinator behavior without a Prefect server, Batch jobs or network sockets."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from prefect import flow
from prefect.client.schemas.objects import ConcurrencyLimitStrategy
from prefect.client.schemas.schedules import CronSchedule

from openknowledge_contracts.datasets import ReleaseMetadata
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion import (
    BatchDiffDownloadResult,
    BatchDownloadResult,
    DownloadDataset,
    DownloadDatasetService,
    DownloadDiff,
    LoadDataset,
    LoadDatasetResult,
    LoadDatasetService,
    LoadDiff,
    LoadDiffResult,
)
from openknowledge_core.ingestion.archive_progress import ArchiveProgress
from openknowledge_core.ingestion.citation_edges import (
    EDGE_TABLES,
    BuildCitationEdgesResult,
    CitationEdgeTableResult,
)
from openknowledge_core.ingestion.coordinator import (
    CORE_ORDER,
    LARGE_ORDER,
    ArchiveBaseline,
    SyncCommand,
    SyncIncomplete,
)
from openknowledge_core.ingestion.errors import (
    CredentialRejected,
    IngestionError,
    SharedCapacityError,
    TableRejected,
)
from openknowledge_core.ingestion.models import TableState
from openknowledge_pipelines import deployments
from openknowledge_pipelines.flows import s2_batch_download, s2_sync
from openknowledge_pipelines.flows.s2_batch_download import S2BatchDownloadFlows
from openknowledge_pipelines.flows.s2_load import S2LoadFlows
from openknowledge_pipelines.settings import ScheduleSettings
from tests.fakes.ports import (
    NEXT,
    PRODUCER,
    RELEASE,
    FakeApplied,
    FakeJobs,
    FakeSource,
    FakeStore,
    FakeTables,
    applied_release,
    clock,
    gz,
)

TARGET = "2026-10-20"


class SyncSource(FakeSource):
    async def resolve_release(self, selector: str) -> ReleaseMetadata:
        metadata = await super().resolve_release(selector)
        # Different advertised dates, including a fulltext release newer than core.
        allowed = ("s2orc_v2",) if selector == TARGET else (*CORE_ORDER, "embeddings-specter_v2")
        return metadata.model_copy(
            update={"datasets": tuple(item for item in metadata.datasets if item.name in allowed)}
        )


class SyncTables(FakeTables):
    async def table(self, dataset: str) -> TableState:
        return (await super().table(dataset)).model_copy(
            update={"updates": CONTRACTS[dataset].updates}
        )

    async def live_rows(self, dataset: str, release: str) -> int:
        return 1  # one key in the snapshot fixture


class Cycle:
    def __init__(self) -> None:
        self.source = SyncSource()
        self.store = FakeStore()
        self.applied = FakeApplied()
        self.tables = SyncTables(self.store)
        self.tables.rows["seed"] = 1
        self.downloads = DownloadDatasetService(
            catalog=self.source,
            downloads=self.source,
            store=self.store,
            producer=PRODUCER,
            clock=clock,
        )
        self.jobs = FakeJobs(self.downloads, failing=set())
        self.progress = ArchiveProgress(self.store, self.downloads, PRODUCER, clock)
        self.loader = LoadDatasetService(
            store=self.store,
            tables=self.tables,
            applied=self.applied,
            producer=PRODUCER,
            clock=clock,
        )
        self.events: list[tuple[str, str, str]] = []
        self.active = 0
        self.leases = 0
        self.loading = False
        self.fail: str | None = None
        self.command = SyncCommand(bucket="raw", prefix="s2")

    @asynccontextmanager
    async def lease(self) -> AsyncGenerator[None]:
        assert self.active == 0, "nested source lease"
        self.active += 1
        self.leases += 1
        try:
            yield
        finally:
            self.active -= 1

    @asynccontextmanager
    async def load_lease(self) -> AsyncGenerator[None]:
        assert self.active == 0
        self.loading = True
        try:
            yield
        finally:
            self.loading = False

    async def initialize(self) -> None:
        self.source.releases = [RELEASE, NEXT, TARGET]
        for dataset in (*CORE_ORDER, *LARGE_ORDER):
            self.source.add(dataset, "part.gz", gz(b'{"id":1}\n'))
            base = DownloadDataset(bucket="raw", prefix="s2", dataset=dataset, release=RELEASE)
            pinned = await self.downloads.pin_dataset(base)
            for shard in pinned.shards:
                await self.downloads.archive_shard(pinned, shard)
            path = await self.downloads.write_manifest(pinned)
            if dataset in CORE_ORDER:
                self.command.core_baselines[dataset] = await self.progress.reference(path)
                self.applied.records.append(applied_release(dataset, RELEASE, timedelta()))
            else:
                anchor = await self.progress.finalize(base)
                self.command.archives[dataset] = ArchiveBaseline(base=base, completion=anchor)
            self.source.add_diff(dataset, RELEASE, NEXT, {}, {})
            self.source.add_diff(dataset, NEXT, TARGET, {}, {})

    def services(self) -> s2_sync.SyncServices:
        @flow
        async def archive_base(command: DownloadDataset) -> BatchDownloadResult:
            assert self.active == 1
            self.events.append(("archive", command.dataset, command.release))
            pinned = await self.downloads.pin_dataset(command)
            for shard in pinned.shards:
                await self.downloads.archive_shard(pinned, shard)
            manifest = await self.downloads.write_manifest(pinned)
            return BatchDownloadResult(
                release_id=command.release,
                dataset=command.dataset,
                shards=len(pinned.shards),
                jobs=(),
                manifest=manifest,
            )

        @flow
        async def archive_diff(command: DownloadDiff) -> BatchDiffDownloadResult:
            assert self.active == 1
            self.events.append(("archive", command.dataset, command.release))
            if self.fail == "access" and command.dataset == "s2orc_v2":
                raise CredentialRejected("access refused")
            if self.fail == "cancel":
                raise asyncio.CancelledError()
            if self.fail == "shared":
                raise SharedCapacityError("foreign job")
            if self.fail == "archive" and command.dataset == "papers":
                raise IngestionError("archive failed")
            pinned = await self.downloads.pin_diff(command)
            manifests: list[ObjectPath] = []
            for part in (pinned.updates, pinned.deletes):
                if part.shards:
                    for shard in part.shards:
                        await self.downloads.archive_shard(part, shard)
                    manifests.append(await self.downloads.write_manifest(part))
            return BatchDiffDownloadResult(
                from_release=command.from_release,
                release_id=command.release,
                dataset=command.dataset,
                updates=len(pinned.updates.shards),
                deletes=len(pinned.deletes.shards),
                jobs=(),
                manifests=tuple(manifests),
            )

        @flow
        async def load_base(command: LoadDataset) -> LoadDatasetResult:
            self.events.append(("load", command.dataset, command.release))
            prepared = await self.loader.prepare(command)
            outcomes = [
                await self.loader.load_object(prepared, entry)
                for entry in prepared.manifest.entries
            ]
            return await self.loader.finish(prepared, outcomes)

        @flow
        async def load_diff(command: LoadDiff) -> LoadDiffResult:
            self.events.append(("load", command.dataset, command.release))
            if self.fail == "load" and command.dataset == "papers":
                raise IngestionError("load failed")
            prepared = await self.loader.prepare_diff(command)
            outcomes = [
                await self.loader.load_object(part, entry)
                for part in prepared.parts
                for entry in part.manifest.entries
            ]
            return await self.loader.finish_diff(prepared, outcomes)

        async def edges_current(release: str) -> bool:
            assert self.loading, "freshness must be checked under the shared lease"
            built = [await self.applied.latest(table) for table in EDGE_TABLES]
            return all(
                row is not None and row.release == release and row.target_uuid == self.tables.uuid
                for row in built
            )

        async def build_edges() -> BuildCitationEdgesResult:
            assert self.loading
            if self.fail == "edges":
                raise TableRejected("not enough disk")
            citations = await self.applied.latest("citations")
            assert citations is not None
            self.events.append(("edges", "citations", citations.release))
            for table in EDGE_TABLES:
                await self.applied.record(applied_release(table, citations.release, timedelta()))
            built = CitationEdgeTableResult(rows=1, seconds=0)
            return BuildCitationEdgesResult(
                source_release=citations.release, citation_edges_out=built, citation_edges_in=built
            )

        return s2_sync.SyncServices(
            jobs=self.jobs,
            catalog=self.source,
            tables=self.tables,
            applied=self.applied,
            progress=self.progress,
            downloads=dict.fromkeys(
                (*CORE_ORDER, *LARGE_ORDER), S2BatchDownloadFlows(archive_base, archive_diff)
            ),
            loads=S2LoadFlows(load_base, load_diff),
            s2_lease=self.lease,
            load_lease=self.load_lease,
            edges=AsyncMock(is_current=edges_current),
            build_edges=build_edges,
        )


@pytest.fixture
async def cycle(monkeypatch: pytest.MonkeyPatch) -> Cycle:
    # These policy tests call run_sync directly, outside a Prefect run. The real
    # recovery logger and its persisted run association are exercised in test_flow.
    monkeypatch.setattr(s2_batch_download, "get_run_logger", lambda: logging.getLogger(__name__))
    cycle = Cycle()
    await cycle.initialize()
    return cycle


async def test_two_transitions_snapshot_order_distinct_targets_and_noop(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verify = AsyncMock(wraps=cycle.progress.baseline_manifest)
    monkeypatch.setattr(cycle.progress, "baseline_manifest", verify)
    # Core initially missed two transitions. Fulltext has its own newer target.
    cycle.source.releases.insert(1, "2026-10-10")
    for dataset in CORE_ORDER:
        cycle.source.diffs[dataset] = []
        cycle.source.add_diff(dataset, RELEASE, "2026-10-10", {}, {})
        cycle.source.add_diff(dataset, "2026-10-10", NEXT, {}, {})
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    result = await s2_sync.run_sync(command, cycle.services())
    assert [item.final for item in result.datasets] == [NEXT] * 7 + [TARGET, NEXT]
    assert [(step.start, step.release) for step in result.datasets[0].loaded] == [
        (RELEASE, "2026-10-10"),
        ("2026-10-10", NEXT),
    ]
    assert [call.args[1] for call in verify.await_args_list] == [
        dataset for dataset in CORE_ORDER if CONTRACTS[dataset].updates == "diffs"
    ]
    assert cycle.tables.marked == [NEXT]
    assert [dataset for action, dataset, _ in cycle.events if action == "archive"] == [
        dataset
        for dataset in CORE_ORDER
        for _ in range(1 if CONTRACTS[dataset].updates == "snapshot" else 2)
    ] + ["s2orc_v2", "s2orc_v2", "embeddings-specter_v2"]
    assert cycle.leases == 34
    cycle.events.clear()
    again = await s2_sync.run_sync(command, cycle.services())
    assert all(item.outcome == "current" for item in again.datasets)
    assert cycle.events == []


@pytest.mark.parametrize("failure", ["archive", "load", "guard"])
async def test_core_failure_preserves_checkpoints_and_blocks_archives(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    cycle.fail = failure
    if failure == "guard":
        cycle.source.diffs["papers"] = []
        cycle.source.add_diff("papers", RELEASE, NEXT, {"update.gz": gz(b'{"id":2}\n')}, {})
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    with pytest.raises(SyncIncomplete) as error:
        await s2_sync.run_sync(command, cycle.services())
    papers, *others = error.value.result.datasets
    assert papers.final == RELEASE
    assert len(papers.archived) == (0 if failure == "archive" else 1)
    applied = await cycle.applied.latest("papers")
    assert applied is not None and applied.release == RELEASE
    assert all(item.final == NEXT for item in others)
    assert not any(dataset in LARGE_ORDER for _, dataset, _ in cycle.events)


async def test_old_citation_edges_rebuild_at_final_release_before_archives(cycle: Cycle) -> None:
    for table in EDGE_TABLES:
        await cycle.applied.record(applied_release(table, RELEASE, timedelta()))
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    services = cycle.services()
    async with cycle.load_lease():
        assert await services.edges.is_current(RELEASE)
        assert not await services.edges.is_current(NEXT)
    result = await s2_sync.run_sync(command, services)
    citations = next(item for item in result.datasets if item.dataset == "citations")
    assert (citations.start, citations.final) == (RELEASE, NEXT)
    assert result.citation_edges is not None and result.citation_edges.source_release == NEXT
    assert [event for event in cycle.events if event[0] == "edges"] == [
        ("edges", "citations", NEXT)
    ]
    built = cycle.events.index(("edges", "citations", NEXT))
    assert built > cycle.events.index(("load", "citations", NEXT))
    archives = [
        index for index, (_, dataset, _) in enumerate(cycle.events) if dataset in LARGE_ORDER
    ]
    assert archives and min(archives) > built
    for table in EDGE_TABLES:
        marker = await cycle.applied.latest(table)
        assert marker is not None and marker.release == NEXT


async def test_citation_edges_rebuild_once_per_citations_release(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    cycle.fail = "edges"
    with pytest.raises(SyncIncomplete, match="citation edges: not enough disk"):
        await s2_sync.run_sync(command, cycle.services())
    assert not any(dataset in LARGE_ORDER for _, dataset, _ in cycle.events)
    cycle.fail, cycle.events = None, []
    await cycle.applied.record(applied_release(EDGE_TABLES[0], NEXT, timedelta()))
    assert await cycle.applied.latest(EDGE_TABLES[1]) is None
    result = await s2_sync.run_sync(command, cycle.services())
    assert result.citation_edges is not None and result.citation_edges.source_release == NEXT
    assert cycle.events[0] == ("edges", "citations", NEXT)
    cycle.events.clear()
    await s2_sync.run_sync(command, cycle.services())
    assert cycle.events == []


@pytest.mark.parametrize("failure", ["shared", "cancel"])
async def test_shared_capacity_and_cancellation_stop_immediately(
    cycle: Cycle, failure: str
) -> None:
    cycle.fail = failure
    with pytest.raises(SharedCapacityError if failure == "shared" else asyncio.CancelledError):
        await s2_sync.run_sync(cycle.command, cycle.services())
    assert cycle.events == [("archive", "papers", NEXT)]


async def test_large_access_failure_keeps_core_progress_and_stops_next_archive(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    cycle.fail = "access"
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    with pytest.raises(SyncIncomplete) as error:
        await s2_sync.run_sync(command, cycle.services())
    assert all(item.final == NEXT for item in error.value.result.datasets[:7])
    assert error.value.result.datasets[-1].reason == "access refused"
    assert not any(dataset == "embeddings-specter_v2" for _, dataset, _ in cycle.events)
    baseline = command.archives["s2orc_v2"]
    assert await cycle.progress.head(baseline.base, baseline.completion) == (
        RELEASE,
        baseline.completion,
    )


@pytest.mark.parametrize("enabled", [False, True])
def test_deployment_schedule_switch_and_existing_runner_budget(
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
) -> None:
    monkeypatch.delenv("OKP_SYNC_SCHEDULE_ENABLED", raising=False)
    assert not ScheduleSettings().sync_schedule_enabled
    monkeypatch.setenv("OKP_SYNC_SCHEDULE_ENABLED", str(enabled).lower())
    deployment = deployments.sync_deployment()
    assert deployment.schedules is not None
    assert len(deployment.schedules) == 1
    schedule = deployment.schedules[0]
    assert schedule.active is enabled
    assert isinstance(schedule.schedule, CronSchedule)
    assert schedule.schedule.cron == "30 1 * * *"
    assert schedule.schedule.timezone == "Europe/Zurich"
    assert deployment.concurrency_limit == 1
    assert deployment.concurrency_options is not None
    assert deployment.concurrency_options.collision_strategy == ConcurrencyLimitStrategy.CANCEL_NEW
    assert deployments.RUNS_AT_ONCE == 3


async def test_large_stages_need_both_enablement_and_baseline(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    command = cycle.command.model_copy(update={"archives": {}})
    result = await s2_sync.run_sync(command, cycle.services())
    assert [item.outcome for item in result.datasets[-2:]] == ["disabled", "disabled"]
    cycle.events.clear()
    with pytest.raises(SyncIncomplete, match="explicit verified base completion"):
        await s2_sync.run_sync(
            command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)}),
            cycle.services(),
        )
    assert cycle.events == []


@pytest.mark.parametrize(
    ("operation", "orphan"),
    [("catalogue", "foreign"), ("diffs", "queued"), ("fulltext", "running")],
)
async def test_orphan_recovery_precedes_source_calls(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    orphan: str,
) -> None:
    from openknowledge_core.ingestion.families import ArchiveFamily
    from openknowledge_core.ingestion.models import DiffChain, JobOutcome

    order: list[str] = []
    pending = True
    inspections = 0

    async def inspect(family: ArchiveFamily) -> dict[str, bool]:
        nonlocal inspections
        inspections += 1
        assert cycle.active == 1
        if operation == "diffs" and inspections <= 1 + len(CORE_ORDER):
            return {}
        if operation == "fulltext" and family != ArchiveFamily.FULLTEXT:
            return {}
        if not pending:
            return {}
        order.append("inspect")
        if orphan == "foreign":
            raise SharedCapacityError("foreign/unlabelled active job")
        return {"orphan": orphan == "queued"}

    async def delete(name: str) -> None:
        assert cycle.active == 1
        order.append("delete")

    async def wait(name: str) -> JobOutcome:
        nonlocal pending
        assert cycle.active == 1
        order.append("wait")
        pending = False
        return JobOutcome.FAILED

    original = cycle.source.list_diffs

    async def diffs(start: str, end: str, dataset: str) -> DiffChain:
        assert not pending and cycle.active == 1
        order.append("source")
        return await original(start, end, dataset)

    catalogue = AsyncMock(return_value=tuple(cycle.source.releases))
    monkeypatch.setattr(cycle.jobs, "running", inspect)
    monkeypatch.setattr(cycle.jobs, "delete", delete)
    monkeypatch.setattr(cycle.jobs, "wait", wait)
    monkeypatch.setattr(cycle.source, "list_releases", catalogue)
    monkeypatch.setattr(cycle.source, "list_diffs", diffs)
    if operation == "catalogue":
        with pytest.raises(SharedCapacityError):
            await s2_sync.run_sync(cycle.command, cycle.services())
        catalogue.assert_not_awaited()
        assert order == ["inspect"]
    elif operation == "diffs":
        for dataset in CORE_ORDER[1:]:
            cycle.applied.records.append(applied_release(dataset, NEXT, timedelta()))
        await s2_sync.run_sync(cycle.command, cycle.services())
        assert order == ["inspect", "delete", "wait", "source", "source"]
    else:
        for dataset in CORE_ORDER:
            cycle.applied.records.append(applied_release(dataset, NEXT, timedelta()))
        command = cycle.command.model_copy(update={"enabled_archives": frozenset({"s2orc_v2"})})
        await s2_sync.run_sync(command, cycle.services())
        assert order == ["inspect", "wait", "source", "source", "source"]


async def test_competing_edge_builder_finishes_during_lease_acquisition(cycle: Cycle) -> None:
    # Settle core loads, then invalidate one marker to require an edge rebuild.
    await s2_sync.run_sync(cycle.command, cycle.services())
    cycle.applied.records = [row for row in cycle.applied.records if row.target != EDGE_TABLES[1]]
    cycle.events.clear()

    @asynccontextmanager
    async def competing_lease() -> AsyncGenerator[None]:
        assert not cycle.loading
        # The competing builder publishes before handing over the shared slot.
        await cycle.applied.record(applied_release(EDGE_TABLES[1], NEXT, timedelta()))
        async with cycle.load_lease():
            yield

    services = replace(cycle.services(), load_lease=competing_lease)
    result = await s2_sync.run_sync(cycle.command, services)
    assert result.citation_edges is None
    assert cycle.events == []


@pytest.mark.parametrize("cancel", [False, True])
async def test_edge_freshness_failure_blocks_archives(
    cycle: Cycle, monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    services = cycle.services()
    check = AsyncMock(
        side_effect=asyncio.CancelledError() if cancel else TableRejected("read denied")
    )
    monkeypatch.setattr(services.edges, "is_current", check)
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    if cancel:
        with pytest.raises(asyncio.CancelledError):
            await s2_sync.run_sync(command, services)
    else:
        with pytest.raises(SyncIncomplete, match="citation edges: read denied"):
            await s2_sync.run_sync(command, services)
    assert not cycle.loading
    assert not any(
        action == "edges" or dataset in LARGE_ORDER for action, dataset, _ in cycle.events
    )
