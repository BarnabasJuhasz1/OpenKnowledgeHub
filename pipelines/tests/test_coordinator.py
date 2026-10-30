"""Coordinator behavior without a Prefect server, Batch jobs or network sockets."""

import asyncio
import logging
import signal
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from prefect import flow
from prefect.client.schemas.objects import ConcurrencyLimitStrategy
from prefect.client.schemas.schedules import CronSchedule
from prefect.exceptions import TerminationSignal

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
from openknowledge_core.ingestion.archive_progress import ArchiveProgress, HashedRef
from openknowledge_core.ingestion.coordinator import (
    CORE_ORDER,
    LARGE_ORDER,
    ArchiveBaseline,
    SyncCommand,
    SyncIncomplete,
)
from openknowledge_core.ingestion.errors import (
    BatchTimedOut,
    CredentialRejected,
    IngestionError,
    SharedCapacityError,
    TableRejected,
)
from openknowledge_core.ingestion.models import PreparedDiff, TableState
from openknowledge_core.ingestion.read_tables import (
    READ_TABLES,
    BuildReadTablesResult,
    ReadTableResult,
)
from openknowledge_pipelines import admission, bootstrap, deployments
from openknowledge_pipelines.flows import s2_batch_download, s2_sync
from openknowledge_pipelines.flows.s2_batch_download import S2BatchDownloadFlows
from openknowledge_pipelines.flows.s2_load import S2LoadFlows
from openknowledge_pipelines.settings import ScheduleSettings, SyncSettings
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
        assert self.active == 1
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
            built = [await self.applied.latest(table) for table in READ_TABLES]
            return all(
                row is not None and row.release == release and row.target_uuid == self.tables.uuid
                for row in built
            )

        async def build_read_tables() -> BuildReadTablesResult:
            assert self.loading
            if self.fail == "edges":
                raise TableRejected("not enough disk")
            citations = await self.applied.latest("citations")
            assert citations is not None
            self.events.append(("edges", "citations", citations.release))
            for table in READ_TABLES:
                await self.applied.record(applied_release(table, citations.release, timedelta()))
            built = ReadTableResult(rows=1, seconds=0)
            return BuildReadTablesResult(
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
            read_tables=AsyncMock(is_current=edges_current),
            build_read_tables=build_read_tables,
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
    assert cycle.leases == 1
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


async def test_old_read_tables_rebuild_at_final_release_before_archives(cycle: Cycle) -> None:
    for table in READ_TABLES:
        await cycle.applied.record(applied_release(table, RELEASE, timedelta()))
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    services = cycle.services()
    async with cycle.lease(), cycle.load_lease():
        assert await services.read_tables.is_current(RELEASE)
        assert not await services.read_tables.is_current(NEXT)
    result = await s2_sync.run_sync(command, services)
    citations = next(item for item in result.datasets if item.dataset == "citations")
    assert (citations.start, citations.final) == (RELEASE, NEXT)
    assert result.read_tables is not None and result.read_tables.source_release == NEXT
    assert [event for event in cycle.events if event[0] == "edges"] == [
        ("edges", "citations", NEXT)
    ]
    built = cycle.events.index(("edges", "citations", NEXT))
    assert built > cycle.events.index(("load", "citations", NEXT))
    archives = [
        index for index, (_, dataset, _) in enumerate(cycle.events) if dataset in LARGE_ORDER
    ]
    assert archives and min(archives) > built
    for table in READ_TABLES:
        marker = await cycle.applied.latest(table)
        assert marker is not None and marker.release == NEXT


async def test_read_tables_rebuild_once_per_citations_release(
    cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    cycle.fail = "edges"
    with pytest.raises(SyncIncomplete, match="read tables: not enough disk"):
        await s2_sync.run_sync(command, cycle.services())
    assert not any(dataset in LARGE_ORDER for _, dataset, _ in cycle.events)
    cycle.fail, cycle.events = None, []
    await cycle.applied.record(applied_release(READ_TABLES[0], NEXT, timedelta()))
    assert await cycle.applied.latest(READ_TABLES[1]) is None
    result = await s2_sync.run_sync(command, cycle.services())
    assert result.read_tables is not None and result.read_tables.source_release == NEXT
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
    cycle.applied.records = [row for row in cycle.applied.records if row.target != READ_TABLES[1]]
    cycle.events.clear()

    @asynccontextmanager
    async def competing_lease() -> AsyncGenerator[None]:
        assert not cycle.loading
        # The competing builder publishes before handing over the shared slot.
        await cycle.applied.record(applied_release(READ_TABLES[1], NEXT, timedelta()))
        async with cycle.load_lease():
            yield

    services = replace(cycle.services(), load_lease=competing_lease)
    result = await s2_sync.run_sync(cycle.command, services)
    assert result.read_tables is None
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
    monkeypatch.setattr(services.read_tables, "is_current", check)
    command = cycle.command.model_copy(update={"enabled_archives": frozenset(LARGE_ORDER)})
    if cancel:
        with pytest.raises(asyncio.CancelledError):
            await s2_sync.run_sync(command, services)
    else:
        with pytest.raises(SyncIncomplete, match="read tables: read denied"):
            await s2_sync.run_sync(command, services)
    assert not cycle.loading
    assert not any(
        action == "edges" or dataset in LARGE_ORDER for action, dataset, _ in cycle.events
    )


@pytest.mark.parametrize("failure", ["first", "load", "cancel", "none"])
async def test_managed_sync_holds_one_lease_and_retains_admission_on_failure(
    cycle: Cycle, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    slots = acquisitions = 0

    @asynccontextmanager
    async def admitted() -> AsyncGenerator[None]:
        nonlocal slots
        assert cycle.active == 1
        slots += 1
        try:
            yield
        except BaseException:
            raise
        else:
            slots -= 1

    @asynccontextmanager
    async def source() -> AsyncGenerator[None]:
        nonlocal acquisitions
        acquisitions += 1
        if failure == "first":
            raise SharedCapacityError("busy source")
        async with cycle.lease():
            yield

    @asynccontextmanager
    async def opened(
        settings: SyncSettings,
        s2_lease: Callable[[], AbstractAsyncContextManager[None]],
        load_lease: Callable[[], AbstractAsyncContextManager[None]],
        *,
        read_table_stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
    ) -> AsyncGenerator[s2_sync.SyncServices]:
        yield replace(cycle.services(), s2_lease=s2_lease, load_lease=load_lease)

    monkeypatch.setattr(admission, "execution", admitted)
    monkeypatch.setattr(deployments, "one_s2_user", source)
    monkeypatch.setattr(deployments, "one_clickhouse_loader", cycle.load_lease)
    monkeypatch.setattr(bootstrap, "open_sync", opened)
    monkeypatch.setattr(
        deployments,
        "SyncSettings",
        lambda: SyncSettings.model_construct(
            sync=cycle.command, image_digest=PRODUCER.image_digest
        ),
    )
    if failure == "cancel":
        cycle.fail = "cancel"
        with pytest.raises(asyncio.CancelledError):
            await deployments.s2_sync.fn()
    elif failure == "load":
        cycle.fail = "load"
        monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
        with pytest.raises(SyncIncomplete):
            await deployments.s2_sync.fn()
    elif failure != "none":
        with pytest.raises(SharedCapacityError, match="busy source"):
            await deployments.s2_sync.fn()
    else:
        await deployments.s2_sync.fn()
    assert slots == (1 if failure in ("load", "cancel") else 0)
    assert acquisitions == 1


@pytest.fixture
async def managed_cycle(
    cycle: Cycle, prefect_server: None, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[Cycle]:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitUpdate

    deployments.ensure_limits()
    admission.status("open")

    @asynccontextmanager
    async def opened(
        settings: SyncSettings,
        s2_lease: Callable[[], AbstractAsyncContextManager[None]],
        load_lease: Callable[[], AbstractAsyncContextManager[None]],
        *,
        read_table_stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
    ) -> AsyncGenerator[s2_sync.SyncServices]:
        @asynccontextmanager
        async def tracked() -> AsyncGenerator[None]:
            async with s2_lease(), cycle.lease():
                yield

        @asynccontextmanager
        async def tracked_load() -> AsyncGenerator[None]:
            async with load_lease(), cycle.load_lease():
                yield

        yield replace(cycle.services(), s2_lease=tracked, load_lease=tracked_load)

    monkeypatch.setattr(bootstrap, "open_sync", opened)
    monkeypatch.setattr(
        deployments,
        "SyncSettings",
        lambda: SyncSettings.model_construct(
            sync=cycle.command, image_digest=PRODUCER.image_digest
        ),
    )
    monkeypatch.setattr(s2_sync, "get_run_logger", lambda: logging.getLogger(__name__))
    try:
        yield cycle
    finally:
        # Isolated test database only; production occupancy needs operator reconciliation.
        with get_client(sync_client=True) as client:
            client.update_global_concurrency_limit(
                admission.NAME,
                GlobalConcurrencyLimitUpdate(limit=admission.OPEN_LIMIT, active_slots=0),
            )


async def test_manual_download_refuses_during_sync_load(
    managed_cycle: Cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    from prefect.concurrency.asyncio import ConcurrencySlotAcquisitionError

    from openknowledge_pipelines.settings import BatchSettings

    entered, resume = asyncio.Event(), asyncio.Event()
    original = managed_cycle.loader.prepare_diff

    async def paused(command: LoadDiff) -> PreparedDiff:
        entered.set()
        await resume.wait()
        return await original(command)

    monkeypatch.setattr(managed_cycle.loader, "prepare_diff", paused)
    monkeypatch.setattr(deployments, "BatchSettings", lambda: BatchSettings.model_construct())
    task = asyncio.create_task(deployments.s2_sync.fn())
    try:
        async with asyncio.timeout(30):
            await entered.wait()
        assert admission.status().active_slots == 1
        with pytest.raises(ConcurrencySlotAcquisitionError):
            await deployments.s2_batch_download.fn(
                DownloadDataset(bucket="raw", dataset="papers", release=NEXT)
            )
        assert admission.status().active_slots == 1
    finally:
        resume.set()
        await task
    assert admission.status().active_slots == 0


async def test_sync_waits_for_standalone_load_then_finishes_without_retained_admission(
    managed_cycle: Cycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    from threading import Event
    from typing import Literal

    import httpx
    from prefect.client.orchestration import PrefectClient
    from prefect.client.schemas.objects import ConcurrencyLeaseHolder

    busy = Event()
    original = PrefectClient.increment_concurrency_slots_with_lease

    async def observed(
        client: PrefectClient,
        names: list[str],
        slots: int,
        mode: Literal["concurrency", "rate_limit"],
        lease_duration: float,
        holder: ConcurrencyLeaseHolder | None = None,
    ) -> httpx.Response:
        try:
            return await original(client, names, slots, mode, lease_duration, holder)
        except httpx.HTTPStatusError as exc:
            if names == [deployments.LOAD_LIMIT] and exc.response.status_code == 423:
                busy.set()
            raise

    monkeypatch.setattr(PrefectClient, "increment_concurrency_slots_with_lease", observed)
    acquire = deployments.one_clickhouse_loader
    task: asyncio.Task[object] | None = None
    try:
        # A standalone load has already acquired capacity and admission before sync starts.
        async with acquire(wait=False), admission.execution():
            task = asyncio.create_task(deployments.s2_sync.fn())
            assert await asyncio.to_thread(busy.wait, 30)
            assert not task.done()
            assert admission.status().active_slots == 2
        async with asyncio.timeout(40):
            await task
        assert admission.status().active_slots == 0
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_managed_sync_failure_after_external_work_retains_real_admission(
    managed_cycle: Cycle,
) -> None:
    managed_cycle.fail = "load"
    with pytest.raises(SyncIncomplete):
        await deployments.s2_sync.fn()
    assert ("archive", "papers", NEXT) in managed_cycle.events
    assert admission.status().active_slots == 1


@pytest.mark.parametrize("failure", [None, "load", "edges"])
async def test_sync_artifact_is_persisted_on_its_real_run_with_partial_outcomes(
    managed_cycle: Cycle, failure: str | None
) -> None:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        ArtifactFilter,
        ArtifactFilterFlowRunId,
        ArtifactFilterKey,
    )
    from prefect.client.schemas.sorting import FlowRunSort
    from prefect.context import FlowRunContext

    managed_cycle.fail = failure
    if failure is None:
        await deployments.s2_sync()
    else:
        with pytest.raises(SyncIncomplete):
            await deployments.s2_sync()
    async with get_client() as client:
        [run] = await client.read_flow_runs(sort=FlowRunSort.START_TIME_DESC, limit=1)
        assert run.state is not None
        assert run.state.is_completed() if failure is None else run.state.is_failed()
        [artifact] = await client.read_artifacts(
            artifact_filter=ArtifactFilter(
                flow_run_id=ArtifactFilterFlowRunId(any_=[run.id]),
                key=ArtifactFilterKey(any_=["s2-sync-results"]),
            )
        )
    assert artifact.key == "s2-sync-results"
    assert artifact.type == "markdown"
    markdown = str(artifact.data)
    assert f"Run: {run.id}" in markdown
    assert f"Status: {'completed' if failure is None else 'failed'}" in markdown
    assert "Software version:" in markdown and "Image digest:" in markdown
    assert "| authors |" in markdown and "| synced |" in markdown
    assert "release discovery" in markdown and "sync papers" in markdown
    assert "Signature" not in markdown and "CLICKHOUSE_PASSWORD" not in markdown
    if failure == "load":
        assert f"| papers | {RELEASE} | {NEXT} | {RELEASE} | failed | 1 | 0 |" in markdown
    elif failure == "edges":
        assert "Rebuild failed; publication may be partial" in markdown
    else:
        assert "| citation_edges_out | 1 |" in markdown
        assert "| citation_edges_in | 1 |" in markdown
    assert FlowRunContext.get() is None


@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (asyncio.CancelledError(), "interrupted"),
        (BatchTimedOut("external work may remain"), "failed"),
        (SharedCapacityError("foreign job"), "failed"),
        (TerminationSignal(signal.SIGTERM), "interrupted"),
    ],
)
async def test_interrupted_archive_artifact_keeps_real_partial_progress(
    managed_cycle: Cycle,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
    expected_status: str,
) -> None:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        ArtifactFilter,
        ArtifactFilterFlowRunId,
        ArtifactFilterKey,
    )
    from prefect.context import FlowRunContext

    managed_cycle.command = managed_cycle.command.model_copy(
        update={"enabled_archives": frozenset({"s2orc_v2"})}
    )
    original = managed_cycle.progress.finalize

    async def finalize(
        command: DownloadDataset | DownloadDiff, predecessor: HashedRef | None = None
    ) -> HashedRef:
        if command.dataset == "s2orc_v2" and command.release == TARGET:
            raise failure
        return await original(command, predecessor)

    monkeypatch.setattr(managed_cycle.progress, "finalize", finalize)

    @flow(persist_result=False)
    async def parent() -> UUID:
        # Exercise the production body and real artifacts while leaving the signal
        # uncaught by a second Prefect engine, which would own a separate final state.
        context = FlowRunContext.get()
        assert context is not None and context.flow_run is not None
        with pytest.raises(type(failure)) as raised:
            await deployments.s2_sync.fn()
        assert raised.value is failure
        return context.flow_run.id

    run_id = await parent()
    async with get_client() as client:
        [artifact] = await client.read_artifacts(
            artifact_filter=ArtifactFilter(
                flow_run_id=ArtifactFilterFlowRunId(any_=[run_id]),
                key=ArtifactFilterKey(any_=["s2-sync-results"]),
            )
        )
    markdown = str(artifact.data)
    assert f"Status: {expected_status}" in markdown
    assert f"| s2orc_v2 | {RELEASE} | {TARGET} | {NEXT} | {expected_status} | 1 | 0 |" in markdown
    assert "| archive s2orc_v2 |" in markdown
    assert "| unstarted |" not in markdown
    assert "Prefect owns the final orchestration state" in markdown
