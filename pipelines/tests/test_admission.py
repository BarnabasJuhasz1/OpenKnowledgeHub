"""The real pinned Prefect API admission boundary with disposable server/database."""

import asyncio
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor

import pytest
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitUpdate

from openknowledge_pipelines import admission


@pytest.fixture
def closed_admission(prefect_server: None) -> Generator[None]:
    with get_client(sync_client=True) as client:
        client.update_global_concurrency_limit(
            admission.NAME, GlobalConcurrencyLimitUpdate(limit=0, active=True, active_slots=0)
        )
    yield
    with get_client(sync_client=True) as client:
        client.update_global_concurrency_limit(
            admission.NAME,
            GlobalConcurrencyLimitUpdate(limit=admission.OPEN_LIMIT, active=True, active_slots=0),
        )


async def test_close_blocks_concurrent_submission_without_cancelling_existing(
    closed_admission: None,
) -> None:
    admission.status("open")
    async with admission.execution():
        with ThreadPoolExecutor(1) as pool:
            closed = pool.submit(admission.status, "close").result(timeout=10)
        assert closed.closed and closed.active_slots == 1
        with pytest.raises(admission.AdmissionError, match="execution refused") as refused:
            async with admission.execution():
                pytest.fail("closed gate admitted new execution")
        assert "reconcile" not in str(refused.value)
    assert admission.status().active_slots == 0
    assert admission.status().closed


async def test_missing_and_inactive_limits_refuse_execution(closed_admission: None) -> None:
    with get_client(sync_client=True) as client:
        client.update_global_concurrency_limit(
            admission.NAME, GlobalConcurrencyLimitUpdate(active=False)
        )
    with pytest.raises(RuntimeError, match="execution refused"):
        async with admission.execution():
            pytest.fail("inactive gate admitted execution")
    with get_client(sync_client=True) as client:
        client.delete_global_concurrency_limit_by_name(admission.NAME)
    with pytest.raises(RuntimeError, match="execution refused"):
        async with admission.execution():
            pytest.fail("missing gate admitted execution")
    admission.ensure_closed()
    assert admission.status().closed


async def test_close_races_acquisition_without_losing_occupancy(
    closed_admission: None,
) -> None:
    admission.status("open")
    release = asyncio.Event()
    ready = [asyncio.Event() for _ in range(4)]
    admitted: list[int] = []

    async def contender(index: int) -> None:
        try:
            async with admission.execution():
                admitted.append(index)
                ready[index].set()
                await release.wait()
        except admission.AdmissionError:
            ready[index].set()

    tasks = [asyncio.create_task(contender(index)) for index in range(4)]
    try:
        await asyncio.to_thread(admission.status, "close")
        async with asyncio.timeout(40):
            await asyncio.gather(*(event.wait() for event in ready))
        observed = await asyncio.to_thread(admission.status)
        assert observed.closed
        assert observed.active_slots == len(admitted)
        with pytest.raises(admission.AdmissionError):
            async with admission.execution():
                pytest.fail("new work crossed the closed boundary")
    finally:
        release.set()
        await asyncio.gather(*tasks)
    assert admission.status().active_slots == 0


async def test_cancellation_retains_real_occupancy(closed_admission: None) -> None:
    admission.status("open")
    entered = asyncio.Event()

    async def work() -> None:
        async with admission.execution():
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(work())
    try:
        async with asyncio.timeout(40):
            await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        admission.status("close")
        admission.ensure_closed()
        assert admission.status().active_slots == 1
        assert admission.status().closed
        with pytest.raises(admission.AdmissionError, match="still occupied"):
            admission.status("open")
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_release_frees_held_slots_only_while_closed(closed_admission: None) -> None:
    admission.status("open")
    with pytest.raises(RuntimeError, match="external job may survive"):
        async with admission.execution():
            raise RuntimeError("external job may survive")
    with pytest.raises(admission.AdmissionError, match="Close admission"):
        admission.release(1)
    admission.status("close")
    with pytest.raises(admission.AdmissionError, match="holds 1 slots, not 2"):
        admission.release(2)
    assert admission.release(1).active_slots == 0
    admission.status("open")


async def _standalone(operation: str) -> None:
    from openknowledge_core.ingestion import LoadDataset, LoadDiff
    from openknowledge_pipelines import deployments

    if operation == "load":
        await deployments.s2_load.fn(
            LoadDataset(bucket="raw", dataset="papers", release="2026-10-06")
        )
    elif operation == "diff":
        await deployments.s2_load_diff.fn(
            LoadDiff(
                bucket="raw", dataset="papers", from_release="2026-10-06", release="2026-10-13"
            )
        )
    else:
        await deployments.s2_build_citation_edges.fn()


def _local_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    from openknowledge_pipelines import bootstrap, deployments
    from openknowledge_pipelines.settings import LoadSettings

    deployments.ensure_limits()
    monkeypatch.setattr(deployments, "LoadSettings", lambda: LoadSettings.model_construct())

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("refusal before work must not open external clients")

    monkeypatch.setattr(bootstrap, "open_load_service", forbidden)
    monkeypatch.setattr(bootstrap, "open_citation_edges", forbidden)


@pytest.mark.parametrize("operation", ["load", "diff", "edges"])
async def test_standalone_busy_capacity_takes_no_admission(
    closed_admission: None, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    from prefect.concurrency.asyncio import ConcurrencySlotAcquisitionError

    from openknowledge_pipelines import deployments

    _local_loads(monkeypatch)
    admission.status("open")
    async with deployments.one_clickhouse_loader(wait=False):
        async with asyncio.timeout(10):
            with pytest.raises(ConcurrencySlotAcquisitionError):
                await _standalone(operation)
        assert admission.status().active_slots == 0


@pytest.mark.parametrize("operation", ["load", "diff", "edges"])
async def test_closed_gate_when_capacity_available_refuses_without_external_work(
    closed_admission: None, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager

    from openknowledge_pipelines import deployments

    _local_loads(monkeypatch)
    admission.status("open")
    acquire = deployments.one_clickhouse_loader

    @asynccontextmanager
    async def close_after_capacity(*, wait: bool = True) -> AsyncGenerator[None]:
        async with acquire(wait=wait):
            # Real API gate change between the resource grant and admission.
            await asyncio.to_thread(admission.status, "close")
            yield

    monkeypatch.setattr(deployments, "one_clickhouse_loader", close_after_capacity)
    with pytest.raises(admission.AdmissionError):
        await _standalone(operation)
    assert admission.status().active_slots == 0
    async with acquire(wait=False):
        pass  # refusal released the granted ClickHouse capacity


@pytest.mark.parametrize("operation", ["load", "diff", "edges"])
async def test_cancel_during_pinned_capacity_acquisition_takes_no_admission(
    closed_admission: None, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    from threading import Event
    from typing import Literal

    import httpx
    from prefect.client.schemas.objects import ConcurrencyLeaseHolder
    from prefect.concurrency.services import ConcurrencySlotAcquisitionWithLeaseService

    from openknowledge_pipelines import deployments

    _local_loads(monkeypatch)
    admission.status("open")
    service = ConcurrencySlotAcquisitionWithLeaseService.instance(
        frozenset({deployments.LOAD_LIMIT})
    )
    # Prefect acquires on a separate service loop. Delay a real request there so
    # cancellation happens before the grant, then verify its orphan-lease cleanup.
    entered, resume, granted = Event(), Event(), Event()
    original = service.acquire

    async def delayed(
        slots: int,
        mode: Literal["concurrency", "rate_limit"],
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
        lease_duration: float = 300,
        strict: bool = False,
        holder: ConcurrencyLeaseHolder | None = None,
    ) -> httpx.Response:
        entered.set()
        await asyncio.to_thread(resume.wait, 20)
        response = await original(
            slots, mode, timeout_seconds, max_retries, lease_duration, strict, holder
        )
        granted.set()
        return response

    monkeypatch.setattr(service, "acquire", delayed)
    task = asyncio.create_task(_standalone(operation))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert admission.status().active_slots == 0
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)
    assert await asyncio.to_thread(granted.wait, 10)
    async with get_client() as client:
        async with asyncio.timeout(10):
            for _ in range(200):
                current = await client.read_global_concurrency_limit_by_name(deployments.LOAD_LIMIT)
                if current.active_slots == 0:
                    break
                await asyncio.sleep(0.05)
            else:
                pytest.fail("cancelled acquisition leaked ClickHouse capacity")
    assert admission.status().active_slots == 0
