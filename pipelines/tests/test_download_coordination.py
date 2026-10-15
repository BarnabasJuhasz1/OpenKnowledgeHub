from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager
from typing import Literal, cast

import httpx
import pytest
from prefect.client.orchestration import PrefectClient, get_client
from prefect.client.schemas.actions import (
    GlobalConcurrencyLimitCreate,
    GlobalConcurrencyLimitUpdate,
)
from prefect.exceptions import ObjectNotFound
from prefect.settings import PREFECT_API_URL, PREFECT_CLIENT_MAX_RETRIES, temporary_settings

from openknowledge_core.ingestion import DownloadDatasetService, ShardJobs
from openknowledge_core.ingestion.errors import BatchTimedOut
from openknowledge_pipelines import bootstrap, deployments
from openknowledge_pipelines.download_coordination import DOWNLOAD_LIMIT, exclusive_download
from openknowledge_pipelines.settings import BatchSettings
from tests.fakes.ports import COMMAND, DIFF

pytestmark = pytest.mark.usefixtures("prefect_server")


@pytest.fixture
async def gate_client() -> AsyncGenerator[PrefectClient]:
    async with get_client() as client:
        try:
            await client.delete_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
        except ObjectNotFound:
            pass
        yield client
        await client.delete_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)


async def test_gate_reserves_before_work_and_releases_only_after_success(
    gate_client: PrefectClient,
) -> None:
    with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 3}):
        async with exclusive_download():
            occupied = await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
            assert (occupied.limit, occupied.active_slots, occupied.active) == (1, 1, True)
            assert PREFECT_CLIENT_MAX_RETRIES.value() == 3
            with pytest.raises(RuntimeError, match="downloads are blocked"):
                async with exclusive_download():
                    pytest.fail("a second download must not start")
    released = await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    assert (released.limit, released.active_slots) == (1, 0)
    async with exclusive_download():
        pass


async def test_timeout_keeps_the_reservation_across_a_new_attempt(
    gate_client: PrefectClient,
) -> None:
    with pytest.raises(BatchTimedOut):
        async with exclusive_download():
            raise BatchTimedOut("projects/project/locations/region/jobs/unknown-job may still run")
    paused = await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    assert (paused.limit, paused.active_slots) == (1, 1)
    with pytest.raises(RuntimeError, match="verify every submitted Batch job"):
        async with exclusive_download():
            pytest.fail("a later run must preserve the occupied slot")
    assert (
        await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    ).active_slots == 1


async def test_cancellation_leaves_the_durable_slot_occupied(gate_client: PrefectClient) -> None:
    working = asyncio.Event()

    async def download() -> None:
        async with exclusive_download():
            working.set()
            await asyncio.Event().wait()

    running = asyncio.create_task(download())
    await asyncio.wait_for(working.wait(), timeout=10)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert (
        await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    ).active_slots == 1


async def test_reservation_does_not_use_an_expiring_lease(
    gate_client: PrefectClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def leased(
        client: PrefectClient,
        names: list[str],
        slots: int,
        mode: Literal["concurrency", "rate_limit"],
        lease_duration: float,
        holder: object = None,
    ) -> httpx.Response:
        pytest.fail("a lease must not expire while a remote Batch job is still running")

    monkeypatch.setattr(PrefectClient, "increment_concurrency_slots_with_lease", leased)
    async with exclusive_download():
        assert (
            await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
        ).active_slots == 1


async def test_a_stale_preflight_read_cannot_admit_a_second_download(
    gate_client: PrefectClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    await gate_client.create_global_concurrency_limit(
        GlobalConcurrencyLimitCreate(name=DOWNLOAD_LIMIT, limit=1)
    )
    original = PrefectClient.increment_concurrency_slots
    both_ready, first_acquired, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
    arrivals = 0
    admitted = 0

    async def increment(
        client: PrefectClient,
        names: list[str],
        slots: int,
        mode: Literal["concurrency", "rate_limit"],
    ) -> httpx.Response:
        nonlocal arrivals
        arrivals += 1
        order = arrivals
        if arrivals == 2:
            both_ready.set()
        await asyncio.wait_for(both_ready.wait(), timeout=10)
        if order == 2:
            await asyncio.wait_for(first_acquired.wait(), timeout=10)
        response = await original(client, names=names, slots=slots, mode=mode)
        first_acquired.set()
        return response

    monkeypatch.setattr(PrefectClient, "increment_concurrency_slots", increment)

    async def download() -> bool:
        nonlocal admitted
        try:
            async with exclusive_download():
                admitted += 1
                await asyncio.wait_for(finish.wait(), timeout=10)
                raise BatchTimedOut("the admitted download stopped")
        except BatchTimedOut:
            return True
        except RuntimeError as exc:
            assert "downloads are blocked" in str(exc)
            finish.set()
            return False

    results = await asyncio.wait_for(asyncio.gather(download(), download()), timeout=15)
    assert sorted(results) == [False, True]
    assert admitted == 1
    assert (
        await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    ).active_slots == 1


async def test_a_lost_release_reply_cannot_clear_the_next_owners_slot(
    gate_client: PrefectClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = httpx.AsyncHTTPTransport.handle_async_request
    releases = 0

    async def handle(transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        nonlocal releases
        response = await original(transport, request)
        if request.url.path.endswith("/concurrency_limits/decrement"):
            releases += 1
            if releases == 1:
                await response.aclose()
                await gate_client.increment_concurrency_slots(
                    names=[DOWNLOAD_LIMIT], slots=1, mode="concurrency"
                )
                raise httpx.ReadTimeout("release committed but reply was lost", request=request)
        return response

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)
    with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 3}):
        with pytest.raises(httpx.ReadTimeout, match="release committed"):
            async with exclusive_download():
                pass
    assert releases == 1
    assert (
        await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    ).active_slots == 1


async def test_a_lost_acquisition_reply_is_never_retried(
    gate_client: PrefectClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = httpx.AsyncHTTPTransport.handle_async_request
    acquisitions = 0

    async def handle(transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        nonlocal acquisitions
        response = await original(transport, request)
        if request.url.path.endswith("/concurrency_limits/increment"):
            acquisitions += 1
            await response.aclose()
            raise httpx.ReadTimeout("acquisition committed but reply was lost", request=request)
        return response

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)
    with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 3}):
        with pytest.raises(httpx.ReadTimeout, match="acquisition committed"):
            async with exclusive_download():
                pytest.fail("work cannot start after an uncertain acquisition")
    assert acquisitions == 1
    assert (
        await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    ).active_slots == 1


@pytest.mark.parametrize(
    "update",
    [
        GlobalConcurrencyLimitUpdate(limit=0),
        GlobalConcurrencyLimitUpdate(limit=2),
        GlobalConcurrencyLimitUpdate(active=False),
        GlobalConcurrencyLimitUpdate(slot_decay_per_second=1),
        GlobalConcurrencyLimitUpdate(active_slots=1),
    ],
)
async def test_existing_operator_configuration_is_checked_and_never_overwritten(
    gate_client: PrefectClient, update: GlobalConcurrencyLimitUpdate
) -> None:
    await gate_client.create_global_concurrency_limit(
        GlobalConcurrencyLimitCreate(name=DOWNLOAD_LIMIT, limit=1)
    )
    await gate_client.update_global_concurrency_limit(DOWNLOAD_LIMIT, update)
    before = await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    with pytest.raises(RuntimeError, match="downloads are blocked"):
        async with exclusive_download():
            pytest.fail("unsafe or occupied limits must fail closed")
    after = await gate_client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    assert before.model_dump(exclude={"updated"}) == after.model_dump(exclude={"updated"})


async def test_both_download_entrypoints_check_the_gate_before_opening_clients(
    gate_client: PrefectClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    await gate_client.create_global_concurrency_limit(
        GlobalConcurrencyLimitCreate(name=DOWNLOAD_LIMIT, limit=1, active_slots=1)
    )
    monkeypatch.setattr(deployments, "BatchSettings", lambda: cast(BatchSettings, object()))

    def must_not_open(
        settings: BatchSettings,
    ) -> AbstractAsyncContextManager[tuple[DownloadDatasetService, ShardJobs]]:
        pytest.fail("a blocked download must not open clients or pin metadata")

    monkeypatch.setattr(bootstrap, "open_batch_download", must_not_open)
    with pytest.raises(RuntimeError, match="downloads are blocked"):
        await deployments.s2_batch_download.fn(COMMAND)
    with pytest.raises(RuntimeError, match="downloads are blocked"):
        await deployments.s2_batch_download_diff.fn(DIFF)


async def test_missing_shared_api_url_fails_before_creating_a_limit() -> None:
    with temporary_settings({PREFECT_API_URL: None}):
        with pytest.raises(RuntimeError, match="persistent PREFECT_API_URL"):
            async with exclusive_download():
                pytest.fail("separate ephemeral servers cannot coordinate downloads")
