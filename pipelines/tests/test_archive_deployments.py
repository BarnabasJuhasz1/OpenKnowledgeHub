"""Managed entry points and the real Prefect shared-budget behavior (loopback test server)."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Literal

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.objects import ConcurrencyLeaseHolder
from prefect.concurrency.asyncio import ConcurrencySlotAcquisitionError

from openknowledge_core.ingestion import DownloadDatasetService, ShardJobs
from openknowledge_pipelines import bootstrap, deployments
from openknowledge_pipelines.settings import BatchSettings
from tests.fakes.ports import COMMAND, DIFF


@pytest.mark.parametrize("dataset", ["s2orc_v2", "embeddings-specter_v2", "s2orc", "unknown"])
async def test_core_mismatch_precedes_settings_and_clients(
    monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    monkeypatch.delenv("S2_API_KEY", raising=False)
    with pytest.raises(ValueError, match="belongs to|Unsupported archive dataset"):
        await deployments.s2_batch_download.fn(COMMAND.model_copy(update={"dataset": dataset}))
    with pytest.raises(ValueError, match="belongs to|Unsupported archive dataset"):
        await deployments.s2_batch_download_diff.fn(DIFF.model_copy(update={"dataset": dataset}))


async def test_archive_mismatch_precedes_client_construction() -> None:
    for base in (deployments.s2_batch_download_fulltext, deployments.s2_batch_download_embeddings):
        with pytest.raises(ValueError, match="belongs to core"):
            await base.fn(COMMAND)
    for diff in (
        deployments.s2_batch_download_fulltext_diff,
        deployments.s2_batch_download_embeddings_diff,
    ):
        with pytest.raises(ValueError, match="belongs to core"):
            await diff.fn(DIFF)


@pytest.mark.usefixtures("prefect_server")
async def test_managed_routes_share_budget_and_release_after_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Clients must never consume settings when the real shared budget is busy.
    monkeypatch.setattr(deployments, "BatchSettings", BatchSettings.model_construct)

    @asynccontextmanager
    async def forbidden(
        settings: BatchSettings,
    ) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs]]:
        pytest.fail("busy controllers must not open clients")
        yield  # pragma: no cover

    monkeypatch.setattr(bootstrap, "open_batch_download", forbidden)
    deployments.ensure_limits()
    async with deployments.one_s2_user():
        with pytest.raises(ConcurrencySlotAcquisitionError, match="semantic-scholar"):
            await deployments.s2_batch_download_fulltext.fn(
                COMMAND.model_copy(update={"dataset": "s2orc_v2"})
            )
        with pytest.raises(ConcurrencySlotAcquisitionError, match="semantic-scholar"):
            await deployments.s2_batch_download_embeddings_diff.fn(
                DIFF.model_copy(update={"dataset": "embeddings-specter_v2"})
            )
    with pytest.raises(RuntimeError, match="job wait failed"):
        async with deployments.one_s2_user():
            raise RuntimeError("job wait failed")
    async with deployments.one_s2_user():
        pass


@pytest.mark.usefixtures("prefect_server")
async def test_missing_shared_limit_refuses_to_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deployments, "S2_LIMIT", "missing-archive-budget-test")
    with pytest.raises(ConcurrencySlotAcquisitionError, match="must be created"):
        async with deployments.one_s2_user():
            pytest.fail("a missing budget must not permit work")


@pytest.mark.usefixtures("prefect_server")
async def test_slow_successful_acquisition_has_no_one_second_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    acquire = PrefectClient.increment_concurrency_slots_with_lease

    async def delayed(
        self: PrefectClient,
        names: list[str],
        slots: int,
        mode: Literal["concurrency", "rate_limit"],
        lease_duration: float,
        holder: ConcurrencyLeaseHolder | None = None,
    ) -> httpx.Response:
        await asyncio.sleep(1.1)
        return await acquire(self, names, slots, mode, lease_duration, holder)

    deployments.ensure_limits()
    monkeypatch.setattr(PrefectClient, "increment_concurrency_slots_with_lease", delayed)
    async with deployments.one_s2_user():
        pass
