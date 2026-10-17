"""Managed entry points and the real Prefect shared-budget behavior (loopback test server)."""

import asyncio
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
from prefect.concurrency.asyncio import ConcurrencySlotAcquisitionError

from openknowledge_core.ingestion import DownloadDatasetService, ShardJobs
from openknowledge_core.ingestion.families import ArchiveFamily
from openknowledge_pipelines import bootstrap, deployments
from openknowledge_pipelines.settings import BatchSettings
from tests.fakes.ports import COMMAND, DIFF


@pytest.mark.parametrize("dataset", ["s2orc_v2", "embeddings-specter_v2", "s2orc", "unknown"])
async def test_core_mismatch_precedes_settings_limits_and_clients(
    monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    @asynccontextmanager
    async def forbidden(
        settings: BatchSettings,
    ) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs]]:
        pytest.fail("mismatched commands must never open clients")
        yield  # pragma: no cover

    monkeypatch.setattr(bootstrap, "open_batch_download", forbidden)
    monkeypatch.delenv("S2_API_KEY", raising=False)
    with pytest.raises(ValueError):
        await deployments.s2_batch_download.fn(COMMAND.model_copy(update={"dataset": dataset}))
    with pytest.raises(ValueError):
        await deployments.s2_batch_download_diff.fn(DIFF.model_copy(update={"dataset": dataset}))


@pytest.mark.parametrize("family", [ArchiveFamily.FULLTEXT, ArchiveFamily.EMBEDDINGS])
async def test_archive_mismatch_precedes_client_construction(family: ArchiveFamily) -> None:
    # Missing settings would fail if validation happened after construction.
    if family == ArchiveFamily.FULLTEXT:
        with pytest.raises(ValueError, match="belongs to core"):
            await deployments.s2_batch_download_fulltext.fn(COMMAND)
        with pytest.raises(ValueError, match="belongs to core"):
            await deployments.s2_batch_download_fulltext_diff.fn(DIFF)
    else:
        with pytest.raises(ValueError, match="belongs to core"):
            await deployments.s2_batch_download_embeddings.fn(COMMAND)
        with pytest.raises(ValueError, match="belongs to core"):
            await deployments.s2_batch_download_embeddings_diff.fn(DIFF)


@pytest.mark.usefixtures("prefect_server")
async def test_real_prefect_budget_refuses_busy_requests_and_releases_after_errors() -> None:
    deployments.ensure_limits()
    async with deployments.one_s2_user():
        # Uses the actual server and acquisition service, including max_retries=0.
        with pytest.raises(ConcurrencySlotAcquisitionError, match="semantic-scholar"):
            async with deployments.one_s2_user():
                pytest.fail("a second controller must not acquire the shared budget")
    with pytest.raises(RuntimeError, match="job wait failed"):
        async with deployments.one_s2_user():
            raise RuntimeError("job wait failed")
    async with deployments.one_s2_user():
        pass


@pytest.mark.usefixtures("prefect_server")
async def test_shared_slot_is_released_on_controller_cancellation() -> None:
    deployments.ensure_limits()
    entered = asyncio.Event()

    async def controller() -> None:
        async with deployments.one_s2_user():
            entered.set()
            await asyncio.Future[None]()

    running = asyncio.create_task(controller())
    await asyncio.wait_for(entered.wait(), timeout=10)
    # Lease loss/deployment cancellation must stop the controller, leaving Batch recovery
    # to the next run. This tests cancellation cleanup without waiting for lease expiry.
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    async with deployments.one_s2_user():
        pass


@pytest.mark.usefixtures("prefect_server")
async def test_renewal_failure_cancels_controller_and_releases_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployments.ensure_limits()

    def failed_renewal(lease_id: UUID, lease_duration: float, stop_event: threading.Event) -> None:
        raise RuntimeError("injected lease renewal failure")

    # Exercise Prefect's actual lease failure handler and cancellation scope without waiting
    # minutes for a renewal. The server still performs real slot acquisition and release.
    with monkeypatch.context() as patch:
        patch.setattr("prefect.concurrency._leases._lease_renewal_loop", failed_renewal)
        with pytest.raises(asyncio.CancelledError):
            async with deployments.one_s2_user():
                await asyncio.wait_for(asyncio.Future[None](), timeout=10)
    async with deployments.one_s2_user():
        pass


@pytest.mark.usefixtures("prefect_server")
async def test_missing_shared_limit_refuses_to_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deployments, "S2_LIMIT", "missing-archive-budget-test")
    with pytest.raises(ConcurrencySlotAcquisitionError, match="must be created"):
        async with deployments.one_s2_user():
            pytest.fail("a missing budget must not permit work")


@pytest.mark.parametrize("dataset", ["papers", "s2orc_v2", "embeddings-specter_v2"])
@pytest.mark.parametrize("diff", [False, True])
async def test_every_managed_route_takes_budget_before_opening_clients(
    monkeypatch: pytest.MonkeyPatch, dataset: str, diff: bool
) -> None:
    settings = BatchSettings.model_validate(
        {
            "S2_API_KEY": "test-key",
            "image_digest": "sha256:" + "a" * 64,
            "batch_project": "p",
            "batch_region": "r",
            "batch_service_account": "s",
            "batch_network": "n",
            "batch_subnetwork": "s",
            "batch_image_repository": "repo",
            "batch_s2_key_secret": "secret",
        }
    )
    monkeypatch.setattr(deployments, "BatchSettings", lambda: settings)

    @asynccontextmanager
    async def busy() -> AsyncGenerator[None]:
        raise ConcurrencySlotAcquisitionError("shared budget busy")
        yield  # pragma: no cover

    @asynccontextmanager
    async def forbidden(
        settings: BatchSettings,
    ) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs]]:
        pytest.fail("busy controllers must not open clients")
        yield  # pragma: no cover

    monkeypatch.setattr(deployments, "one_s2_user", busy)
    monkeypatch.setattr(bootstrap, "open_batch_download", forbidden)
    with pytest.raises(ConcurrencySlotAcquisitionError, match="shared budget busy"):
        if diff:
            diff_routes = {
                "papers": deployments.s2_batch_download_diff,
                "s2orc_v2": deployments.s2_batch_download_fulltext_diff,
                "embeddings-specter_v2": deployments.s2_batch_download_embeddings_diff,
            }
            await diff_routes[dataset].fn(DIFF.model_copy(update={"dataset": dataset}))
        else:
            routes = {
                "papers": deployments.s2_batch_download,
                "s2orc_v2": deployments.s2_batch_download_fulltext,
                "embeddings-specter_v2": deployments.s2_batch_download_embeddings,
            }
            await routes[dataset].fn(COMMAND.model_copy(update={"dataset": dataset}))
