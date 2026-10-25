"""Admission checks the full Batch listing before allowing conversion work."""

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import pytest
from gcloud.aio.auth import Token

from openknowledge_adapters.batch import BatchConfig, CloudBatchJobs
from openknowledge_core.ingestion.errors import SharedCapacityError


@pytest.mark.parametrize("foreign", [False, True])
async def test_conversion_listing_waits_for_owned_jobs_and_refuses_foreign_pages(
    monkeypatch: pytest.MonkeyPatch,
    foreign: bool,
) -> None:
    config = BatchConfig(
        project="p",
        region="r",
        service_account="a",
        network="n",
        subnetwork="s",
        image="repo@sha256:" + "a" * 64,
        machine_type="e2-standard-2",
        parallelism=1,
        max_run_seconds=60,
        poll_seconds=1,
        environment={},
        secrets={},
    )
    jobs = CloudBatchJobs(
        cast(aiohttp.ClientSession, object()),
        cast(Token, object()),
        config,
        api_root="unused",
        sleep=asyncio.sleep,
    )

    def active(name: str, family: str, state: str) -> dict[str, Any]:
        return {
            "name": name,
            "labels": {"operation-id": "id", "archive-family": family},
            "status": {"state": state},
        }

    call = AsyncMock(
        side_effect=[
            {
                "jobs": [active("owned-queued", "embedding-conversion", "QUEUED")],
                "nextPageToken": "next",
            },
            {
                "jobs": [
                    active("other", "core" if foreign else "embedding-conversion", "RUNNING"),
                    active("finished", "core", "SUCCEEDED"),
                ]
            },
        ]
    )
    monkeypatch.setattr(jobs, "_call", call)
    if foreign:
        with pytest.raises(SharedCapacityError):
            await jobs.conversion_running()
    else:
        assert await jobs.conversion_running() == ("owned-queued", "other")
    assert call.await_count == 2
    assert call.await_args_list[1].kwargs["params"] == {"pageToken": "next"}
    assert all(item.args[0] == "GET" for item in call.await_args_list)
