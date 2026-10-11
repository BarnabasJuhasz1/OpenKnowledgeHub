from collections.abc import AsyncGenerator

import pytest

from openknowledge_adapters.gcs import GcsArtifactStore, GcsConfig, connect
from tests.fakes.gcs_json_api import FakeGcs, running


@pytest.fixture
async def gcs() -> AsyncGenerator[FakeGcs, None]:
    async with running(FakeGcs()) as fake:
        yield fake


@pytest.fixture
async def store(gcs: FakeGcs) -> AsyncGenerator[GcsArtifactStore, None]:
    async with connect(
        GcsConfig(request_timeout_seconds=30, min_transfer_rate=1024**2, api_root=gcs.base_url)
    ) as adapter:
        yield adapter
