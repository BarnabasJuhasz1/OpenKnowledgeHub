from collections.abc import AsyncGenerator

import pytest

from openknowledge_adapters.semantic_scholar import SemanticScholar, connect
from tests.fakes.s2_http import FakeClock, FakeS2, s2_config


@pytest.fixture
def s2() -> FakeS2:
    return FakeS2()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def client(s2: FakeS2, clock: FakeClock) -> AsyncGenerator[SemanticScholar, None]:
    async with connect(s2_config(), transport=s2.transport, clock=clock, sleep=clock.sleep) as s2c:
        yield s2c
