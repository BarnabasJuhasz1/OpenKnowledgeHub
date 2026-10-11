import pytest

from openknowledge_core.ingestion import DownloadDatasetService
from tests.fakes.ports import PRODUCER, FakeSource, FakeStore, clock


@pytest.fixture
def source() -> FakeSource:
    return FakeSource()


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def service(source: FakeSource, store: FakeStore) -> DownloadDatasetService:
    return DownloadDatasetService(
        catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
    )
