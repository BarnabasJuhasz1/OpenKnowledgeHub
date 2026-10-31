from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager

import pytest

# Prefect 3.8.8 uses this same helper in its test harness for plain threads.
from prefect.context import (
    _temporary_global_settings_context,  # pyright: ignore[reportPrivateUsage]
    get_settings_context,
)
from prefect.events.worker import EventsWorker
from prefect.logging.handlers import APILogWorker
from prefect.server.api.server import SubprocessASGIServer
from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_SERVER_DATABASE_CONNECTION_URL,
    temporary_settings,
)

from openknowledge_core.ingestion import DownloadDatasetService
from openknowledge_pipelines import admission, bootstrap
from openknowledge_pipelines.settings import PipelineSettings
from tests.fakes.ports import PAPERS, PRODUCER, FakeSource, FakeStore, clock


@pytest.fixture(scope="session")
def prefect_server() -> Generator[None, None, None]:
    # The stock harness uses file-backed SQLite with multiple connections. Prefect
    # 3.8.8's deferred read-then-write concurrency transactions can fail with
    # SQLITE_BUSY_SNAPSHOT when engine/event writes intervene; busy_timeout cannot
    # repair a stale snapshot. Its in-memory configuration instead uses one pooled
    # connection with no overflow, serializing transactions inside the real server.
    with temporary_settings(
        {PREFECT_SERVER_DATABASE_CONNECTION_URL: "sqlite+aiosqlite:///:memory:"}
    ):
        with SubprocessASGIServer() as server:
            with (
                temporary_settings({PREFECT_API_URL: server.api_url}),
                # Match the stock harness for threads without copied contextvars.
                _temporary_global_settings_context(get_settings_context()),
            ):
                try:
                    admission.ensure_closed()
                    admission.status("open")
                    yield
                finally:
                    # Drain while the server and its settings are still available.
                    APILogWorker.drain_all()
                    EventsWorker.drain_all()


@pytest.fixture
def source() -> FakeSource:
    fake = FakeSource()
    for name, data in PAPERS.items():
        fake.add("papers", name, data)
    return fake


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def service(source: FakeSource, store: FakeStore) -> DownloadDatasetService:
    return DownloadDatasetService(
        catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
    )


@pytest.fixture
def wired(
    monkeypatch: pytest.MonkeyPatch, service: DownloadDatasetService
) -> DownloadDatasetService:
    @asynccontextmanager
    async def open_service(settings: PipelineSettings) -> AsyncGenerator[DownloadDatasetService]:
        yield service

    @asynccontextmanager
    async def admitted() -> AsyncGenerator[None]:
        yield

    monkeypatch.setattr(admission, "execution", admitted)
    monkeypatch.setattr(bootstrap, "open_service", open_service)
    monkeypatch.setenv("S2_API_KEY", "test-key")
    monkeypatch.setenv("OKP_TASK_RETRY_DELAY_SECONDS", "0")
    return service
