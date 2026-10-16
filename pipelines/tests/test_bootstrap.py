from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from openknowledge_adapters import clickhouse, gcs, semantic_scholar
from openknowledge_core.ingestion import DownloadDatasetService, WatchdogService
from openknowledge_pipelines import bootstrap
from openknowledge_pipelines.settings import PipelineSettings, WatchdogSettings
from tests.fakes.ports import FakeApplied, FakeStore

DIGEST = "sha256:" + "a" * 64
LOAD_ENV = {
    "OKP_CLICKHOUSE_URL": "http://clickhouse:8123",
    "CLICKHOUSE_PASSWORD": "dev",
    "OKP_CLICKHOUSE_RAW_BUCKET": "raw",
}


def settings() -> PipelineSettings:
    return PipelineSettings()  # pyright: ignore[reportCallIssue]


async def test_the_service_is_wired_from_settings_and_closes_its_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2_API_KEY", "test-key")
    monkeypatch.setenv("OKP_S2_ATTEMPTS", "3")
    monkeypatch.setenv("OKP_GCS_REQUEST_TIMEOUT_SECONDS", "30")
    events: list[str] = []
    s2_connect = semantic_scholar.connect

    @asynccontextmanager
    async def spy_s2(
        config: semantic_scholar.SemanticScholarConfig,
    ) -> AsyncGenerator[semantic_scholar.SemanticScholar]:
        assert (config.api_key.get_secret_value(), config.attempts) == ("test-key", 3)
        async with s2_connect(config) as connected:
            events.append("s2 open")
            yield connected
        events.append("s2 closed")

    @asynccontextmanager
    async def spy_gcs(config: gcs.GcsConfig) -> AsyncGenerator[FakeStore]:
        assert (config.request_timeout_seconds, config.min_transfer_rate) == (30, 256 * 1024)
        assert config.api_root is None
        events.append("gcs open")
        yield FakeStore()  # the real client would fetch Google credentials here
        events.append("gcs closed")

    monkeypatch.setattr(semantic_scholar, "connect", spy_s2)
    monkeypatch.setattr(gcs, "connect", spy_gcs)
    async with bootstrap.open_service(settings()) as service:
        assert isinstance(service, DownloadDatasetService)
        assert events == ["s2 open", "gcs open"]
    assert events == ["s2 open", "gcs open", "gcs closed", "s2 closed"]


def test_settings_load_and_the_key_never_shows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S2_API_KEY", "topsecret")
    monkeypatch.setenv("OKP_TASK_RETRIES", "5")
    loaded = settings()
    assert loaded.s2_api_key.get_secret_value() == "topsecret"
    assert loaded.task_retries == 5
    assert "topsecret" not in repr(loaded) and "topsecret" not in loaded.model_dump_json()


@pytest.mark.parametrize("key", ["topsecret\u00a0", "top secret", "topsecret\u200b"])
def test_a_key_httpx_cannot_send_is_refused_without_echo(
    monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    monkeypatch.setenv("S2_API_KEY", key)
    with pytest.raises(ValidationError, match="printable ASCII") as info:
        settings()
    assert "secret" not in str(info.value)


def test_a_bad_setting_does_not_echo_its_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S2_API_KEY", "k")
    monkeypatch.setenv("OKP_S2_ATTEMPTS", "many-topsecret")
    with pytest.raises(ValidationError) as info:
        settings()
    assert "topsecret" not in str(info.value)


def test_the_producer_names_the_package_and_the_image(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S2_API_KEY", "k")
    monkeypatch.setenv("OKP_IMAGE_DIGEST", DIGEST)
    producer = bootstrap.producer(settings())
    assert (producer.transform, producer.software_version, producer.image_digest) == (
        "verbatim",
        "0.1.0",
        DIGEST,
    )


@pytest.mark.parametrize(
    ("configured", "expected"),
    [(None, 60.0), ("7.5", 7.5)],
    ids=["default", "OKP_WATCHDOG_TIMEOUT_SECONDS=7.5"],
)
async def test_the_watchdog_queries_with_its_own_timeout_not_the_load_one(
    monkeypatch: pytest.MonkeyPatch, configured: str | None, expected: float
) -> None:
    for name, value in LOAD_ENV.items():
        monkeypatch.setenv(name, value)
    if configured is not None:
        monkeypatch.setenv("OKP_WATCHDOG_TIMEOUT_SECONDS", configured)
    configs: list[clickhouse.ClickHouseConfig] = []

    @asynccontextmanager
    async def spy_clickhouse(
        config: clickhouse.ClickHouseConfig,
    ) -> AsyncGenerator[SimpleNamespace]:
        configs.append(config)
        yield SimpleNamespace(applied=FakeApplied())  # the real client would open a pool here

    monkeypatch.setattr(clickhouse, "connect", spy_clickhouse)
    settings = WatchdogSettings()  # pyright: ignore[reportCallIssue]
    async with bootstrap.open_watchdog(settings) as service:
        assert isinstance(service, WatchdogService)
    [config] = configs
    assert (config.timeout_seconds, settings.clickhouse_timeout_seconds) == (expected, 3600.0)
