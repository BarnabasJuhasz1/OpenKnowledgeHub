from __future__ import annotations

import gc
from pathlib import Path
from typing import Any, cast

import aiohttp
import gcloud.aio.storage.storage as gcloud_storage
import pytest
from gcloud.aio.storage import Storage
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from openknowledge_adapters import gcs
from openknowledge_adapters.gcs import GCS_API_ROOT, GcsArtifactStore, GcsConfig, connect
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.ingestion.errors import StoreRejected, StoreUnavailable
from tests.fakes.gcs_json_api import FakeGcs

CONFIG = GcsConfig(request_timeout_seconds=60, min_transfer_rate=1024)
EMULATOR = "http://127.0.0.1:9"
PATH = ObjectPath(bucket="raw", name="s2/2026-10-06/release.json")
OAUTH = "https://oauth2.googleapis.com/token"
METADATA = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
)
LOGIN = "gcloud auth application-default login"


@pytest.fixture(autouse=True)
def _no_credentials_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)


def fake_storage(
    monkeypatch: pytest.MonkeyPatch, *, failure: Exception | None = None
) -> list[tuple[str, bool]]:
    opened: list[tuple[str, bool]] = []

    class Token:
        async def get(self) -> str:
            if failure is not None:
                raise failure
            return "token"

    class FakeStorage:
        def __init__(self, *, api_root: str, api_is_dev: bool, **_: object) -> None:
            opened.append((api_root, api_is_dev))
            self.token = Token()

        async def __aenter__(self) -> FakeStorage:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr(gcs, "Storage", FakeStorage)
    return opened


def refused_by(url: str, status: int) -> aiohttp.ClientResponseError:
    info = aiohttp.RequestInfo(URL(url), "POST", CIMultiDictProxy(CIMultiDict[str]()), URL(url))
    return aiohttp.ClientResponseError(info, (), status=status)


async def test_gcs_itself_is_named_whatever_the_environment_says(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = fake_storage(monkeypatch)
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", EMULATOR)
    async with connect(CONFIG):
        pass
    assert opened == [(GCS_API_ROOT, False)]


async def test_storage_emulator_host_cannot_redirect_the_archive(
    monkeypatch: pytest.MonkeyPatch, gcs: FakeGcs
) -> None:
    class Token:
        def __init__(self, **_: object) -> None:
            pass

        async def get(self) -> str:
            return "token"

    monkeypatch.setattr(gcloud_storage, "Token", Token)
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", gcs.base_url)
    async with connect(CONFIG) as store:
        # The request goes to GCS, which tests cannot reach, where the emulator would have answered.
        with pytest.raises(StoreUnavailable, match="could not look up"):
            await store.stat(PATH)


async def test_missing_credentials_fail_before_any_object_is_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_storage(monkeypatch, failure=RuntimeError("Bearer topsecret"))
    with pytest.raises(StoreRejected, match=LOGIN) as info:
        async with connect(CONFIG):
            pass
    assert "topsecret" not in str(info.value)
    assert info.value.__context__ is None


async def test_a_token_service_outage_at_the_start_is_transient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_storage(monkeypatch, failure=refused_by(OAUTH, 503))
    with pytest.raises(StoreUnavailable, match="token service answered HTTP 503; rerun later"):
        async with connect(CONFIG):
            pass


async def test_an_unusable_credentials_file_is_named_and_leaks_no_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "missing.json"))
    with pytest.raises(
        StoreRejected, match="GOOGLE_APPLICATION_CREDENTIALS cannot be used"
    ) as info:
        async with connect(CONFIG):
            pass
    assert str(tmp_path) not in str(info.value)
    gc.collect()
    assert "Unclosed client session" not in caplog.text


async def test_an_emulator_reads_no_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, gcs: FakeGcs
) -> None:
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "missing.json"))
    async with connect(CONFIG.model_copy(update={"api_root": gcs.base_url})) as store:
        assert await store.stat(PATH) is None


async def test_an_empty_root_still_means_gcs_with_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = fake_storage(monkeypatch)
    async with connect(CONFIG.model_copy(update={"api_root": ""})):
        pass
    assert opened == [(GCS_API_ROOT, False)]


@pytest.mark.parametrize(
    ("url", "status", "error", "message"),
    [
        (OAUTH, 400, StoreRejected, LOGIN),
        (METADATA, 404, StoreRejected, LOGIN),
        (OAUTH, 503, StoreUnavailable, "token service answered HTTP 503"),
    ],
)
async def test_a_token_refresh_that_fails_mid_run_is_named_as_such(
    gcs: FakeGcs, url: str, status: int, error: type[Exception], message: str
) -> None:
    class Token:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self) -> str:
            self.calls += 1
            if self.calls > 1:
                raise refused_by(url, status)
            return "token"

    token = cast(Any, Token())
    async with Storage(api_root=gcs.base_url, api_is_dev=False, token=token) as storage:
        store = GcsArtifactStore(storage, request_timeout=30, min_transfer_rate=1024**2)
        assert await store.stat(PATH) is None
        with pytest.raises(error, match=message) as info:
            await store.stat(PATH)
    assert info.value.__context__ is None
