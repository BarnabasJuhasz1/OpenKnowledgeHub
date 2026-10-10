"""Fakes shared by all tests: the Semantic Scholar API, its signed file host and a GCS client.

Nothing here touches the network. The Prefect flow tests run against Prefect's own test
harness, which starts a throwaway server on localhost.
"""

from __future__ import annotations

import gzip
import io
import os
from collections.abc import Iterator
from types import ModuleType

import httpx
import pytest
from google.api_core.exceptions import Forbidden, PreconditionFailed

# The tests set it because the harness imports Prefect before the package.
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")

from openknowledge_adapters.semantic_scholar import SemanticScholarClient  # noqa: E402
from openknowledge_adapters.semantic_scholar.client import BASE_URL  # noqa: E402

API_HOST = "api.semanticscholar.org"
FILE_HOST = "files.test"
KEY = "test-key"
LINK_TTL = 3600


def gz(payload: bytes) -> bytes:
    return gzip.compress(payload, mtime=0)


class FakeClock:
    """Monotonic time that only moves when something sleeps."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakeS2:
    """The datasets API and the file host behind one httpx MockTransport.

    Links are signed with an ``Expires`` value from ``now``; the file host rejects links
    older than that with 403, like S3 does.
    """

    def __init__(self) -> None:
        self.releases = ["2026-09-29", "2026-10-06"]
        self.files: dict[str, dict[str, bytes]] = {}
        self.now = 1_000_000
        self.api_calls: list[str] = []
        self.file_calls: list[str] = []
        self.fail_next: list[int | Exception | httpx.Response] = []
        self.cut_next: set[str] = set()
        self.expire_after_first_list = False
        self.transport = httpx.MockTransport(self.handle)

    def add(self, dataset: str, name: str, data: bytes) -> None:
        self.files.setdefault(dataset, {})[name] = data

    def sign(self, folder: str, dataset: str, name: str) -> str:
        expires = self.now + LINK_TTL
        return (
            f"https://{FILE_HOST}/staging/{folder}/{dataset}/{name}"
            f"?AWSAccessKeyId=AKIAFAKE&Signature=c2lnbmF0dXJl&Expires={expires}"
        )

    def expire_links(self) -> None:
        self.now += LINK_TTL + 1

    def listings(self) -> int:
        return sum(1 for path in self.api_calls if "/dataset/" in path)

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == API_HOST:
            return self._api(request)
        if request.url.host == FILE_HOST:
            return self._file(request)
        return httpx.Response(502)

    def _api(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/datasets/v1/")
        self.api_calls.append(request.url.raw_path.decode().removeprefix("/datasets/v1/"))
        if request.headers.get("x-api-key") != KEY:
            return httpx.Response(403, json={"message": "Forbidden"})
        if self.fail_next:
            failure = self.fail_next.pop(0)
            if isinstance(failure, Exception):
                raise failure
            if isinstance(failure, httpx.Response):
                return failure
            return httpx.Response(failure)
        if path == "release/":
            return httpx.Response(200, json=self.releases)
        parts = path.strip("/").split("/")
        if len(parts) == 4 and parts[0] == "release" and parts[2] == "dataset":
            release, dataset = parts[1], parts[3]
            if release not in self.releases or dataset not in self.files:
                error = f"No dataset named {dataset} in release {release}"
                return httpx.Response(404, json={"error": error})
            files = [self.sign(release, dataset, name) for name in self.files[dataset]]
            if self.expire_after_first_list:
                self.expire_after_first_list = False
                self.expire_links()
            body = {"name": dataset, "description": "", "README": "", "files": files}
            return httpx.Response(200, json=body)
        if len(parts) == 5 and parts[0] == "diffs" and parts[2] == "to":
            start, end, dataset = parts[1], parts[3], parts[4]
            folder = f"{start}-to-{end}"
            diff = {
                "from_release": start,
                "to_release": end,
                "update_files": [self.sign(folder, dataset, "update-1.gz")],
                "delete_files": [self.sign(folder, dataset, "delete-1.gz")],
            }
            body = {"dataset": dataset, "start_release": start, "end_release": end}
            return httpx.Response(200, json={**body, "diffs": [diff]})
        return httpx.Response(404, json={"error": "unknown path"})

    def _file(self, request: httpx.Request) -> httpx.Response:
        segments = request.url.path.split("/")
        dataset, name = segments[-2], segments[-1]
        self.file_calls.append(name)
        if int(request.url.params["Expires"]) < self.now:
            return httpx.Response(403, text="<Error><Code>AccessDenied</Code></Error>")
        data = self.files[dataset][name]
        if name in self.cut_next:
            self.cut_next.discard(name)
            return httpx.Response(200, stream=CutStream(data[: len(data) // 2]))
        return httpx.Response(200, stream=httpx.ByteStream(data))  # streamed, as S3 would


class CutStream(httpx.SyncByteStream):
    """A response body that breaks off after its first piece."""

    def __init__(self, head: bytes) -> None:
        self.head = head

    def __iter__(self) -> Iterator[bytes]:
        yield self.head
        raise httpx.ReadError("connection lost")


class FakeBlob:
    """Enough of ``google.cloud.storage.Blob`` for an objectCreator + objectViewer account."""

    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self._bucket = bucket
        self.name = name
        self.chunk_size: int | None = None

    def exists(self, **_: object) -> bool:
        return self.name in self._bucket.objects and self.name not in self._bucket.hidden

    def _guard(self, if_generation_match: int | None) -> None:
        if self.name not in self._bucket.objects:
            return
        if if_generation_match == 0:
            raise PreconditionFailed(f"{self.name} exists")
        raise Forbidden(f"{self.name}: the account cannot overwrite")

    def upload_from_file(
        self, file_obj: io.RawIOBase, if_generation_match: int | None = None, **_: object
    ) -> None:
        """Read like the resumable client: full chunks until a short one, stored only then."""
        self._guard(if_generation_match)
        assert file_obj.tell() == 0
        size = self.chunk_size or 8 * 1024 * 1024
        parts = []
        while True:
            chunk = file_obj.read(size)
            parts.append(chunk)
            if len(chunk) < size:
                break
        self._bucket.objects[self.name] = b"".join(parts)

    def upload_from_string(
        self,
        data: bytes | str,
        content_type: str = "text/plain",
        if_generation_match: int | None = None,
        **_: object,
    ) -> None:
        self._guard(if_generation_match)
        self._bucket.objects[self.name] = data.encode() if isinstance(data, str) else data
        self._bucket.content_types[self.name] = content_type

    def open(self, mode: str = "r", **_: object) -> io.BytesIO:
        assert mode == "rb"
        return io.BytesIO(self._bucket.objects[self.name])


class FakeBucket:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str] = {}
        self.hidden: set[str] = set()  # names exists() denies, to stage a create race

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)


class FakeStorage:
    def __init__(self) -> None:
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, name: str) -> FakeBucket:
        return self.buckets.setdefault(name, FakeBucket())


@pytest.fixture
def s2() -> FakeS2:
    return FakeS2()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def s2_client(s2: FakeS2, clock: FakeClock) -> SemanticScholarClient:
    http = httpx.Client(base_url=BASE_URL, transport=s2.transport)
    return SemanticScholarClient(KEY, http=http, clock=clock, sleep=clock.sleep)


@pytest.fixture
def storage() -> FakeStorage:
    return FakeStorage()


@pytest.fixture(scope="session")
def prefect_server() -> None:
    from prefect.testing.utilities import prefect_test_harness

    with prefect_test_harness():
        yield


@pytest.fixture
def pipeline(
    monkeypatch: pytest.MonkeyPatch,
    s2: FakeS2,
    s2_client: SemanticScholarClient,
    storage: FakeStorage,
    prefect_server: None,
) -> ModuleType:
    """The flow module wired to the fakes, with no delay between task retries."""
    from openknowledge_pipelines.flows import s2_download as module

    http = httpx.Client(transport=s2.transport)
    monkeypatch.setattr(module, "s2_client", lambda: s2_client)
    monkeypatch.setattr(module, "storage_client", lambda: storage)
    monkeypatch.setattr(module, "http_client", lambda: http)
    monkeypatch.setattr(
        module, "download_file", module.download_file.with_options(retry_delay_seconds=0)
    )
    return module
