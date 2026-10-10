"""The GCS adapter against the fake storage client."""

from __future__ import annotations

import gzip
import hashlib
import logging
import pickle

import httpx
import pytest
from google.auth.exceptions import RefreshError

from openknowledge_adapters import gcs
from openknowledge_adapters.gcs import objects
from tests.conftest import CutStream, FakeS2, FakeStorage

URL = "https://files.test/staging/2026-10-06/papers/part-0.gz?Signature=secret&Expires=9999999"
EXPIRED_URL = URL.replace("Expires=9999999", "Expires=1")


class Recorder:
    """An observer that keeps what it saw."""

    def __init__(self, reject: bool = False) -> None:
        self.seen = bytearray()
        self.closed = 0
        self.reject = reject

    def update(self, data: bytes) -> None:
        self.seen += data

    def close(self) -> None:
        self.closed += 1
        if self.reject:
            raise ValueError("rejected")


@pytest.fixture
def one_kib_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Chunk sizes small enough that a few kilobytes take several upload and read rounds."""
    monkeypatch.setattr(objects, "CHUNK_SIZE", 1024)
    monkeypatch.setattr(objects, "READ_SIZE", 100)


@pytest.fixture
def http(s2: FakeS2) -> httpx.Client:
    return httpx.Client(transport=s2.transport)


def test_streams_url_into_object_with_digest(
    one_kib_chunks: None, s2: FakeS2, http: httpx.Client, storage: FakeStorage
) -> None:
    data = bytes(range(256)) * 20
    s2.add("papers", "part-0.gz", data)
    seen = Recorder()
    digest = gcs.stream_url_to_object(storage, http, URL, "raw", "a/part-0.gz", observer=seen)
    assert digest == gcs.ObjectDigest(hashlib.sha256(data).hexdigest(), len(data))
    assert storage.bucket("raw").objects["a/part-0.gz"] == data
    assert bytes(seen.seen) == data
    assert seen.closed == 1


def test_existing_object_counts_as_done_without_a_request(
    s2: FakeS2, http: httpx.Client, storage: FakeStorage
) -> None:
    storage.bucket("raw").objects["a/part-0.gz"] = b"old"
    assert gcs.stream_url_to_object(storage, http, URL, "raw", "a/part-0.gz") is None
    assert s2.file_calls == []
    assert storage.bucket("raw").objects["a/part-0.gz"] == b"old"


def test_an_object_created_between_the_exists_check_and_the_upload_is_left_alone(
    s2: FakeS2, http: httpx.Client, storage: FakeStorage
) -> None:
    s2.add("papers", "part-0.gz", b"new")
    bucket = storage.bucket("raw")
    bucket.objects["a/part-0.gz"] = b"theirs"
    bucket.hidden.add("a/part-0.gz")  # exists() says no, the create precondition says yes
    assert gcs.stream_url_to_object(storage, http, URL, "raw", "a/part-0.gz") is None
    assert bucket.objects["a/part-0.gz"] == b"theirs"


def test_forbidden_source_raises_without_the_query_string(
    s2: FakeS2, http: httpx.Client, storage: FakeStorage
) -> None:
    s2.add("papers", "part-0.gz", b"data")
    with pytest.raises(gcs.SourceError) as info:
        gcs.stream_url_to_object(storage, http, EXPIRED_URL, "raw", "a/part-0.gz")
    assert info.value.status_code == 403
    assert "secret" not in str(info.value)
    assert "https://files.test/staging/2026-10-06/papers/part-0.gz" in str(info.value)
    assert storage.bucket("raw").objects == {}


def test_rejected_by_the_observer_creates_nothing(
    s2: FakeS2, http: httpx.Client, storage: FakeStorage
) -> None:
    s2.add("papers", "part-0.gz", b"data")
    with pytest.raises(ValueError, match="rejected"):
        gcs.stream_url_to_object(
            storage, http, URL, "raw", "a/part-0.gz", observer=Recorder(reject=True)
        )
    assert storage.bucket("raw").objects == {}


def test_source_cut_off_mid_stream_creates_nothing(storage: FakeStorage) -> None:
    cut = CutStream(b"first part")
    http = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=cut)))
    with pytest.raises(httpx.ReadError):
        gcs.stream_url_to_object(storage, http, URL, "raw", "a/part-0.gz")
    assert storage.bucket("raw").objects == {}


def test_create_object_never_overwrites(storage: FakeStorage) -> None:
    assert gcs.create_object(storage, "raw", "r.json", b"{}", "application/json") is True
    assert gcs.create_object(storage, "raw", "r.json", b"[]", "application/json") is False
    assert storage.bucket("raw").objects["r.json"] == b"{}"
    assert storage.bucket("raw").content_types["r.json"] == "application/json"


def test_digest_object_reads_back_through_the_observer(
    one_kib_chunks: None, storage: FakeStorage
) -> None:
    data = b"x" * 2500
    storage.bucket("raw").objects["a/part-0.gz"] = data
    seen = Recorder()
    digest = gcs.digest_object(storage, "raw", "a/part-0.gz", observer=seen)
    assert digest == gcs.ObjectDigest(hashlib.sha256(data).hexdigest(), 2500)
    assert bytes(seen.seen) == data
    assert seen.closed == 1


def test_redact_drops_the_query_string() -> None:
    assert objects.redact(URL) == "https://files.test/staging/2026-10-06/papers/part-0.gz"


def test_object_exists(storage: FakeStorage) -> None:
    assert gcs.object_exists(storage, "raw", "x") is False
    storage.bucket("raw").objects["x"] = b""
    assert gcs.object_exists(storage, "raw", "x") is True


def test_httpx_log_lines_carry_no_query_string(
    s2: FakeS2, http: httpx.Client, storage: FakeStorage, caplog: pytest.LogCaptureFixture
) -> None:
    s2.add("papers", "part-0.gz", b"data")
    caplog.set_level(logging.INFO, logger="httpx")
    gcs.stream_url_to_object(storage, http, URL, "raw", "a/part-0.gz")
    lines = [record.getMessage() for record in caplog.records if record.name == "httpx"]
    assert lines
    assert all("https://files.test/staging/2026-10-06/papers/part-0.gz" in line for line in lines)
    assert not any("secret" in line for line in lines)


def test_bytes_are_stored_as_served_whatever_the_content_encoding(storage: FakeStorage) -> None:
    payload = gzip.compress(b"json lines", mtime=0)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(
            200, stream=httpx.ByteStream(payload), headers={"content-encoding": "gzip"}
        )

    http = httpx.Client(transport=httpx.MockTransport(handler))
    digest = gcs.stream_url_to_object(storage, http, URL, "raw", "a/x.gz")
    assert storage.bucket("raw").objects["a/x.gz"] == payload
    assert digest is not None and digest.size == len(payload)


def test_redact_also_drops_userinfo() -> None:
    assert (
        objects.redact("https://user:pw@files.test:8443/p?Signature=s")
        == "https://files.test:8443/p"
    )
    assert (
        objects.redact("https://[2001:db8::1]:99999/p?Signature=s")
        == "https://[2001:db8::1]:99999/p"
    )


def test_source_error_survives_pickling_and_stays_redacted() -> None:
    error = pickle.loads(pickle.dumps(gcs.SourceError(403, URL)))
    assert error.status_code == 403
    assert str(error) == "HTTP 403 from https://files.test/staging/2026-10-06/papers/part-0.gz"
    assert "secret" not in repr(error)


@pytest.mark.parametrize(
    ("error", "transient"),
    [
        (gcs.SourceError(503, URL), True),
        (gcs.SourceError(429, URL), True),
        (gcs.SourceError(403, URL), False),
        (gcs.SourceError(404, URL), False),
        (httpx.ReadError("lost"), True),
        (httpx.ConnectTimeout("slow"), True),
        (RefreshError("metadata server unavailable"), True),
        (ValueError("rejected"), False),
    ],
)
def test_is_transient(error: Exception, transient: bool) -> None:
    assert gcs.is_transient(error) is transient
