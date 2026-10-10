"""The GCS adapter driving the real storage client against a fake resumable upload API.

google-cloud-storage reads the source through its own chunker, so this is the test that holds
the _ChunkReader contract against the installed library rather than against the fake blob.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from urllib.parse import unquote

import google_crc32c
import httpx
import pytest
import requests
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

from openknowledge_adapters import gcs
from openknowledge_adapters.gcs import objects
from tests.test_gcs import URL, Recorder

CHUNK = 256 * 1024  # the smallest chunk size GCS accepts
JSON = {"content-type": "application/json"}


def reply(
    method: str, url: str, status: int, body: bytes = b"", headers: dict | None = None
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    response._content = body
    response.request = requests.Request(method, url).prepare()
    return response


class FakeJsonApi:
    """The GCS JSON API as the client speaks it: object GET, resumable POST, chunk PUTs."""

    is_mtls = False

    def __init__(
        self, existing: dict[str, bytes] | None = None, hidden: set[str] | None = None
    ) -> None:
        self.objects = dict(existing or {})
        self.hidden = hidden or set()  # names the GET denies, to stage a create race
        self.sessions: dict[str, dict] = {}
        self.ranges: list[str] = []  # the Content-Range of every PUT, in order

    def request(
        self,
        method: str,
        url: str,
        data: bytes | None = None,
        headers: dict | None = None,
        **_: object,
    ) -> requests.Response:
        headers = headers or {}
        if method == "GET" and "/o/" in url:
            name = unquote(url.split("/o/")[1].split("?")[0])
            if name in self.objects and name not in self.hidden:
                return reply(method, url, 200, json.dumps({"name": name}).encode(), JSON)
            return reply(
                method, url, 404, b'{"error": {"code": 404, "message": "Not Found"}}', JSON
            )
        if method == "POST" and "uploadType=resumable" in url:
            name = json.loads(data)["name"]
            if "ifGenerationMatch=0" in url and name in self.objects:
                return reply(
                    method,
                    url,
                    412,
                    b'{"error": {"code": 412, "message": "conditionNotMet"}}',
                    JSON,
                )
            session = f"session-{len(self.sessions)}"
            self.sessions[session] = {"name": name, "data": bytearray()}
            location = {"Location": f"https://storage.googleapis.com/upload/{session}"}
            return reply(method, url, 200, headers=location)
        if method == "PUT" and "/upload/" in url:
            session = self.sessions[url.rsplit("/", 1)[1]]
            content_range = {key.lower(): value for key, value in headers.items()}["content-range"]
            self.ranges.append(content_range)
            match = re.fullmatch(r"bytes (\*|(\d+)-(\d+))/(\*|\d+)", content_range)
            assert match, content_range
            if match.group(1) != "*":
                assert int(match.group(2)) == len(session["data"])
                assert int(match.group(3)) - int(match.group(2)) + 1 == len(data)
                session["data"] += data
            if match.group(4) == "*":
                end = len(session["data"]) - 1
                return reply(
                    method, url, 308, headers={"Range": f"bytes=0-{end}"} if end >= 0 else {}
                )
            assert int(match.group(4)) == len(session["data"])
            stored = bytes(session["data"])
            self.objects[session["name"]] = stored
            crc = base64.b64encode(google_crc32c.Checksum(stored).digest()).decode()
            md5 = base64.b64encode(hashlib.md5(stored).digest()).decode()
            meta = {"name": session["name"], "bucket": "raw", "size": str(len(stored))}
            meta |= {"crc32c": crc, "md5Hash": md5}  # the client checks whichever it computed
            return reply(method, url, 200, json.dumps(meta).encode(), JSON)
        raise AssertionError(f"unexpected request {method} {url}")


@pytest.fixture
def quarter_mib_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(objects, "CHUNK_SIZE", CHUNK)
    monkeypatch.setattr(
        objects, "READ_SIZE", 100 * 1024
    )  # no divisor of CHUNK: pieces straddle chunks


def store(api: FakeJsonApi) -> storage.Client:
    return storage.Client(project="test", credentials=AnonymousCredentials(), _http=api)


def source(payload: bytes) -> httpx.Client:
    def serve(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(payload))  # streamed, as S3 would

    return httpx.Client(transport=httpx.MockTransport(serve))


def pattern(size: int) -> bytes:
    return (bytes(range(256)) * (size // 256 + 1))[:size]


@pytest.mark.parametrize("size", [3 * CHUNK + 1234, 2 * CHUNK, 11_000, 0])
def test_uploads_in_chunks_and_names_the_total_only_at_the_end(
    quarter_mib_chunks: None, size: int
) -> None:
    payload = pattern(size)
    api = FakeJsonApi()
    seen = Recorder()
    digest = gcs.stream_url_to_object(
        store(api), source(payload), URL, "raw", "a/x.gz", observer=seen
    )
    assert api.objects["a/x.gz"] == payload
    assert digest == gcs.ObjectDigest(hashlib.sha256(payload).hexdigest(), size)
    assert (seen.closed, bytes(seen.seen)) == (1, payload)
    assert api.ranges[-1].endswith(f"/{size}")
    assert all(content_range.endswith("/*") for content_range in api.ranges[:-1])


def test_a_rejection_at_the_end_sends_no_final_request(quarter_mib_chunks: None) -> None:
    api = FakeJsonApi()
    with pytest.raises(ValueError, match="rejected"):
        gcs.stream_url_to_object(
            store(api),
            source(pattern(3 * CHUNK)),
            URL,
            "raw",
            "a/x.gz",
            observer=Recorder(reject=True),
        )
    assert api.objects == {}
    assert len(api.ranges) == 3
    assert all(content_range.endswith("/*") for content_range in api.ranges)


def test_an_existing_object_is_done_without_an_upload(quarter_mib_chunks: None) -> None:
    api = FakeJsonApi(existing={"a/x.gz": b"theirs"})
    assert gcs.stream_url_to_object(store(api), source(b"new"), URL, "raw", "a/x.gz") is None
    assert api.ranges == [] and api.objects["a/x.gz"] == b"theirs"


def test_a_412_at_initiation_is_done_too(quarter_mib_chunks: None) -> None:
    api = FakeJsonApi(existing={"a/x.gz": b"theirs"}, hidden={"a/x.gz"})
    assert gcs.stream_url_to_object(store(api), source(b"new"), URL, "raw", "a/x.gz") is None
    assert api.ranges == [] and api.objects["a/x.gz"] == b"theirs"
