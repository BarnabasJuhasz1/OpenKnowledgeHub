"""Create-only objects in Google Cloud Storage.

The job account holds objectCreator and objectViewer on the raw bucket, so nothing here can
overwrite or delete. Every write carries ``if_generation_match=0`` and treats an object that
already exists as done. Importing this module caps the ``httpx`` logger at WARNING unless a
level was set, because its INFO line prints every request URL, signature included.
"""

from __future__ import annotations

import hashlib
import io
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlsplit

import httpx
from google.api_core.exceptions import PreconditionFailed, RetryError, ServerError, TooManyRequests
from google.auth.exceptions import RefreshError
from google.cloud import storage

Client = storage.Client

CHUNK_SIZE = 32 * 1024 * 1024  # bytes per resumable upload request, a multiple of 256 KiB
READ_SIZE = 1024 * 1024


class ByteObserver(Protocol):
    """Sees every byte once, in order; ``close`` runs once the source is exhausted."""

    def update(self, data: bytes) -> None: ...

    def close(self) -> None:
        """The source ended. Raise to reject the data before an object is created."""


@dataclass(frozen=True)
class ObjectDigest:
    """What was read: its SHA-256 hex digest and its length in bytes."""

    sha256: str
    size: int


class SourceError(Exception):
    """The source URL did not deliver. Names the status and the URL without its query string."""

    def __init__(self, status_code: int, url: str) -> None:
        """Keep the status so a caller can tell an expired link (403) from the rest."""
        super().__init__(status_code, redact(url))
        self.status_code = status_code

    def __str__(self) -> str:
        """Status and redacted URL, built from args so pickling keeps both."""
        return f"HTTP {self.args[0]} from {self.args[1]}"


def redact(url: str) -> str:
    """The URL reduced to scheme, host and path: no query string, where a signature lives."""
    parts = urlsplit(url)
    host = parts.netloc.rpartition("@")[-1]  # drops userinfo, keeps the port and IPv6 brackets
    return f"{parts.scheme}://{host}{parts.path}"


def is_transient(exc: BaseException) -> bool:
    """Whether a failure raised from here may pass on a retry: network trouble, not bad data."""
    if isinstance(exc, SourceError):
        return exc.status_code == 429 or exc.status_code >= 500
    # RefreshError is how metadata-server trouble surfaces on GCE; a bad credential then costs the
    # task its two retries, 30 seconds apart.
    return isinstance(
        exc, (httpx.TransportError, ServerError, TooManyRequests, RetryError, RefreshError)
    )


def client() -> Client:
    """A storage client on Application Default Credentials."""
    return storage.Client()


def object_exists(store: Client, bucket: str, name: str) -> bool:
    return store.bucket(bucket).blob(name).exists()


def stream_url_to_object(
    store: Client,
    http: httpx.Client,
    url: str,
    bucket: str,
    name: str,
    *,
    observer: ByteObserver | None = None,
) -> ObjectDigest | None:
    """Stream ``url`` into ``gs://bucket/name``, hashing and counting on the way.

    Returns None when the object already exists, which a create-only account counts as done.
    GCS creates the object on the final upload request only, and that request is sent after
    the source is exhausted and ``observer.close()`` has passed, so a failure before it leaves
    nothing behind. Should GCS report a checksum mismatch on that final request, the object
    stays, since this account cannot delete it, and the next run checks the stored copy.
    The bytes are stored as served, without transfer decoding.
    """
    _guard_httpx_logging()
    blob = store.bucket(bucket).blob(name)
    if blob.exists():
        return None
    blob.chunk_size = CHUNK_SIZE
    with http.stream("GET", url, headers={"accept-encoding": "identity"}) as response:
        if response.status_code != 200:
            raise SourceError(response.status_code, url)
        digester = _Digester(observer)
        try:
            blob.upload_from_file(
                _ChunkReader(response.iter_raw(READ_SIZE), digester), if_generation_match=0
            )
        except PreconditionFailed:
            return None  # created by another run since the exists() check
    return digester.result()


def digest_object(
    store: Client, bucket: str, name: str, *, observer: ByteObserver | None = None
) -> ObjectDigest:
    """Read an existing object back through the same hashing and observer."""
    digester = _Digester(observer)
    with store.bucket(bucket).blob(name).open("rb", chunk_size=CHUNK_SIZE) as source:
        while data := source.read(READ_SIZE):
            digester.update(data)
    digester.close()
    return digester.result()


def create_object(store: Client, bucket: str, name: str, data: bytes, content_type: str) -> bool:
    """Create a small object in one request. False when it already existed; it is left as is."""
    try:
        store.bucket(bucket).blob(name).upload_from_string(
            data, content_type=content_type, if_generation_match=0
        )
    except PreconditionFailed:
        return False
    return True


class _RedactUrls(logging.Filter):
    """Strips query strings from the URLs in httpx's own log lines."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.args, tuple):
                record.args = tuple(
                    redact(str(arg)) if isinstance(arg, httpx.URL) or "://" in str(arg) else arg
                    for arg in record.args
                )
        except Exception:
            return False  # a line that cannot be redacted is not logged
        return True


_HTTPX_LOG = logging.getLogger("httpx")
_REDACT_URLS = _RedactUrls()


def _guard_httpx_logging() -> None:
    """Keep signatures out of httpx's log lines, which print every request URL at INFO."""
    _HTTPX_LOG.addFilter(_REDACT_URLS)
    if _HTTPX_LOG.level == logging.NOTSET:
        _HTTPX_LOG.setLevel(logging.WARNING)


_guard_httpx_logging()


class _Digester:
    """SHA-256 and byte count over everything passed through, with the observer in tow."""

    def __init__(self, observer: ByteObserver | None) -> None:
        self._hash = hashlib.sha256()
        self._size = 0
        self._observer = observer

    def update(self, data: bytes) -> None:
        self._hash.update(data)
        self._size += len(data)
        if self._observer is not None:
            self._observer.update(data)

    def close(self) -> None:
        if self._observer is not None:
            self._observer.close()

    def result(self) -> ObjectDigest:
        return ObjectDigest(sha256=self._hash.hexdigest(), size=self._size)


class _ChunkReader(io.RawIOBase):
    """Read-once file over a chunk iterator, feeding a digester as bytes come in.

    The resumable upload reads ``chunk_size`` bytes per request and takes a shorter read as
    the end of the stream, so ``read`` returns the full size requested until the source runs
    out. The digester is closed before that final short read is handed back.
    """

    def __init__(self, chunks: Iterator[bytes], digester: _Digester) -> None:
        super().__init__()
        self._chunks = chunks
        self._digester = digester
        self._buffer = bytearray()
        self._position = 0
        self._exhausted = False

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            raise ValueError("read() needs a size; the source is streamed")
        while len(self._buffer) < size and not self._exhausted:
            data = next(self._chunks, None)
            if data is None:
                self._exhausted = True
                self._digester.close()
            else:
                self._digester.update(data)
                self._buffer += data
        out = bytes(self._buffer[:size])
        del self._buffer[:size]
        self._position += len(out)
        return out
