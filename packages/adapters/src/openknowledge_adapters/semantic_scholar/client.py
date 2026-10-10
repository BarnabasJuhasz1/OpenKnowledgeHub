"""Client for the Semantic Scholar Datasets API.

Reference: https://api.semanticscholar.org/api-docs/datasets. Every request carries the key
in the ``x-api-key`` header. Nothing in this module logs or formats the key or a signed link.
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

BASE_URL = "https://api.semanticscholar.org/datasets/v1/"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
INTERVAL_SECONDS = 1.0
ATTEMPTS = 8  # the waits double from BACKOFF_SECONDS, so the last attempt comes after 127 s
BACKOFF_SECONDS = 1.0
TIMEOUT_SECONDS = 30.0
_FILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class S2Error(Exception):
    """A request failed for good. Its message names the path and status, never the key or body."""


@dataclass(frozen=True)
class FileLink:
    """One dataset file. The URL is signed and expires, so it stays out of ``repr()``."""

    name: str
    url: str = field(repr=False)

    @classmethod
    def from_url(cls, url: str) -> FileLink:
        """Build a link from a signed URL; the file name is the last path segment.

        The listing is untrusted input: only https links with a plain file name pass, so a
        name can never carry a path separator, whitespace or a control character into the
        bucket, the logs or a task run name.
        """
        parts = urlsplit(url)
        name = parts.path.rsplit("/", 1)[-1]
        if parts.scheme != "https" or not _FILE_NAME.fullmatch(name) or not url.isprintable():
            raise S2Error("a file link in the listing is not an https URL with a plain file name")
        return cls(name=name, url=url)


@dataclass(frozen=True)
class Diff:
    """The files that take a dataset from one release to the next."""

    from_release: str
    to_release: str
    update_files: list[FileLink]
    delete_files: list[FileLink]


class RateLimiter:
    """Keeps calls at least ``interval`` seconds apart, also across threads."""

    def __init__(
        self,
        interval: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Allow the first call right away."""
        self._interval = interval
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_allowed = clock()

    def wait(self) -> None:
        """Block until the next call is allowed, then reserve the slot after it."""
        with self._lock:
            delay = self._next_allowed - self._clock()
            if delay > 0:
                self._sleep(delay)
            self._next_allowed = self._clock() + self._interval


class SemanticScholarClient:
    """Lists releases, resolves a dataset's file links and reads diffs between releases.

    At most one request per second, which is the documented limit for a key. Failed requests
    are retried with exponential backoff on 429 and 5xx and on transport errors.
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        http: httpx.Client | None = None,
        interval: float = INTERVAL_SECONDS,
        attempts: int = ATTEMPTS,
        backoff: float = BACKOFF_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._key = api_key or os.environ.get("S2_API_KEY", "")
        if not self._key:
            raise S2Error("S2_API_KEY is not set")
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        self._http = http or httpx.Client(base_url=BASE_URL, timeout=TIMEOUT_SECONDS)
        self._limiter = RateLimiter(interval, clock=clock, sleep=sleep)
        self._attempts = attempts
        self._backoff = backoff
        self._sleep = sleep

    def list_releases(self) -> list[str]:
        """All release ids, oldest first."""
        return sorted(self._get("release/"))

    def dataset_files(self, release_id: str, dataset: str) -> list[FileLink]:
        """Signed links for every file of ``dataset`` in the release."""
        body = self._get(_path("release", release_id, "dataset", dataset))
        return [FileLink.from_url(url) for url in body["files"]]

    def diffs(self, start_release: str, end_release: str, dataset: str) -> list[Diff]:
        """The diffs that take ``dataset`` from ``start_release`` to ``end_release``."""
        body = self._get(_path("diffs", start_release, "to", end_release, dataset))
        return [
            Diff(
                from_release=diff["from_release"],
                to_release=diff["to_release"],
                update_files=[FileLink.from_url(url) for url in diff["update_files"]],
                delete_files=[FileLink.from_url(url) for url in diff["delete_files"]],
            )
            for diff in body["diffs"]
        ]

    def _get(self, path: str) -> Any:  # noqa: ANN401 - the API returns lists or objects
        for attempt in range(1, self._attempts + 1):
            self._limiter.wait()
            retry_after = 0.0
            try:
                response = self._http.get(path, headers={"x-api-key": self._key})
            except httpx.TransportError as exc:
                failure = f"GET {path}: {type(exc).__name__}"
            else:
                if response.is_success:
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise S2Error(f"GET {path}: the response is not JSON") from exc
                failure = f"GET {path}: HTTP {response.status_code}"
                if response.status_code not in RETRY_STATUSES:
                    raise S2Error(failure)
                retry_after = _retry_after(response)
            if attempt < self._attempts:
                self._sleep(max(self._backoff * 2 ** (attempt - 1), retry_after))
        raise S2Error(f"{failure} after {self._attempts} attempts")


def _path(*segments: str) -> str:
    """Join path segments, percent-encoding each so an id cannot change the route."""
    if any(segment in ("", ".", "..") for segment in segments):
        raise S2Error("release ids and dataset names cannot be empty or dots")
    return "/".join(quote(segment, safe="") for segment in segments)


def _retry_after(response: httpx.Response) -> float:
    """Seconds the server asked for, when it said so in whole seconds."""
    value = response.headers.get("retry-after", "")
    return float(value) if value.isdecimal() else 0.0
