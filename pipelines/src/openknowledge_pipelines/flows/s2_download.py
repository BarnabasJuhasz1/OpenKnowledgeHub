"""Download one Semantic Scholar dataset release into the raw bucket.

Each file lands at ``<prefix>/<release>/<dataset>/<file>`` and gets a ``<file>.receipt.json``
beside it once the upload is complete and the gzip stream proved whole. A rerun skips every
file that has a receipt, so the flow can be restarted until it is done.
"""

from __future__ import annotations

import functools
import json
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from prefect import Task, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.client.schemas.objects import TaskRun
from prefect.states import State

from openknowledge_adapters import gcs
from openknowledge_adapters.semantic_scholar import FileLink, S2Error, SemanticScholarClient

RECEIPT_SUFFIX = ".receipt.json"
DEFAULT_PREFIX = "semantic-scholar"
LATEST = "latest"
RETRIES = 2
RETRY_DELAY_SECONDS = 30
READ_TIMEOUT_SECONDS = 300.0
CONNECT_TIMEOUT_SECONDS = 30.0


class GzipError(Exception):
    """The gzip stream was cut short or did not decode."""


class GzipCheck:
    """Decompresses the bytes as they pass; ``close`` fails unless every member ended cleanly.

    zlib verifies each member's CRC-32 and length when the member ends, so a stream that
    passes is complete and intact. The output is discarded in bounded pieces.
    """

    _OUT = 8 * 1024 * 1024

    def __init__(self) -> None:
        self.members = 0
        self._start_member()

    def _start_member(self) -> None:
        self._z = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._open = False

    def update(self, data: bytes) -> None:
        """Feed the next bytes, which may span any number of members."""
        while data:
            if not self._open and self.members:
                data = data.lstrip(b"\x00")  # gzip tools accept zero padding after a member
                if not data:
                    return
            self._open = True
            try:
                self._z.decompress(data, self._OUT)
            except zlib.error as exc:
                raise GzipError(f"gzip stream does not decode: {exc}") from exc
            if self._z.eof:
                self.members += 1
                data = self._z.unused_data
                self._start_member()
            else:
                data = self._z.unconsumed_tail

    def close(self) -> None:
        """Fail unless at least one member was seen and the last one ended."""
        if self.members == 0 or self._open:
            raise GzipError("gzip stream ends before its last member does")


@dataclass(frozen=True)
class Target:
    """Where one dataset release lands."""

    bucket: str
    prefix: str
    release: str
    dataset: str

    def object_name(self, file_name: str) -> str:
        """``<prefix>/<release>/<dataset>/<file_name>`` with no empty segments."""
        parts = (self.prefix.strip("/"), self.release, self.dataset, file_name)
        return "/".join(part for part in parts if part)

    def receipt_name(self, file_name: str) -> str:
        """The receipt that marks ``file_name`` as done."""
        return self.object_name(file_name) + RECEIPT_SUFFIX


@dataclass(frozen=True)
class Summary:
    """What one run did: files listed, completed now, and skipped for an existing receipt."""

    release: str
    dataset: str
    files: int
    completed: int
    skipped: int


@functools.cache
def s2_client() -> SemanticScholarClient:
    """One client per process, so the rate limit holds across tasks."""
    return SemanticScholarClient()


@functools.cache
def storage_client() -> gcs.Client:
    return gcs.client()


@functools.cache
def http_client() -> httpx.Client:
    """For the signed file links. Those are not API calls, so they are not rate limited."""
    return httpx.Client(
        timeout=httpx.Timeout(READ_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS)
    )


def _retry_if_transient(task: Task, task_run: TaskRun, state: State) -> bool:
    """Retry network trouble only; a bad gzip or a missing file will not improve by trying again."""
    try:
        state.result()
    except Exception as exc:
        return gcs.is_transient(exc)
    return False


@task(
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_if_transient,
    cache_policy=NO_CACHE,
    task_run_name="download {link.name}",
)
def download_file(link: FileLink, target: Target) -> dict | None:
    """Upload one file and then its receipt. Returns None when the receipt already existed."""
    log = get_run_logger()
    store = storage_client()
    if gcs.object_exists(store, target.bucket, target.receipt_name(link.name)):
        log.info("%s: receipt exists, skipping", link.name)
        return None
    digest = _upload(store, link, target)
    if digest is None:
        log.info("%s: object exists without a receipt, checking the stored copy", link.name)
        digest = _digest_stored(store, link, target)
    receipt = {
        "release": target.release,
        "dataset": target.dataset,
        "file": link.name,
        "sha256": digest.sha256,
        "bytes": digest.size,
        "completed_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    gcs.create_object(
        store,
        target.bucket,
        target.receipt_name(link.name),
        json.dumps(receipt, indent=1).encode(),
        "application/json",
    )
    log.info("%s: %d bytes, sha256 %s", link.name, digest.size, digest.sha256)
    return receipt


def _upload(store: gcs.Client, link: FileLink, target: Target) -> gcs.ObjectDigest | None:
    """Stream the file in. On an expired link, fetch a fresh list once and go again."""
    name = target.object_name(link.name)
    try:
        return gcs.stream_url_to_object(
            store, http_client(), link.url, target.bucket, name, observer=GzipCheck()
        )
    except gcs.SourceError as exc:
        if exc.status_code != 403:
            raise
    get_run_logger().info("%s: link expired, fetching a fresh list", link.name)
    fresh = _fresh_link(target, link.name)
    return gcs.stream_url_to_object(
        store, http_client(), fresh.url, target.bucket, name, observer=GzipCheck()
    )


def _fresh_link(target: Target, file_name: str) -> FileLink:
    links = s2_client().dataset_files(target.release, target.dataset)
    for link in links:
        if link.name == file_name:
            return link
    raise S2Error(f"{file_name} is no longer listed for {target.dataset} {target.release}")


def _digest_stored(store: gcs.Client, link: FileLink, target: Target) -> gcs.ObjectDigest:
    """Hash and check an object an earlier run uploaded without getting to its receipt."""
    name = target.object_name(link.name)
    try:
        return gcs.digest_object(store, target.bucket, name, observer=GzipCheck())
    except GzipError as exc:
        raise GzipError(
            f"the stored copy of {name} in {target.bucket} is not a whole gzip stream; "
            "this account cannot replace it, so delete it with one that can, then rerun"
        ) from exc


@flow(name="s2-download")
def s2_download(release: str, dataset: str, bucket: str, prefix: str = DEFAULT_PREFIX) -> Summary:
    """Download every file of ``dataset`` in ``release`` into the bucket.

    ``latest`` is pinned to one release id here, once, so a run that crosses a weekly
    release still writes a single consistent set.
    """
    log = get_run_logger()
    s2 = s2_client()
    if release == LATEST:
        releases = s2.list_releases()
        if not releases:
            raise S2Error("the API lists no releases")
        release = releases[-1]
    target = Target(bucket=bucket, prefix=prefix, release=release, dataset=dataset)
    links = s2.dataset_files(release, dataset)
    log.info(
        "%s %s: %d %s to gs://%s/%s",
        dataset,
        release,
        len(links),
        "file" if len(links) == 1 else "files",
        bucket,
        target.object_name(""),
    )
    completed = sum(download_file(link, target) is not None for link in links)
    return Summary(
        release=release,
        dataset=dataset,
        files=len(links),
        completed=completed,
        skipped=len(links) - completed,
    )
