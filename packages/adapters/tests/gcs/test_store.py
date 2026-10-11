"""The GCS adapter on the real gcloud-aio-storage and aiohttp, against the loopback fake API."""

import asyncio
import pickle
import random
from collections.abc import AsyncGenerator
from contextlib import aclosing

import pytest

from openknowledge_adapters.gcs import GcsArtifactStore, GcsConfig, connect
from openknowledge_adapters.gcs.store import transfer_timeout
from openknowledge_contracts.manifests import ObjectPath, ObjectRef
from openknowledge_core.ingestion.errors import (
    GzipInvalid,
    IncompleteTransfer,
    ObjectChanged,
    StoreRejected,
    StoreUnavailable,
)
from openknowledge_core.ingestion.models import CheckedStream
from tests.fakes.checksums import crc32c_b64, md5_b64
from tests.fakes.gcs_json_api import FakeGcs
from tests.fakes.ports import body_of, gz

PATH = ObjectPath(bucket="raw", name="s2/2026-10-06/papers/part-0.gz")
DATA = gz(random.Random(0).randbytes(1_000_000))  # several of aiohttp's 256 KiB reads
KEY = ("raw", PATH.name)
GZIP = "application/gzip"


async def test_a_small_create_is_create_only(store: GcsArtifactStore, gcs: FakeGcs) -> None:
    created = await store.create(PATH, b"{}", content_type="application/json")
    assert created is not None
    assert (created.size, created.md5, created.crc32c) == (2, md5_b64(b"{}"), crc32c_b64(b"{}"))
    assert await store.create(PATH, b"[]", content_type="application/json") is None
    assert gcs.data(*KEY) == b"{}"
    assert gcs.objects[KEY].content_type == "application/json"


async def test_a_streamed_create_reports_what_gcs_stored(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    stored = await store.create_from_stream(PATH, body_of(DATA), content_type=GZIP)
    assert stored is not None
    assert gcs.data(*KEY) == DATA
    assert (stored.size, stored.md5, stored.crc32c) == (len(DATA), md5_b64(DATA), crc32c_b64(DATA))
    assert stored.ref.generation == gcs.objects[KEY].generation
    assert gcs.received == [len(DATA)]


async def test_an_existing_object_is_left_alone_and_the_body_closed(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    gcs.put_object(*KEY, b"theirs")
    body = body_of(DATA)
    assert await store.create_from_stream(PATH, body, content_type=GZIP) is None
    assert gcs.data(*KEY) == b"theirs"
    with pytest.raises(StopAsyncIteration):
        await anext(body.chunks)


async def test_a_412_on_the_final_request_means_the_object_exists(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    def another_writer() -> None:
        gcs.put_object(*KEY, b"theirs")

    gcs.before_put = another_writer
    assert await store.create_from_stream(PATH, body_of(DATA), content_type=GZIP) is None
    assert gcs.data(*KEY) == b"theirs"


async def test_a_failing_body_raises_its_own_error_and_creates_nothing(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    with pytest.raises(GzipInvalid, match="boom"):
        await store.create_from_stream(
            PATH, body_of(DATA, error=GzipInvalid("boom")), content_type=GZIP
        )
    assert KEY not in gcs.objects
    assert all(received < len(DATA) for received in gcs.received)


async def test_a_body_that_ends_unchecked_creates_nothing(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    with pytest.raises(IncompleteTransfer):
        await store.create_from_stream(PATH, body_of(DATA, complete=False), content_type=GZIP)
    assert KEY not in gcs.objects


async def test_a_cancelled_upload_creates_nothing(store: GcsArtifactStore, gcs: FakeGcs) -> None:
    started, never = asyncio.Event(), asyncio.Event()

    async def chunks() -> AsyncGenerator[bytes, None]:
        yield DATA[: len(DATA) // 2]
        started.set()
        await never.wait()
        yield DATA[len(DATA) // 2 :]

    upload = asyncio.create_task(
        store.create_from_stream(
            PATH, CheckedStream(size=len(DATA), chunks=chunks()), content_type=GZIP
        )
    )
    await started.wait()
    await asyncio.sleep(0.05)
    upload.cancel()
    with pytest.raises(asyncio.CancelledError):
        await upload
    await asyncio.sleep(0.05)
    assert KEY not in gcs.objects
    assert all(received < len(DATA) for received in gcs.received)


async def test_the_event_loop_keeps_running_during_an_upload(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.001)

    beat = asyncio.create_task(heartbeat())
    try:
        assert await store.create_from_stream(PATH, body_of(DATA), content_type=GZIP)
    finally:
        beat.cancel()
    assert ticks > 0


@pytest.mark.parametrize(("status", "error"), [(503, StoreUnavailable), (403, StoreRejected)])
async def test_a_failed_upload_names_no_upload_session(
    store: GcsArtifactStore, gcs: FakeGcs, status: int, error: type[Exception]
) -> None:
    gcs.put_statuses = [status]
    with pytest.raises(error) as info:
        await store.create_from_stream(PATH, body_of(DATA), content_type=GZIP)
    text = f"{info.value} {info.value!r} {pickle.loads(pickle.dumps(info.value))}"
    assert "SESSION" not in text and "upload_id" not in text
    assert info.value.__context__ is None and info.value.__cause__ is None
    assert KEY not in gcs.objects


@pytest.mark.parametrize(
    ("status", "advice"),
    [
        (401, "gcloud auth application-default login"),
        (403, "give this account access to create and read objects in raw"),
        (404, "GCS has no bucket raw; check --bucket"),
    ],
)
async def test_a_refused_upload_says_what_to_fix(
    store: GcsArtifactStore,
    gcs: FakeGcs,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    advice: str,
) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    gcs.put_statuses = [status]
    with pytest.raises(StoreRejected, match=advice):
        await store.create_from_stream(PATH, body_of(DATA), content_type=GZIP)


async def test_stat_and_an_exact_read(store: GcsArtifactStore, gcs: FakeGcs) -> None:
    assert await store.stat(PATH) is None
    generation = gcs.put_object(*KEY, DATA)
    stored = await store.stat(PATH)
    assert stored is not None
    assert (stored.ref.generation, stored.size, stored.md5) == (
        generation,
        len(DATA),
        md5_b64(DATA),
    )
    stream = await store.open_read(stored.ref)
    async with aclosing(stream.chunks) as chunks:
        assert b"".join([chunk async for chunk in chunks]) == DATA
    assert {key.lower(): value for key, value in gcs.media_headers[-1].items()}[
        "accept-encoding"
    ] == "identity"


async def test_a_malformed_answer_is_a_store_error_that_quotes_nothing(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    gcs.put_object(*KEY, DATA)
    gcs.metadata_reply = b"<html>topsecret</html>"
    with pytest.raises(StoreUnavailable, match="JSONDecodeError") as info:
        await store.stat(PATH)
    assert "topsecret" not in str(info.value)
    assert info.value.__context__ is None


async def test_a_read_of_another_generation_is_refused(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    generation = gcs.put_object(*KEY, DATA)
    with pytest.raises(ObjectChanged):
        await store.open_read(ObjectRef(path=PATH, generation=generation + 1))


async def test_a_generation_change_during_a_read_is_caught(
    store: GcsArtifactStore, gcs: FakeGcs
) -> None:
    generation = gcs.put_object(*KEY, DATA)

    def replace() -> None:
        gcs.put_object(*KEY, b"replaced")

    gcs.after_media = replace
    stream = await store.open_read(ObjectRef(path=PATH, generation=generation))
    with pytest.raises(ObjectChanged, match="while it was read"):
        async with aclosing(stream.chunks) as chunks:
            async for _ in chunks:
                pass


@pytest.mark.parametrize(
    ("size", "seconds"), [(0, 60), (1, 61), (256 * 1024, 61), (1024**3, 60 + 4096)]
)
def test_transfer_timeouts_grow_with_the_object(size: int, seconds: int) -> None:
    assert transfer_timeout(size, 60, 256 * 1024) == seconds


async def test_a_stalled_read_times_out_per_read_not_per_transfer(gcs: FakeGcs) -> None:
    generation = gcs.put_object(*KEY, DATA)
    gcs.media_stall_seconds = 5.0
    config = GcsConfig(request_timeout_seconds=1, min_transfer_rate=1, api_root=gcs.base_url)
    async with connect(config) as slow:  # the whole-transfer limit is len(DATA) seconds
        stream = await slow.open_read(ObjectRef(path=PATH, generation=generation))
        started = asyncio.get_running_loop().time()
        with pytest.raises(StoreUnavailable, match="TimeoutError"):
            async with aclosing(stream.chunks) as chunks:
                async for _ in chunks:
                    pass
        assert asyncio.get_running_loop().time() - started < 4
