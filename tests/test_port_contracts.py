"""One behaviour suite for every ArtifactStore, so the fake cannot drift from the GCS adapter."""

from collections.abc import AsyncGenerator
from contextlib import aclosing

import pytest

from openknowledge_adapters.gcs import GcsConfig, connect
from openknowledge_contracts.manifests import ObjectPath, ObjectRef
from openknowledge_core.ingestion import ArtifactStore
from openknowledge_core.ingestion.errors import GzipInvalid, IncompleteTransfer, ObjectChanged
from tests.fakes.checksums import crc32c_b64, md5_b64
from tests.fakes.gcs_json_api import FakeGcs, running
from tests.fakes.ports import FakeStore, body_of, gz

PATH = ObjectPath(bucket="raw", name="s2/2026-10-06/papers/part-0.gz")
DATA = gz(bytes(range(256)) * 400)


@pytest.fixture(params=["fake", "gcs"])
async def store(request: pytest.FixtureRequest) -> AsyncGenerator[ArtifactStore]:
    if request.param == "fake":
        yield FakeStore()
        return
    async with running(FakeGcs()) as fake:
        async with connect(
            GcsConfig(request_timeout_seconds=30, min_transfer_rate=1024**2, api_root=fake.base_url)
        ) as gcs:
            yield gcs


async def read(store: ArtifactStore, ref: ObjectRef) -> bytes:
    stream = await store.open_read(ref)
    async with aclosing(stream.chunks) as chunks:
        return b"".join([chunk async for chunk in chunks])


async def test_create_is_create_only(store: ArtifactStore) -> None:
    first = await store.create(PATH, b"{}", content_type="application/json")
    assert first is not None
    assert await store.create(PATH, b"[]", content_type="application/json") is None
    assert await read(store, first.ref) == b"{}"


async def test_a_streamed_create_reports_size_checksums_and_generation(
    store: ArtifactStore,
) -> None:
    stored = await store.create_from_stream(PATH, body_of(DATA), content_type="application/gzip")
    assert stored is not None
    assert (stored.size, stored.md5, stored.crc32c) == (len(DATA), md5_b64(DATA), crc32c_b64(DATA))
    assert stored.ref.generation > 0
    assert await store.stat(PATH) == stored
    assert await read(store, stored.ref) == DATA


async def test_an_existing_object_is_left_alone(store: ArtifactStore) -> None:
    first = await store.create(PATH, b"{}", content_type="application/json")
    assert first is not None
    assert await store.create_from_stream(PATH, body_of(DATA), content_type="x") is None
    assert await read(store, first.ref) == b"{}"


async def test_an_error_from_the_body_propagates_unchanged(store: ArtifactStore) -> None:
    with pytest.raises(GzipInvalid, match="boom"):
        await store.create_from_stream(
            PATH, body_of(DATA, error=GzipInvalid("boom")), content_type="x"
        )
    assert await store.stat(PATH) is None


async def test_a_body_that_ends_unchecked_creates_nothing(store: ArtifactStore) -> None:
    with pytest.raises(IncompleteTransfer):
        await store.create_from_stream(PATH, body_of(DATA, complete=False), content_type="x")
    assert await store.stat(PATH) is None


async def test_reads_are_pinned_to_a_generation(store: ArtifactStore) -> None:
    stored = await store.create(PATH, b"{}", content_type="application/json")
    assert stored is not None
    other = ObjectRef(path=PATH, generation=stored.ref.generation + 1)
    with pytest.raises(ObjectChanged):
        await store.open_read(other)
