"""release.json, dataset.json and manifest.json: written once, read back by every rerun."""

import hashlib
import json

import pytest

from openknowledge_contracts.manifests import DatasetManifest
from openknowledge_core.ingestion import DownloadDatasetService, PinRelease
from openknowledge_core.ingestion import service as service_module
from openknowledge_core.ingestion.errors import ArchiveConflict, ReceiptInvalid, SourceRejected
from openknowledge_core.ingestion.models import release_record_path
from openknowledge_core.ingestion.service import SERVED_NOW
from tests.fakes.ports import COMMAND, PAPERS, PRODUCER, RELEASE, FakeSource, FakeStore, clock, gz

RELEASE_JSON = "gs://raw/s2/2026-10-06/release.json"
DATASET_JSON = "gs://raw/s2/2026-10-06/papers/dataset.json"


def add_papers(source: FakeSource) -> None:
    for name, data in PAPERS.items():
        source.add("papers", name, data)


async def test_release_and_inventory_land_before_any_shard(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    assert sorted(store.objects) == [DATASET_JSON, RELEASE_JSON]


async def test_archived_documents_keep_readmes_and_unknown_fields(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    release = json.loads(store.objects[RELEASE_JSON].data)["metadata"]
    assert release["README"] == source.readme
    assert release["license_notes"] == "ODC-BY for most datasets"
    assert release["datasets"][0]["README"] == "About papers"
    inventory = json.loads(store.objects[DATASET_JSON].data)
    assert inventory["dataset"]["citation"] == "Kinney et al. 2023"
    assert [shard["file_name"] for shard in inventory["shards"]] == ["part-0.gz", "part-1.gz"]
    for stored in store.objects.values():
        assert b"Signature" not in stored.data and b"files.test" not in stored.data


async def test_a_rerun_reads_the_archive_back_and_keeps_its_order(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    first = await service.pin_dataset(COMMAND)
    source.reorder = True
    rerun = DownloadDatasetService(
        catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
    )
    assert (await rerun.pin_dataset(COMMAND)).shards == first.shards


async def test_an_inventory_unlike_the_listing_conflicts(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    source.add("papers", "part-2.gz", gz(b"x\n"))
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetInventory with a different set of shards than .* {SERVED_NOW}",
    ):
        await service.pin_dataset(COMMAND)


async def test_an_inventory_of_another_release_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    path = (await service.pin_dataset(COMMAND)).inventory_path
    stored = json.loads(store.data(path))
    stored["release_id"] = "2026-09-29"
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetInventory with a different release id than .* {SERVED_NOW}",
    ):
        await service.pin_dataset(COMMAND)


async def test_an_inventory_of_another_dataset_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    path = (await service.pin_dataset(COMMAND)).inventory_path
    stored = json.loads(store.data(path))
    stored["dataset"]["name"] = "authors"
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetInventory with a different dataset than .* {SERVED_NOW}",
    ):
        await service.pin_dataset(COMMAND)


async def test_a_release_record_of_another_release_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    await service.pin_release(PinRelease(release=RELEASE, bucket="raw", prefix="s2"))
    path = release_record_path("raw", "s2", RELEASE)
    stored = json.loads(store.data(path))
    stored["metadata"]["release_id"] = "2026-09-29"
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict, match=f"a ReleaseRecord with a different release id than .* {SERVED_NOW}"
    ):
        await service.pin_dataset(COMMAND)


async def test_a_corrupt_document_conflicts_without_echoing_it(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    store.put(release_record_path("raw", "s2", RELEASE), b"topsecret garbage")
    with pytest.raises(
        ArchiveConflict,
        match=r"not a valid ReleaseRecord \(the document could not be parsed as JSON",
    ) as info:
        await service.pin_dataset(COMMAND)
    assert "topsecret" not in str(info.value)


async def test_a_signing_parameter_upstream_is_refused_and_nothing_written(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    source.readme = "Get it at https://x.test/a?X-Amz-Signature=topsecret"
    with pytest.raises(SourceRejected, match=r"\$\.metadata\.README") as info:
        await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert "topsecret" not in str(info.value)
    assert store.objects == {}


async def test_ordinary_readme_links_archive(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    source.readme = "Docs at https://example.org/datasets?page=2&lang=en"
    await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert RELEASE_JSON in store.objects


async def test_the_manifest_waits_for_every_receipt_and_lists_exact_objects(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    await service.archive_shard(pinned, pinned.shards[0])
    with pytest.raises(ReceiptInvalid, match="is missing"):
        await service.write_manifest(pinned)
    await service.archive_shard(pinned, pinned.shards[1])
    path = await service.write_manifest(pinned)
    manifest = DatasetManifest.from_json(store.data(path))
    assert [entry.object_ref.path for entry in manifest.entries] == [
        pinned.object_path(shard) for shard in pinned.shards
    ]
    assert [entry.object_ref.generation for entry in manifest.entries] == [
        store.generation(pinned.object_path(shard)) for shard in pinned.shards
    ]
    assert [entry.sha256 for entry in manifest.entries] == [
        hashlib.sha256(PAPERS[shard.file_name]).hexdigest() for shard in pinned.shards
    ]
    assert await service.write_manifest(pinned) == path


async def test_a_manifest_that_disagrees_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    path = await service.write_manifest(pinned)
    stored = json.loads(store.data(path))
    stored["entries"][0]["object_ref"]["generation"] += 1
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match="a DatasetManifest with a different entry list than .* the receipts beside it",
    ):
        await service.write_manifest(pinned)


@pytest.mark.parametrize(
    ("field", "value", "named"),
    [("release_id", "2026-09-29", "release id"), ("dataset", "authors", "dataset")],
)
async def test_a_manifest_of_another_release_or_dataset_conflicts(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    field: str,
    value: str,
    named: str,
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    path = await service.write_manifest(pinned)
    stored = json.loads(store.data(path))
    stored[field] = value
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetManifest with a different {named} than .* the receipts beside it",
    ):
        await service.write_manifest(pinned)


async def test_a_document_too_large_to_read_back_is_never_written(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    add_papers(source)
    monkeypatch.setattr(service_module, "DOCUMENT_LIMIT", 100)
    with pytest.raises(SourceRejected, match="more than a rerun reads back"):
        await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert store.objects == {}
