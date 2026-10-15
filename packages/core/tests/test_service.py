import hashlib
import logging

import pytest

from openknowledge_contracts.datasets import DiffInventory
from openknowledge_contracts.manifests import ShardReceipt
from openknowledge_core.ingestion import (
    DownloadDatasetService,
    DownloadShard,
    PinRelease,
    ShardOutcome,
)
from openknowledge_core.ingestion.errors import (
    GzipInvalid,
    IncompleteTransfer,
    InventoryChanged,
    LinkExpired,
    ShardNotListed,
    SourceRejected,
    SourceUnavailable,
    StoredCopyMismatch,
)
from tests.fakes.checksums import crc32c_b64, md5_b64
from tests.fakes.ports import (
    COMMAND,
    DIFF,
    NEXT,
    PAPERS,
    RELEASE,
    FakeSource,
    FakeStore,
    gz,
    shard_ref,
)


def add_papers(source: FakeSource) -> None:
    for name, data in PAPERS.items():
        source.add("papers", name, data)


async def test_every_shard_gets_an_object_and_a_receipt(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    outcomes = [await service.archive_shard(pinned, shard) for shard in pinned.shards]
    assert outcomes == [ShardOutcome.CREATED, ShardOutcome.CREATED]
    for shard, records in zip(pinned.shards, (1, 50), strict=True):
        data = PAPERS[shard.file_name]
        target = pinned.object_path(shard)
        assert store.data(target) == data
        assert store.content_types[str(target)] == "application/gzip"
        receipt = ShardReceipt.from_json(store.data(pinned.receipt_path(shard)))
        assert receipt.object_ref.generation == store.generation(target)
        assert receipt.sha256 == hashlib.sha256(data).hexdigest()
        assert (receipt.size, receipt.md5, receipt.crc32c) == (
            len(data),
            md5_b64(data),
            crc32c_b64(data),
        )
        assert (receipt.records, receipt.gzip_members) == (records, 1)
        assert receipt.schema_ref == "semantic-scholar/papers@2026-10-06"


async def test_latest_is_resolved_only_by_pin(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    pinned_release = await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert pinned_release.release_id == RELEASE
    assert pinned_release.record.name == "s2/2026-10-06/release.json"
    pinned = await service.pin_dataset(COMMAND)
    assert source.resolved == ["latest", RELEASE]
    assert all(RELEASE in pinned.object_path(shard).name for shard in pinned.shards)


async def test_a_rerun_downloads_nothing(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    opened = list(source.opened)
    again = [await service.archive_shard(pinned, shard) for shard in pinned.shards]
    assert again == [ShardOutcome.VERIFIED, ShardOutcome.VERIFIED]
    assert source.opened == opened


async def test_an_object_without_a_receipt_is_hashed_in_place(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    generation = store.put(pinned.object_path(shard), PAPERS[shard.file_name])
    assert await service.archive_shard(pinned, shard) == ShardOutcome.RECOVERED
    assert source.opened == ["part-0.gz"]  # opened only for its declared length, never read
    receipt = ShardReceipt.from_json(store.data(pinned.receipt_path(shard)))
    assert receipt.object_ref.generation == generation


async def test_a_broken_stored_copy_fails_with_removal_instructions(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    corrupt = bytearray(PAPERS[shard.file_name])
    corrupt[-6] ^= 0xFF  # same length as the source, so only the gzip check can catch it
    store.put(pinned.object_path(shard), bytes(corrupt))
    with pytest.raises(
        GzipInvalid,
        match=r"the stored copy (gs://raw/s2/2026-10-06/papers/part-0.gz) .* delete \1 with an",
    ):
        await service.archive_shard(pinned, shard)
    assert str(pinned.receipt_path(shard)) not in store.objects


async def test_a_whole_but_short_stored_copy_is_never_receipted(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    source.add("papers", "part-0.gz", gz(b"a\n") + gz(b"b\n"))
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    store.put(pinned.object_path(shard), gz(b"a\n"))  # what a store dropping a member would keep
    for _ in range(2):
        with pytest.raises(StoredCopyMismatch, match="but the source declares"):
            await service.archive_shard(pinned, shard)
    assert str(pinned.receipt_path(shard)) not in store.objects


async def test_recovery_without_a_declared_length_stops_without_removal_advice(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    store.put(pinned.object_path(shard), PAPERS[shard.file_name])
    source.declared["part-0.gz"] = None
    with pytest.raises(IncompleteTransfer, match="sent no length") as info:
        await service.archive_shard(pinned, shard)
    assert "delete" not in str(info.value)


async def test_a_stored_copy_unlike_the_stream_gets_no_receipt(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    store.md5_override[str(pinned.object_path(shard))] = "A" * 22 + "=="
    with pytest.raises(StoredCopyMismatch, match="passed the checks"):
        await service.archive_shard(pinned, shard)
    assert str(pinned.receipt_path(shard)) not in store.objects


@pytest.mark.parametrize("damage", ["truncate", "flip"])
async def test_bad_gzip_finalizes_nothing(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore, damage: str
) -> None:
    data = bytearray(PAPERS["part-1.gz"])
    if damage == "truncate":
        del data[-10:]
    else:
        data[len(data) // 2] ^= 0xFF
    source.add("papers", "part-0.gz", bytes(data))
    pinned = await service.pin_dataset(COMMAND)
    with pytest.raises(GzipInvalid):
        await service.archive_shard(pinned, pinned.shards[0])
    assert not any(key.endswith((".gz", ".receipt.json")) for key in store.objects)


@pytest.mark.parametrize(
    ("declared", "error", "message"),
    [
        (10_000, IncompleteTransfer, "part-0.gz: received .* were due; nothing was stored"),
        (None, IncompleteTransfer, "sent no length, so nothing was uploaded"),
        (0, GzipInvalid, "says the shard is empty"),
    ],
)
async def test_a_body_unlike_its_declared_length_finalizes_nothing(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    declared: int | None,
    error: type[Exception],
    message: str,
) -> None:
    add_papers(source)
    source.declared["part-0.gz"] = declared
    pinned = await service.pin_dataset(COMMAND)
    with pytest.raises(error, match=message):
        await service.archive_shard(pinned, pinned.shards[0])
    assert str(pinned.object_path(pinned.shards[0])) not in store.objects


async def test_links_come_from_one_listing_and_refresh_by_path_on_403(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    source.expired_listings = {1}
    source.reorder = True
    for shard in pinned.shards:
        assert await service.archive_shard(pinned, shard) == ShardOutcome.CREATED
        assert store.data(pinned.object_path(shard)) == PAPERS[shard.file_name]
    assert source.listings == 2
    assert source.opened == ["part-0.gz", "part-0.gz", "part-1.gz"]


async def test_a_fresh_link_refused_too_says_so(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    source.expired_listings = {1, 2}
    with pytest.raises(LinkExpired, match="part-0.gz: a freshly listed link was refused too"):
        await service.archive_shard(pinned, pinned.shards[0])


async def test_an_object_without_an_md5_is_named_as_composite(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    store.md5_override[str(pinned.object_path(pinned.shards[0]))] = None
    with pytest.raises(StoredCopyMismatch, match="has no MD5, which GCS omits for composite"):
        await service.archive_shard(pinned, pinned.shards[0])


async def test_a_refreshed_listing_with_other_shards_stops_the_run(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    source.expired_listings = {1}
    source.add("papers", "part-2.gz", gz(b"x\n"))
    with pytest.raises(InventoryChanged):
        await service.archive_shard(pinned, pinned.shards[0])


async def test_a_shard_outside_the_inventory_is_refused(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    with pytest.raises(ShardNotListed):
        await service.archive_shard(pinned, shard_ref("papers", "part-9.gz"))


async def test_an_object_another_writer_finalized_first_is_recovered(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    store.appears_during_upload[str(pinned.object_path(shard))] = PAPERS[shard.file_name]
    assert await service.archive_shard(pinned, shard) == ShardOutcome.RECOVERED
    receipt = ShardReceipt.from_json(store.data(pinned.receipt_path(shard)))
    assert receipt.object_ref.generation == store.generation(pinned.object_path(shard))


async def test_a_cut_transfer_is_transient_and_stores_nothing(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    source.cut.add("part-1.gz")
    pinned = await service.pin_dataset(COMMAND)
    with pytest.raises(SourceUnavailable):
        await service.archive_shard(pinned, pinned.shards[1])
    assert str(pinned.object_path(pinned.shards[1])) not in store.objects


async def test_download_shard_indexes_the_archived_inventory(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    source.reorder = True
    command = DownloadShard(release=RELEASE, dataset="papers", shard=1, bucket="raw", prefix="s2")
    result = await service.archive_shard_at(command)
    assert (result.shard.file_name, result.outcome) == ("part-1.gz", ShardOutcome.CREATED)
    with pytest.raises(ShardNotListed, match="numbered 0 to 1, so there is no shard 2"):
        await service.archive_shard_at(command.model_copy(update={"shard": 2}))


async def test_no_signed_link_reaches_the_bucket_or_a_log_line(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    source.expired_listings = {1}
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    await service.write_manifest(pinned)
    for stored in store.objects.values():
        assert b"Signature" not in stored.data and b"AWSAccessKeyId" not in stored.data
    assert caplog.records
    assert not any("Signature" in record.getMessage() for record in caplog.records)


async def test_a_diff_keeps_its_update_and_delete_files_apart(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    deleted = gz(b'{"corpusid": 2}\n{"corpusid": 3}\n')
    source.add_diff("papers", RELEASE, NEXT, {"u-0.gz": PAPERS["part-1.gz"]}, {"d-0.gz": deleted})
    source.expired_listings.add(1)
    pinned = await service.pin_diff(DIFF)
    receipts: list[ShardReceipt] = []
    for part in (pinned.updates, pinned.deletes):
        [shard] = part.shards
        assert await service.archive_shard(part, shard) is ShardOutcome.CREATED
        receipts.append(ShardReceipt.from_json(store.data(part.receipt_path(shard))))
        await service.write_manifest(part)
    assert [(receipt.records, receipt.schema_ref) for receipt in receipts] == [
        (50, "semantic-scholar/papers@2026-10-13"),
        (2, "semantic-scholar/papers@2026-10-13/deletes"),
    ]
    assert str(pinned.deletes.manifest_path) == (
        "gs://raw/s2/diffs/2026-10-06/2026-10-13/papers/deletes/manifest.json"
    )
    inventory = DiffInventory.from_json(store.data(pinned.updates.inventory_path))
    assert [shard.file_name for shard in inventory.deletes] == ["d-0.gz"]
    assert source.listings == 2  # the expired links were listed again once, for both parts


async def test_a_diff_spans_exactly_one_release(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    source.add_diff("papers", "2026-09-29", RELEASE, {"u-0.gz": PAPERS["part-0.gz"]}, {})
    source.add_diff("papers", RELEASE, NEXT, {"u-1.gz": PAPERS["part-0.gz"]}, {})
    with pytest.raises(SourceRejected, match="exactly one diff of papers from 2026-09-29"):
        await service.pin_diff(DIFF.model_copy(update={"from_release": "2026-09-29"}))
