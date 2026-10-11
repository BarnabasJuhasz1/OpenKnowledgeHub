"""A receipt counts only when it and the object it names agree on every field."""

import json

import pytest

from openknowledge_contracts.datasets import ShardRef
from openknowledge_core.ingestion import DownloadDatasetService, PinnedDataset, ShardOutcome
from openknowledge_core.ingestion.errors import ReceiptInvalid
from openknowledge_core.ingestion.service import RECEIPT_LIMIT
from tests.fakes.ports import COMMAND, PAPERS, FakeSource, FakeStore


async def archived(
    service: DownloadDatasetService, source: FakeSource
) -> tuple[PinnedDataset, ShardRef]:
    for name, data in PAPERS.items():
        source.add("papers", name, data)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    await service.archive_shard(pinned, shard)
    source.opened.clear()
    return pinned, shard


def rewrite(store: FakeStore, pinned: PinnedDataset, shard: ShardRef, /, **changes: object) -> None:
    stored = json.loads(store.data(pinned.receipt_path(shard)))
    store.put(pinned.receipt_path(shard), json.dumps({**stored, **changes}).encode())


async def test_a_valid_receipt_skips_after_one_small_read(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    store.reads.clear()
    assert await service.archive_shard(pinned, shard) == ShardOutcome.VERIFIED
    assert store.reads == [str(pinned.receipt_path(shard))]
    assert (source.opened, source.listings) == ([], 1)


@pytest.mark.parametrize(
    "data", [b"not json", b"\xff\xfe", b"{}", b'{"schema_version": 1}', b"[1, 2]"]
)
async def test_a_receipt_that_does_not_parse_fails(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore, data: bytes
) -> None:
    pinned, shard = await archived(service, source)
    store.put(pinned.receipt_path(shard), data)
    with pytest.raises(ReceiptInvalid, match="is not a valid receipt"):
        await service.archive_shard(pinned, shard)
    assert source.opened == []


@pytest.mark.parametrize(
    ("changes", "named"),
    [
        ({"release_id": "2026-09-29"}, "release"),
        ({"dataset": "abstracts"}, "dataset"),
        (
            {
                "shard": {
                    "source_path": "/staging/2026-10-06/papers/part-1.gz",
                    "file_name": "part-1.gz",
                }
            },
            "shard",
        ),
        (
            {
                "object_ref": {
                    "path": {"bucket": "raw", "name": "s2/2026-10-06/papers/part-1.gz"},
                    "generation": 1,
                }
            },
            "object",
        ),
    ],
)
async def test_a_receipt_naming_something_else_fails(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    changes: dict[str, object],
    named: str,
) -> None:
    pinned, shard = await archived(service, source)
    rewrite(store, pinned, shard, **changes)
    with pytest.raises(ReceiptInvalid, match=f"is for another {named};"):
        await service.archive_shard(pinned, shard)


async def test_a_receipt_whose_object_is_gone_fails(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    store.remove(pinned.object_path(shard))
    with pytest.raises(ReceiptInvalid, match="which does not exist"):
        await service.archive_shard(pinned, shard)
    assert source.opened == []


async def test_a_receipt_whose_object_was_replaced_fails(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    store.put(pinned.object_path(shard), PAPERS[shard.file_name])
    with pytest.raises(ReceiptInvalid, match="disagree on generation;"):
        await service.archive_shard(pinned, shard)


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"size": 999}, "size"),
        ({"md5": "A" * 22 + "=="}, "MD5"),
        ({"crc32c": "AAAAAA=="}, "CRC32C"),
    ],
)
async def test_a_receipt_whose_length_or_checksum_disagrees_fails(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    changes: dict[str, object],
    field: str,
) -> None:
    pinned, shard = await archived(service, source)
    rewrite(store, pinned, shard, **changes)
    with pytest.raises(ReceiptInvalid, match=f"disagree on {field};"):
        await service.archive_shard(pinned, shard)


async def test_an_object_gcs_now_reports_other_checksums_for_fails(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    store.crc32c_override[str(pinned.object_path(shard))] = "AAAAAA=="
    with pytest.raises(ReceiptInvalid, match="disagree on CRC32C;"):
        await service.archive_shard(pinned, shard)


async def test_a_receipt_too_large_to_be_one_fails(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    store.put(pinned.receipt_path(shard), b" " * (RECEIPT_LIMIT + 1))
    with pytest.raises(ReceiptInvalid, match="larger than a receipt can be"):
        await service.archive_shard(pinned, shard)


async def test_a_concurrent_receipt_that_disagrees_fails(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    for name, data in PAPERS.items():
        source.add("papers", name, data)
    pinned = await service.pin_dataset(COMMAND)
    shard = pinned.shards[0]
    store.appears_before_create[str(pinned.receipt_path(shard))] = b'{"not": "a receipt"}'
    with pytest.raises(ReceiptInvalid, match="is not a valid receipt"):
        await service.archive_shard(pinned, shard)


async def test_a_concurrent_receipt_that_agrees_is_accepted(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    theirs = store.data(pinned.receipt_path(shard))
    store.remove(pinned.receipt_path(shard))
    store.appears_before_create[str(pinned.receipt_path(shard))] = theirs
    assert await service.archive_shard(pinned, shard) == ShardOutcome.RECOVERED
    assert store.data(pinned.receipt_path(shard)) == theirs


async def test_a_deeply_nested_receipt_fails_as_a_receipt(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    store.put(pinned.receipt_path(shard), b"[" * 5000 + b"]" * 5000)
    with pytest.raises(ReceiptInvalid, match="nests too deeply"):
        await service.archive_shard(pinned, shard)


async def test_a_receipt_with_unknown_keys_is_refused_without_quoting_them(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    pinned, shard = await archived(service, source)
    rewrite(store, pinned, shard, **{"PRIVATE hunter2\nforged log line": 1})
    with pytest.raises(ReceiptInvalid, match="unexpected fields") as info:
        await service.archive_shard(pinned, shard)
    assert "hunter2" not in str(info.value) and "forged" not in str(info.value)
