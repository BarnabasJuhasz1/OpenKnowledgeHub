"""The s2-download flow end to end against the fake API and the fake bucket."""

from __future__ import annotations

import hashlib
import inspect
import json
import zlib
from datetime import datetime
from types import ModuleType

import pytest

from openknowledge_adapters.semantic_scholar import S2Error
from openknowledge_pipelines.flows import s2_download as flow_module
from openknowledge_pipelines.flows.s2_download import GzipCheck, GzipError, Target
from tests.conftest import FakeS2, FakeStorage, gz

PAPERS = {"part-0.gz": gz(b'{"id": 1}\n'), "part-1.gz": gz(b'{"id": 2}\n' * 50)}


def run(pipeline: ModuleType, release: str = "latest") -> flow_module.Summary:
    return pipeline.s2_download(release=release, dataset="papers", bucket="raw", prefix="s2")


def receipt(storage: FakeStorage, name: str) -> dict:
    return json.loads(storage.bucket("raw").objects[f"s2/2026-10-06/papers/{name}.receipt.json"])


def test_downloads_every_file_and_writes_receipts(
    pipeline: ModuleType,
    s2: FakeS2,
    storage: FakeStorage,
) -> None:
    for name, data in PAPERS.items():
        s2.add("papers", name, data)
    summary = run(pipeline)
    assert summary == flow_module.Summary("2026-10-06", "papers", files=2, completed=2, skipped=0)
    objects = storage.bucket("raw").objects
    for name, data in PAPERS.items():
        assert objects[f"s2/2026-10-06/papers/{name}"] == data
        assert receipt(storage, name) == {
            "release": "2026-10-06",
            "dataset": "papers",
            "file": name,
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
            "completed_at": receipt(storage, name)["completed_at"],
        }
        assert datetime.fromisoformat(receipt(storage, name)["completed_at"]).tzinfo is not None
    assert storage.bucket("raw").content_types["s2/2026-10-06/papers/part-0.gz.receipt.json"] == (
        "application/json"
    )


def test_a_rerun_skips_files_with_receipts(
    pipeline: ModuleType, s2: FakeS2, storage: FakeStorage
) -> None:
    s2.add("papers", "part-0.gz", PAPERS["part-0.gz"])
    run(pipeline)
    downloads = len(s2.file_calls)
    summary = run(pipeline, release="2026-10-06")
    assert (summary.completed, summary.skipped) == (0, 1)
    assert len(s2.file_calls) == downloads


def test_an_object_without_a_receipt_is_checked_in_place(
    pipeline: ModuleType,
    s2: FakeS2,
    storage: FakeStorage,
) -> None:
    data = PAPERS["part-0.gz"]
    s2.add("papers", "part-0.gz", data)
    storage.bucket("raw").objects["s2/2026-10-06/papers/part-0.gz"] = data
    summary = run(pipeline)
    assert summary.completed == 1
    assert s2.file_calls == []
    assert receipt(storage, "part-0.gz")["sha256"] == hashlib.sha256(data).hexdigest()


def test_an_object_without_a_receipt_that_is_broken_fails_the_run(
    pipeline: ModuleType,
    s2: FakeS2,
    storage: FakeStorage,
) -> None:
    s2.add("papers", "part-0.gz", PAPERS["part-0.gz"])
    storage.bucket("raw").objects["s2/2026-10-06/papers/part-0.gz"] = PAPERS["part-0.gz"][:-4]
    with pytest.raises(
        GzipError, match="stored copy of s2/2026-10-06/papers/part-0.gz .* delete it"
    ):
        run(pipeline)
    assert "s2/2026-10-06/papers/part-0.gz.receipt.json" not in storage.bucket("raw").objects


def test_an_expired_link_refreshes_the_list(
    pipeline: ModuleType, s2: FakeS2, storage: FakeStorage
) -> None:
    s2.add("papers", "part-0.gz", PAPERS["part-0.gz"])
    s2.expire_after_first_list = True
    summary = run(pipeline)
    assert summary.completed == 1
    assert s2.listings() == 2
    assert s2.file_calls == ["part-0.gz", "part-0.gz"]
    assert storage.bucket("raw").objects["s2/2026-10-06/papers/part-0.gz"] == PAPERS["part-0.gz"]


def test_a_truncated_gzip_leaves_no_object_and_no_receipt(
    pipeline: ModuleType,
    s2: FakeS2,
    storage: FakeStorage,
) -> None:
    s2.add("papers", "part-0.gz", PAPERS["part-1.gz"][:-10])
    with pytest.raises(GzipError):
        run(pipeline)
    assert storage.bucket("raw").objects == {}
    assert s2.file_calls == ["part-0.gz"]  # bad data is not retried


def test_a_connection_cut_mid_file_is_retried(
    pipeline: ModuleType, s2: FakeS2, storage: FakeStorage
) -> None:
    s2.add("papers", "part-0.gz", PAPERS["part-1.gz"])
    s2.cut_next.add("part-0.gz")
    summary = run(pipeline)
    assert summary.completed == 1
    assert s2.file_calls == ["part-0.gz", "part-0.gz"]
    assert storage.bucket("raw").objects["s2/2026-10-06/papers/part-0.gz"] == PAPERS["part-1.gz"]


def test_no_releases_is_an_error(pipeline: ModuleType, s2: FakeS2) -> None:
    s2.releases = []
    with pytest.raises(S2Error, match="no releases"):
        run(pipeline)


def test_a_corrupt_gzip_leaves_no_object_and_no_receipt(
    pipeline: ModuleType,
    s2: FakeS2,
    storage: FakeStorage,
) -> None:
    data = bytearray(PAPERS["part-1.gz"])
    data[len(data) // 2] ^= 0xFF
    s2.add("papers", "part-0.gz", bytes(data))
    with pytest.raises(GzipError):
        run(pipeline)
    assert storage.bucket("raw").objects == {}


def test_target_paths() -> None:
    target = Target(bucket="raw", prefix="/s2/", release="2026-10-06", dataset="papers")
    assert target.object_name("a.gz") == "s2/2026-10-06/papers/a.gz"
    assert target.receipt_name("a.gz") == "s2/2026-10-06/papers/a.gz.receipt.json"
    assert Target("raw", "", "r", "d").object_name("a.gz") == "r/d/a.gz"


@pytest.mark.parametrize(
    "data",
    [
        gz(b"a" * 5000) + gz(b"b" * 5000),
        gz(b"a" * 5000) + b"\x00" * 13 + gz(b"b" * 5000) + b"\x00" * 7,
    ],
)
def test_gzip_check_accepts_whole_streams_in_any_chunking(data: bytes) -> None:
    for step in (1, 7, 4096, len(data)):
        check = GzipCheck()
        for start in range(0, len(data), step):
            check.update(data[start : start + step])
        check.close()
        assert check.members == 2


@pytest.mark.parametrize(
    "data",
    [
        b"",
        gz(b"abc")[:-1],
        gz(b"abc")[:5],
        b"not gzip at all",
        gz(b"abc") + b"trailing junk",
        b"\x00" + gz(b"abc"),
    ],
)
def test_gzip_check_rejects_incomplete_streams(data: bytes) -> None:
    check = GzipCheck()
    with pytest.raises(GzipError):
        check.update(data)
        check.close()


def test_gzip_check_bounds_its_output_per_step() -> None:
    check = GzipCheck()
    check.update(
        gz(b"\x00" * (40 * 1024 * 1024))
    )  # 40 MiB out of one small update, over the 8 MiB cap
    check.close()
    assert check.members == 1


def test_gzip_check_rejects_a_bad_crc() -> None:
    data = bytearray(gz(b"abc" * 100))
    data[-8] ^= 0xFF  # first byte of the CRC-32 trailer
    with pytest.raises(GzipError, match="does not decode"):
        GzipCheck().update(bytes(data))
    with pytest.raises(
        zlib.error
    ):  # the same bytes fail plain zlib too, so the check adds no leniency
        zlib.decompress(bytes(data), 16 + zlib.MAX_WBITS)


def test_the_flow_has_no_cloud_specific_code() -> None:
    assert "google" not in inspect.getsource(flow_module)
