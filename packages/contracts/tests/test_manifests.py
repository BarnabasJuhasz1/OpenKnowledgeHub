import json
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from openknowledge_contracts.datasets import ShardRef
from openknowledge_contracts.manifests import (
    BucketName,
    DatasetManifest,
    ManifestEntry,
    ObjectPath,
    ObjectPrefix,
    ObjectRef,
    Producer,
    ShardReceipt,
)

NOW = datetime(2026, 10, 10, tzinfo=UTC)
PATH = ObjectPath(bucket="raw", name="semantic-scholar/2026-10-06/papers/part-0.gz")
PRODUCER = Producer(
    transform="verbatim", transform_version=1, software_version="0.1.0", image_digest=None
)


def receipt_fields(**changes: object) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "schema_version": 1,
        "source": "semantic-scholar",
        "release_id": "2026-10-06",
        "dataset": "papers",
        "shard": ShardRef(
            source_path="/staging/2026-10-06/papers/part-0.gz", file_name="part-0.gz"
        ),
        "object_ref": ObjectRef(path=PATH, generation=7),
        "size": 10,
        "sha256": "a" * 64,
        "md5": "1B2M2Y8AsgTpgAmY7PhCfg==",
        "crc32c": "AAAAAA==",
        "schema_ref": "semantic-scholar/papers@2026-10-06",
        "gzip_members": 1,
        "records": 3,
        "uncompressed_size": 30,
        "producer": PRODUCER,
        "completed_at": NOW,
    }
    return {**fields, **changes}


def test_receipt_round_trip() -> None:
    receipt = ShardReceipt(**receipt_fields())
    assert ShardReceipt.from_json(receipt.to_json()) == receipt


def test_receipt_rejects_unknown_fields_and_missing_ones() -> None:
    stored = json.loads(ShardReceipt(**receipt_fields()).to_json())
    with pytest.raises(ValidationError):
        ShardReceipt.from_json(json.dumps({**stored, "note": "x"}).encode())
    del stored["object_ref"]
    with pytest.raises(ValidationError):
        ShardReceipt.from_json(json.dumps(stored).encode())


@pytest.mark.parametrize(
    "changes",
    [
        {"sha256": "A" * 64},
        {"sha256": "a" * 63},
        {"md5": "not-base64"},
        {"crc32c": "AAAAAAAA"},
        {"object_ref": {"path": PATH, "generation": 0}},
        {"gzip_members": 0},
        {"size": -1},
        {"schema_version": 2},
    ],
)
def test_receipt_field_formats(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ShardReceipt(**receipt_fields(**changes))


@pytest.mark.parametrize("name", ["", "a//b", "a/./b", "a/../b", "a/b\n", "x" * 1025])
def test_object_names(name: str) -> None:
    with pytest.raises(ValidationError):
        ObjectPath(bucket="raw", name=name)


@pytest.mark.parametrize(("raw", "kept"), [("/s2/", "s2"), ("", ""), ("a/b", "a/b")])
def test_prefixes_drop_outer_slashes(raw: str, kept: str) -> None:
    assert TypeAdapter(ObjectPrefix).validate_python(raw) == kept


def test_paths_under_a_prefix() -> None:
    assert str(ObjectPath.under("raw", "s2", "r", "d", "a.gz")) == "gs://raw/s2/r/d/a.gz"
    assert ObjectPath.under("raw", "", "r", "d", "a.gz").name == "r/d/a.gz"


@pytest.mark.parametrize("bucket", ["Raw", "ab", "-raw", "raw/x"])
def test_bucket_names(bucket: str) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(BucketName).validate_python(bucket)


def test_manifest_needs_one_entry_per_object() -> None:
    entry = ManifestEntry(
        object_ref=ObjectRef(path=PATH, generation=7),
        size=10,
        sha256="a" * 64,
        md5="1B2M2Y8AsgTpgAmY7PhCfg==",
        crc32c="AAAAAA==",
        records=3,
    )
    manifest = DatasetManifest(
        schema_version=1,
        release_id="2026-10-06",
        dataset="papers",
        entries=(entry,),
        producer=PRODUCER,
        completed_at=NOW,
    )
    assert DatasetManifest.from_json(manifest.to_json()) == manifest
    with pytest.raises(ValidationError):
        manifest.model_validate({**manifest.model_dump(), "entries": (entry, entry)})


def test_producer_image_digest_format() -> None:
    with pytest.raises(ValidationError):
        Producer(
            transform="verbatim", transform_version=1, software_version="1", image_digest="latest"
        )
