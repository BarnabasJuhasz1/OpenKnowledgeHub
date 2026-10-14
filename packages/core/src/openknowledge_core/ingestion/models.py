"""Values that are never written to storage."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    NonNegativeInt,
    PositiveInt,
    SecretStr,
)

from openknowledge_contracts.datasets import (
    INVENTORY_NAME,
    MANIFEST_NAME,
    RECEIPT_SUFFIX,
    RELEASE_RECORD_NAME,
    DatasetName,
    DatasetSummary,
    ReleaseId,
    ShardRef,
)
from openknowledge_contracts.manifests import (
    BucketName,
    Crc32cBase64,
    DatasetManifest,
    ImageDigest,
    Md5Base64,
    ObjectPath,
    ObjectPrefix,
    ObjectRef,
    Sha256Hex,
)


def _sendable_as_a_header(key: SecretStr) -> SecretStr:
    # A header value must be ASCII, and an HTTP client's error for another character holds it.
    if not re.fullmatch(r"[!-~]+", key.get_secret_value()):
        raise ValueError("must be printable ASCII without spaces")
    return key


ApiKey = Annotated[SecretStr, AfterValidator(_sendable_as_a_header)]


class ShardLink(BaseModel):
    """A signed link to one shard, whose URL is a credential."""

    model_config = ConfigDict(frozen=True)

    shard: ShardRef
    url: SecretStr


class DatasetListing(BaseModel):
    model_config = ConfigDict(frozen=True)

    summary: DatasetSummary
    links: tuple[ShardLink, ...]


class ByteStream(BaseModel):
    """A body being read, where closing ``chunks`` closes the response behind it."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    expected_size: NonNegativeInt | None
    chunks: AsyncGenerator[bytes, None]


class CheckedStream(BaseModel):
    """A store finalizes this body only after ``chunks`` ends with ``completed`` set."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    size: NonNegativeInt
    chunks: AsyncGenerator[bytes, None]
    completed: bool = False


class StoredObject(BaseModel):
    model_config = ConfigDict(frozen=True)

    ref: ObjectRef
    size: NonNegativeInt
    md5: Md5Base64 | None  # GCS reports no MD5 for composite objects
    crc32c: Crc32cBase64


class PinnedDataset(BaseModel):
    """One dataset of one release, with the shards of its archived inventory."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    release_id: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix
    shards: tuple[ShardRef, ...]

    def path(self, file_name: str) -> ObjectPath:
        return dataset_file_path(self.bucket, self.prefix, self.release_id, self.dataset, file_name)

    def object_path(self, shard: ShardRef) -> ObjectPath:
        return self.path(shard.file_name)

    def receipt_path(self, shard: ShardRef) -> ObjectPath:
        return self.path(shard.file_name + RECEIPT_SUFFIX)

    @property
    def inventory_path(self) -> ObjectPath:
        return self.path(INVENTORY_NAME)

    @property
    def manifest_path(self) -> ObjectPath:
        return self.path(MANIFEST_NAME)


def release_record_path(bucket: str, prefix: str, release_id: str) -> ObjectPath:
    return ObjectPath.under(bucket, prefix, release_id, RELEASE_RECORD_NAME)


def dataset_file_path(
    bucket: str, prefix: str, release_id: str, dataset: str, file_name: str
) -> ObjectPath:
    return ObjectPath.under(bucket, prefix, release_id, dataset, file_name)


class ShardOutcome(StrEnum):
    CREATED = "created"
    """Downloaded and receipted by this run."""
    RECOVERED = "recovered"
    """Found in the bucket without a receipt, hashed in place and receipted."""
    VERIFIED = "verified"
    """Its receipt and object passed validation, so the shard was not downloaded again."""


class TableState(BaseModel):
    """A dataset's table when a load starts, whose UUID changes when it is created again."""

    model_config = ConfigDict(frozen=True)

    name: str
    uuid: UUID
    has_rows: bool
    load_sql_sha256: Sha256Hex


class Duplicates(BaseModel):
    """Keys that repeat across a release's objects.

    ``identical_rows`` counts the records beyond the first of keys whose records hold the same
    values; every other repeated key conflicts.
    """

    model_config = ConfigDict(frozen=True)

    identical_rows: NonNegativeInt
    conflicting_keys: NonNegativeInt
    examples: tuple[str, ...]


class PreparedLoad(BaseModel):
    model_config = ConfigDict(frozen=True)

    manifest_path: ObjectPath
    manifest: DatasetManifest
    table: TableState


class LoadObject(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset: DatasetName
    release: ReleaseId
    object_ref: ObjectRef


class LoadRecord(BaseModel):
    """One object loaded into a table, as the ``loads`` table keeps it."""

    model_config = ConfigDict(frozen=True)

    target: str
    target_uuid: UUID
    source_object: str
    generation: PositiveInt
    sha256: Sha256Hex
    source_release: ReleaseId
    records: NonNegativeInt
    written_rows: NonNegativeInt
    load_sql_sha256: Sha256Hex
    software_version: str
    image_digest: ImageDigest | None
    loaded_at: AwareDatetime


class LoadOutcome(StrEnum):
    LOADED = "loaded"
    ALREADY_LOADED = "already-loaded"
