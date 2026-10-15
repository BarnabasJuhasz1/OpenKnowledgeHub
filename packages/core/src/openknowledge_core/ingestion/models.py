"""Values that are never written to storage."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID, uuid4

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    SecretStr,
    field_validator,
    model_validator,
)

from openknowledge_contracts.datasets import (
    DIFF_INVENTORY_NAME,
    DIFFS_FOLDER,
    INVENTORY_NAME,
    MANIFEST_NAME,
    RECEIPT_SUFFIX,
    RELEASE_RECORD_NAME,
    DatasetName,
    DatasetSummary,
    DiffPart,
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
from openknowledge_contracts.tables.contract import UpdateMode

type Operation = Literal["upsert", "delete"]


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


class DiffListing(BaseModel):
    """One listed diff: the files that bring a dataset from ``from_release`` to ``release_id``."""

    model_config = ConfigDict(frozen=True)

    from_release: ReleaseId
    release_id: ReleaseId
    updates: tuple[ShardLink, ...]
    deletes: tuple[ShardLink, ...]


class DiffChain(BaseModel):
    """The diffs listed between two releases, one per release that followed the first."""

    model_config = ConfigDict(frozen=True)

    dataset: DatasetName
    diffs: tuple[DiffListing, ...]


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
    """One dataset of one release, with the shards of its archived inventory.

    With ``from_release`` and ``part``, it is instead that part of the dataset's diff from
    ``from_release`` to ``release_id``, whose shards ``diff.json`` lists.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    release_id: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix
    shards: tuple[ShardRef, ...]
    from_release: ReleaseId | None = None
    part: DiffPart | None = None

    @model_validator(mode="after")
    def _diff(self) -> Self:
        check_diff(self.from_release, self.part, self.release_id)
        return self

    def path(self, file_name: str) -> ObjectPath:
        if self.from_release is None or self.part is None:
            return dataset_file_path(
                self.bucket, self.prefix, self.release_id, self.dataset, file_name
            )
        return diff_file_path(
            self.bucket,
            self.prefix,
            self.from_release,
            self.release_id,
            self.dataset,
            self.part,
            file_name,
        )

    def object_path(self, shard: ShardRef) -> ObjectPath:
        return self.path(shard.file_name)

    def receipt_path(self, shard: ShardRef) -> ObjectPath:
        return self.path(shard.file_name + RECEIPT_SUFFIX)

    @property
    def inventory_path(self) -> ObjectPath:
        if self.from_release is None:
            return self.path(INVENTORY_NAME)
        return diff_file_path(
            self.bucket,
            self.prefix,
            self.from_release,
            self.release_id,
            self.dataset,
            DIFF_INVENTORY_NAME,
        )

    @property
    def manifest_path(self) -> ObjectPath:
        return self.path(MANIFEST_NAME)


def release_record_path(bucket: str, prefix: str, release_id: str) -> ObjectPath:
    return ObjectPath.under(bucket, prefix, release_id, RELEASE_RECORD_NAME)


def dataset_file_path(
    bucket: str, prefix: str, release_id: str, dataset: str, file_name: str
) -> ObjectPath:
    return ObjectPath.under(bucket, prefix, release_id, dataset, file_name)


def diff_file_path(
    bucket: str, prefix: str, from_release: str, release_id: str, dataset: str, *names: str
) -> ObjectPath:
    return ObjectPath.under(bucket, prefix, DIFFS_FOLDER, from_release, release_id, dataset, *names)


def check_diff(from_release: str | None, part: str | None, release_id: str) -> None:
    """A diff part names the release it starts from and its part, and a base names neither."""
    if (from_release is None) != (part is None):
        raise ValueError("a diff file needs both the release it starts from and its part")
    if from_release is not None and from_release >= release_id:
        raise ValueError("a diff must start from a release before the one it brings")


class PinnedDiff(BaseModel):
    """One dataset's diff from one release to the next, as its archived ``diff.json`` lists it."""

    model_config = ConfigDict(frozen=True)

    updates: PinnedDataset
    deletes: PinnedDataset

    def part(self, name: DiffPart) -> PinnedDataset:
        return self.updates if name == "updates" else self.deletes


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
    delete_sql_sha256: Sha256Hex
    updates: UpdateMode

    def sql_sha256(self, operation: Operation) -> str:
        return self.delete_sql_sha256 if operation == "delete" else self.load_sql_sha256


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
    operation: Operation = "upsert"


class PreparedDiff(BaseModel):
    """A diff to load, where a part without files, and so without a manifest, is ``None``."""

    model_config = ConfigDict(frozen=True)

    from_release: ReleaseId
    release_id: ReleaseId
    dataset: DatasetName
    table: TableState
    inventory_path: ObjectPath
    updates: PreparedLoad | None
    deletes: PreparedLoad | None
    identical_duplicates: NonNegativeInt

    @property
    def parts(self) -> tuple[PreparedLoad, ...]:
        return tuple(part for part in (self.updates, self.deletes) if part is not None)


class LoadObject(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset: DatasetName
    release: ReleaseId
    object_ref: ObjectRef
    operation: Operation = "upsert"


class LoadRecord(BaseModel):
    """One object loaded into a table, as the ``loads`` table keeps it."""

    model_config = ConfigDict(frozen=True)

    target: str
    target_uuid: UUID
    source_object: str
    generation: PositiveInt
    sha256: Sha256Hex
    source_release: ReleaseId
    operation: Operation
    records: NonNegativeInt
    written_rows: NonNegativeInt
    load_sql_sha256: Sha256Hex
    software_version: str
    image_digest: ImageDigest | None
    loaded_at: AwareDatetime


class AppliedRelease(BaseModel):
    """A base release or a diff that a table took in, as the ``applied`` table keeps it."""

    model_config = ConfigDict(frozen=True)

    target: str
    target_uuid: UUID
    release: ReleaseId
    from_release: ReleaseId | None
    software_version: str
    image_digest: ImageDigest | None
    applied_at: AwareDatetime


def held_release(table: TableState, latest: AppliedRelease | None) -> ReleaseId | None:
    """The release ``table`` holds, which a dropped, recreated or emptied table no longer does."""
    if latest is None or latest.target_uuid != table.uuid or not table.has_rows:
        return None
    return latest.release


class LoadOutcome(StrEnum):
    LOADED = "loaded"
    ALREADY_LOADED = "already-loaded"


class ShardJob(BaseModel):
    """Archive these shards of a pinned dataset or diff part, each in its own task."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation_id: UUID = Field(default_factory=uuid4)
    """One logical submission, retained across retries and renewed for the next round."""
    release_id: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix
    shards: tuple[NonNegativeInt, ...] = Field(min_length=1)
    from_release: ReleaseId | None = None
    part: DiffPart | None = None

    @field_validator("operation_id")
    @classmethod
    def _nonzero_operation_id(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("an operation id must not be zero")
        return value

    @model_validator(mode="after")
    def _diff(self) -> Self:
        check_diff(self.from_release, self.part, self.release_id)
        return self


class JobOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
