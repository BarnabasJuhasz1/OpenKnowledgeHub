"""What landed in the bucket: object references, shard receipts and the per-dataset manifest."""

from __future__ import annotations

from typing import Annotated, Final, Literal, Self

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    model_validator,
)

from openknowledge_contracts.datasets import ArchiveDocument, DatasetName, ReleaseId, ShardRef


def _object_name(value: str) -> str:
    if not 0 < len(value.encode()) <= 1024 or not value.isprintable():
        raise ValueError("an object name is 1 to 1024 bytes of printable UTF-8")
    if any(segment in ("", ".", "..") for segment in value.split("/")):
        raise ValueError("an object name has no empty, '.' or '..' segment")
    return value


def _object_prefix(value: str) -> str:
    value = value.strip("/")
    return _object_name(value) if value else value


BucketName = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$")]
ObjectName = Annotated[str, AfterValidator(_object_name)]
ObjectPrefix = Annotated[str, AfterValidator(_object_prefix)]
"""An empty prefix puts objects at the bucket root."""
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Md5Base64 = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9+/]{22}==$")]
Crc32cBase64 = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9+/]{6}==$")]
ImageDigest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]

SOURCE: Final = "semantic-scholar"
SCHEMA_VERSION: Final = 1
TRANSFORM_VERSION: Final = 1


class ObjectPath(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    bucket: BucketName
    name: ObjectName

    def __str__(self) -> str:
        return f"gs://{self.bucket}/{self.name}"

    @classmethod
    def under(cls, bucket: str, prefix: str, *parts: str) -> ObjectPath:
        return cls(bucket=bucket, name="/".join(part for part in (prefix, *parts) if part))


class ObjectRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    path: ObjectPath
    generation: PositiveInt


class Producer(BaseModel):
    """What wrote an object, where ``verbatim`` means the bytes are stored exactly as served."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    transform: Literal["verbatim"]
    transform_version: Literal[1]
    software_version: Annotated[str, StringConstraints(min_length=1, max_length=64)]
    image_digest: ImageDigest | None


class ShardReceipt(ArchiveDocument):
    """``<file>.receipt.json``: written last, after the object it names passed every check."""

    schema_version: Literal[1]
    source: Literal["semantic-scholar"]
    release_id: ReleaseId
    dataset: DatasetName
    shard: ShardRef
    object_ref: ObjectRef
    size: NonNegativeInt
    sha256: Sha256Hex
    md5: Md5Base64
    crc32c: Crc32cBase64
    schema_ref: Annotated[str, StringConstraints(min_length=1, max_length=256)]
    """The source's schema for this dataset and release, as semantic-scholar/<dataset>@<release>."""
    gzip_members: PositiveInt
    records: NonNegativeInt
    uncompressed_size: NonNegativeInt
    producer: Producer
    completed_at: AwareDatetime


class ManifestEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    object_ref: ObjectRef
    size: NonNegativeInt
    sha256: Sha256Hex
    md5: Md5Base64
    crc32c: Crc32cBase64
    records: NonNegativeInt


class DatasetManifest(ArchiveDocument):
    """``manifest.json``: every receipted object of one dataset."""

    schema_version: Literal[1]
    release_id: ReleaseId
    dataset: DatasetName
    entries: tuple[ManifestEntry, ...] = Field(min_length=1)
    producer: Producer
    completed_at: AwareDatetime

    @model_validator(mode="after")
    def _unique(self) -> Self:
        names = {str(entry.object_ref.path) for entry in self.entries}
        if len(names) != len(self.entries):
            raise ValueError("every entry needs its own object")
        return self
