"""Inputs and results of the ingestion use cases."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, NonNegativeInt, model_validator

from openknowledge_contracts.datasets import DatasetName, DiffPart, ReleaseId, ShardRef
from openknowledge_contracts.manifests import BucketName, ObjectPath, ObjectPrefix
from openknowledge_core.ingestion.models import ShardOutcome, check_diff

DEFAULT_PREFIX = "semantic-scholar"


class PinRelease(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    release: ReleaseId | Literal["latest"] = "latest"
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX


class PinnedRelease(BaseModel):
    model_config = ConfigDict(frozen=True)

    release_id: ReleaseId
    record: ObjectPath


class DownloadDataset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    release: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX


class DownloadShard(BaseModel):
    """``shard`` indexes the archived inventory, whose order is fixed once it is written.

    With ``from_release`` and ``part``, it indexes that part of the diff in ``diff.json``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: ReleaseId
    dataset: DatasetName
    shard: NonNegativeInt
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX
    from_release: ReleaseId | None = None
    part: DiffPart | None = None

    @model_validator(mode="after")
    def _diff(self) -> Self:
        check_diff(self.from_release, self.part, self.release)
        return self


class DownloadDiff(BaseModel):
    """One dataset's changes from ``from_release`` to ``release``, the release right after it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    from_release: ReleaseId
    release: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.from_release >= self.release:
            raise ValueError("a diff must start from a release before the one it brings")
        return self


class DownloadDatasetResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    release_id: ReleaseId
    dataset: DatasetName
    shards: NonNegativeInt
    created: NonNegativeInt
    recovered: NonNegativeInt
    verified: NonNegativeInt
    inventory: ObjectPath
    manifest: ObjectPath


class DownloadShardResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    release_id: ReleaseId
    dataset: DatasetName
    index: NonNegativeInt
    shard: ShardRef
    outcome: ShardOutcome
    receipt: ObjectPath


class LoadDataset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    release: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX


class LoadDatasetResult(BaseModel):
    """``removed`` counts the rows that flag keys a snapshot release no longer lists."""

    model_config = ConfigDict(frozen=True)

    release_id: ReleaseId
    dataset: DatasetName
    table: str
    objects: NonNegativeInt
    loaded: NonNegativeInt
    already_loaded: NonNegativeInt
    live_rows: NonNegativeInt
    identical_duplicates: NonNegativeInt
    removed: NonNegativeInt
    manifest: ObjectPath


class LoadDiff(BaseModel):
    """One dataset's changes from ``from_release`` to ``release``, the release right after it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    from_release: ReleaseId
    release: ReleaseId
    dataset: DatasetName
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.from_release >= self.release:
            raise ValueError("a diff must start from a release before the one it brings")
        return self


class LoadDiffResult(BaseModel):
    """``upserted`` and ``deleted`` count the records in the update and the delete files."""

    model_config = ConfigDict(frozen=True)

    from_release: ReleaseId
    release_id: ReleaseId
    dataset: DatasetName
    table: str
    objects: NonNegativeInt
    loaded: NonNegativeInt
    already_loaded: NonNegativeInt
    upserted: NonNegativeInt
    deleted: NonNegativeInt
    identical_duplicates: NonNegativeInt
    manifests: tuple[ObjectPath, ...]


class BatchDownloadResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    release_id: ReleaseId
    dataset: DatasetName
    shards: NonNegativeInt
    jobs: tuple[str, ...]
    manifest: ObjectPath


class BatchDiffDownloadResult(BaseModel):
    """``manifests`` has one per part with files; a part without files has none."""

    model_config = ConfigDict(frozen=True)

    from_release: ReleaseId
    release_id: ReleaseId
    dataset: DatasetName
    updates: NonNegativeInt
    deletes: NonNegativeInt
    jobs: tuple[str, ...]
    manifests: tuple[ObjectPath, ...]
