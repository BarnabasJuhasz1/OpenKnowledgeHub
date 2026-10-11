"""Inputs and results of the ingestion use cases."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, NonNegativeInt

from openknowledge_contracts.datasets import DatasetName, ReleaseId, ShardRef
from openknowledge_contracts.manifests import BucketName, ObjectPath, ObjectPrefix
from openknowledge_core.ingestion.models import ShardOutcome

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
    """``shard`` indexes the archived inventory, whose order is fixed once it is written."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: ReleaseId
    dataset: DatasetName
    shard: NonNegativeInt
    bucket: BucketName
    prefix: ObjectPrefix = DEFAULT_PREFIX


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
