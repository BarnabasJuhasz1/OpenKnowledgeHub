"""Semantic Scholar release and dataset metadata, as served and as archived in the raw bucket."""

from __future__ import annotations

import json
import re
from typing import Annotated, Literal, Self, cast
from urllib.parse import unquote

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

ReleaseId = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]
DatasetName = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")]
FileName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")]
DiffPart = Literal["updates", "deletes"]

RELEASE_RECORD_NAME = "release.json"
INVENTORY_NAME = "dataset.json"
MANIFEST_NAME = "manifest.json"
RECEIPT_SUFFIX = ".receipt.json"
DIFF_INVENTORY_NAME = "diff.json"
DIFFS_FOLDER = "diffs"

_PLAIN_KEY = re.compile(r"[A-Za-z0-9_-]{1,64}")
_DECODE_ROUNDS = 3
# Query parameters that turn a URL into a credential: S3 SigV2 and SigV4, GCS V4, CloudFront.
_SIGNING_PARAMETER = re.compile(
    r"(?:^|[?&;\s\"'(<])(?:awsaccesskeyid|signature|key-pair-id|x-amz-[a-z0-9-]+|x-goog-[a-z0-9-]+)=",
    re.IGNORECASE,
)


class SignedMaterialError(ValueError):
    """Raised for a URL signing parameter in an archived document, naming only where it sits."""


def reject_signed(document: object, where: str = "$") -> None:
    """Raise if a JSON key or string holds a URL signing parameter, even percent-encoded."""
    if isinstance(document, str):
        text = document
        for _ in range(_DECODE_ROUNDS + 1):
            if _SIGNING_PARAMETER.search(text):
                raise SignedMaterialError(f"{where} holds a URL signing parameter")
            decoded = unquote(text)
            if decoded == text:
                return
            text = decoded
    elif isinstance(document, dict):
        for key, value in cast("dict[str, object]", document).items():
            try:
                reject_signed(key)
            except SignedMaterialError:
                raise SignedMaterialError(
                    f"a key under {where} holds a URL signing parameter"
                ) from None
            reject_signed(value, f"{where}.{key if _PLAIN_KEY.fullmatch(key) else '?'}")
    elif isinstance(document, list):
        for index, item in enumerate(cast("list[object]", document)):
            reject_signed(item, f"{where}[{index}]")


class ArchiveDocument(BaseModel):
    """Written once to the bucket, then read back by any later run that would write it again."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    def to_json(self) -> bytes:
        document = self.model_dump(mode="json")
        reject_signed(document)
        return json.dumps(document, ensure_ascii=False, indent=1).encode()

    @classmethod
    def from_json(cls, data: bytes) -> Self:
        """Every failure is a ValueError that may name the document's keys but never its values."""
        try:
            document: object = json.loads(data)
            reject_signed(document)
        except RecursionError:
            raise ValueError("the document nests too deeply") from None
        except SignedMaterialError:
            raise
        except ValueError:  # also bad UTF-8, and integers too long for Python to parse
            raise ValueError("the document could not be parsed as JSON") from None
        return cls.model_validate(document)


class _Upstream(BaseModel):
    """A shape Semantic Scholar serves, keeping any field it adds later."""

    model_config = ConfigDict(
        frozen=True, extra="allow", hide_input_in_errors=True, serialize_by_alias=True
    )


class DatasetSummary(_Upstream):
    name: DatasetName
    description: str
    readme: str = Field(alias="README")


class ReleaseMetadata(_Upstream):
    release_id: ReleaseId
    readme: str = Field(alias="README")
    datasets: tuple[DatasetSummary, ...]


class ShardRef(BaseModel):
    """One source object of a dataset, identified across listings by ``source_path``."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    source_path: Annotated[str, StringConstraints(max_length=2048)]
    file_name: FileName

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        path = self.source_path
        if not path.startswith("/") or not path.isprintable() or "?" in path or "#" in path:
            raise ValueError("source_path must be a printable URL path without query or fragment")
        if path.rsplit("/", 1)[-1] != self.file_name:
            raise ValueError("file_name must be the last segment of source_path")
        name = self.file_name
        if name in (INVENTORY_NAME, MANIFEST_NAME) or name.endswith(RECEIPT_SUFFIX):
            raise ValueError("file_name collides with an archive document")
        return self


class ReleaseRecord(ArchiveDocument):
    """``release.json``: the release metadata with its README, as Semantic Scholar served it."""

    retrieved_at: AwareDatetime
    metadata: ReleaseMetadata


class DatasetInventory(ArchiveDocument):
    """``dataset.json``: the dataset metadata and its shards, in the order shard indices use."""

    release_id: ReleaseId
    retrieved_at: AwareDatetime
    dataset: DatasetSummary
    shards: tuple[ShardRef, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self) -> Self:
        if not _distinct(self.shards):
            raise ValueError("every shard needs its own source path and file name")
        return self


class DiffInventory(ArchiveDocument):
    """``diff.json``: one dataset's update and delete files from one release to the next."""

    from_release: ReleaseId
    release_id: ReleaseId
    dataset: DatasetName
    retrieved_at: AwareDatetime
    updates: tuple[ShardRef, ...]
    deletes: tuple[ShardRef, ...]

    @model_validator(mode="after")
    def _ordered_and_unique(self) -> Self:
        if self.from_release >= self.release_id:
            raise ValueError("from_release must come before release_id")
        if not (_distinct(self.updates) and _distinct(self.deletes)):
            raise ValueError("every file of a part needs its own source path and file name")
        return self


def _distinct(shards: tuple[ShardRef, ...]) -> bool:
    paths = {shard.source_path for shard in shards}
    names = {shard.file_name for shard in shards}
    return len(paths) == len(names) == len(shards)
