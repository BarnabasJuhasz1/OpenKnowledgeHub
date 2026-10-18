"""Bounded, read-only streams from completed bases; diffs are not materialized here."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    Json,
    JsonValue,
    StrictInt,
    ValidationError,
    field_validator,
)

from openknowledge_contracts.datasets import DatasetName, ReleaseId
from openknowledge_contracts.manifests import ManifestEntry, ObjectRef
from openknowledge_core.ingestion.archive_progress import ArchiveReader, HashedRef
from openknowledge_core.ingestion.commands import DownloadDataset
from openknowledge_core.ingestion.errors import IngestionError, ObjectChanged
from openknowledge_core.ingestion.integrity import StreamCheck
from openknowledge_core.ingestion.ports import ArtifactStore


class Payload(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True, hide_input_in_errors=True)


class TextBlock(Payload):
    text: str
    annotations: dict[str, str | None] | None = None


class FulltextPaper(Payload):
    corpusid: Annotated[StrictInt, Field(ge=0)]
    body: TextBlock
    title: str | None = None
    authors: list[str] | None = None
    bibliography: TextBlock | None = None
    openaccessinfo: dict[str, JsonValue] | None = None


class EmbeddingPaper(Payload):
    corpusid: Annotated[StrictInt, Field(ge=0)]
    model: Literal["specter_v2"]
    vector: Json[Annotated[list[FiniteFloat], Field(min_length=768, max_length=768)]]


class ConsumeBase(DownloadDataset):
    max_records: Annotated[int, Field(gt=0)] | None = 10
    max_record_bytes: Annotated[int, Field(gt=0, le=64 * 1024 * 1024)] = 8 * 1024 * 1024

    @field_validator("dataset")
    @classmethod
    def _supported_dataset(cls, value: str) -> str:
        if value not in ("s2orc_v2", "embeddings-specter_v2"):
            raise ValueError("select s2orc_v2 or embeddings-specter_v2 completed bases")
        return value


class SourceLineage(BaseModel):
    model_config = ConfigDict(frozen=True)
    dataset: DatasetName
    release: ReleaseId
    kind: Literal["base"] = "base"
    completion: HashedRef
    manifest: HashedRef
    object_ref: ObjectRef
    line: int


class ArchivePaper(BaseModel):
    source: SourceLineage
    paper: FulltextPaper | EmbeddingPaper
    integrity: Literal["completion-pinned-prefix"] = "completion-pinned-prefix"
    """Whole-shard hashes are checked only when iteration reaches the shard's end."""


def parse_paper(dataset: str, line: bytes) -> FulltextPaper | EmbeddingPaper:
    try:
        value: object = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError("expected a JSON object")
        if "schema_version" in value:
            raise ValueError("explicit schema versions are unsupported; expected unversioned rows")
        if dataset == "s2orc_v2":
            return FulltextPaper.model_validate(value)
        if dataset == "embeddings-specter_v2":
            return EmbeddingPaper.model_validate(value)
        raise ValueError("unsupported dataset; select s2orc_v2 or embeddings-specter_v2")
    except ValidationError as exc:
        fields = ", ".join(
            ".".join(str(part) for part in error["loc"])
            for error in exc.errors(include_input=False, include_url=False)
        )
        raise IngestionError(
            f"unsupported or malformed {dataset} payload fields: {fields}"
        ) from None
    except (ValueError, RecursionError):
        raise IngestionError(
            f"invalid {dataset} row; expected unversioned JSON with corpusid and "
            "body.text, or model=specter_v2 and a JSON string of 768 finite vector values"
        ) from None


class ArchiveConsumer:
    def __init__(self, store: ArtifactStore) -> None:
        self._store = store
        self._reader = ArchiveReader(store)

    async def stream(self, command: ConsumeBase) -> AsyncGenerator[ArchivePaper]:
        """Use aclosing around this generator when stopping early; records are a base prefix.

        Completion and document hashes are checked before reading payloads. Each fully
        consumed shard is checked against its manifest; a bounded prefix cannot certify
        unread bytes. Nothing is written, including missing completion or manifest files.
        """
        completion, manifest_ref, manifest = await self._reader.completed_base(command)
        count = 0
        for entry in manifest.entries:
            async with aclosing(self._lines(entry, command.max_record_bytes)) as lines:
                async for number, line in lines:
                    try:
                        paper = parse_paper(command.dataset, line)
                    except IngestionError as exc:
                        raise IngestionError(
                            f"{entry.object_ref.path} line {number}: {exc}; "
                            "check the source schema before retrying"
                        ) from None
                    yield ArchivePaper(
                        source=SourceLineage(
                            dataset=command.dataset,
                            release=command.release,
                            completion=completion,
                            manifest=manifest_ref,
                            object_ref=entry.object_ref,
                            line=number,
                        ),
                        paper=paper,
                    )
                    count += 1
                    if count % 128 == 0:
                        await asyncio.sleep(0)
                    if command.max_records is not None and count >= command.max_records:
                        return

    async def _lines(self, entry: ManifestEntry, limit: int) -> AsyncGenerator[tuple[int, bytes]]:
        source = await self._store.open_read(entry.object_ref)
        check = StreamCheck()
        pending = bytearray()
        number = 0
        async with aclosing(source.chunks) as chunks:
            async for chunk in chunks:
                for block in check.decode(chunk):
                    start = 0
                    while start < len(block):
                        end = block.find(b"\n", start)
                        stop = len(block) if end < 0 else end
                        if len(pending) + stop - start > limit:
                            raise IngestionError(
                                f"{entry.object_ref.path} line {number + 1} exceeds "
                                f"max_record_bytes={limit}; raise the explicit bound if appropriate"
                            )
                        pending.extend(block[start:stop])
                        if end < 0:
                            break
                        number += 1
                        yield number, bytes(pending)
                        pending.clear()
                        start = end + 1
            if pending:
                number += 1
                yield number, bytes(pending)
            report = check.finish(entry.size)
            if (report.sha256, report.md5, report.records) != (
                entry.sha256,
                entry.md5,
                entry.records,
            ):
                raise ObjectChanged(
                    f"{entry.object_ref.path}: streamed bytes disagree with manifest"
                )
