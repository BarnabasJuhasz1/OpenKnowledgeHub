"""Restricted metadata from an explicitly scoped archive stream, without exposing text."""

from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictInt

from openknowledge_core.ingestion.consumers import ArchivePaper, FulltextPaper, SourceLineage

CorpusId = Annotated[StrictInt, Field(ge=0, le=2**64 - 1)]


class FulltextMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    corpusid: CorpusId
    source: SourceLineage
    text_state: Literal["present", "empty"]
    source_scope: Literal["historic-base-only; later diffs/deletes unsupported"] = (
        "historic-base-only; later diffs/deletes unsupported"
    )
    integrity: Literal["completion-pinned-prefix"] = "completion-pinned-prefix"
    text_utf8_bytes: int
    license_reported: str | None
    oa_status_reported: str | None
    # Evidence flags, never an interpretation of redistribution or derivative rights.
    restriction: Literal["policy-review-required"] = "policy-review-required"
    display_allowed: Literal[False] = False
    derivation_allowed: Literal[False] = False


class LookupRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    corpusid: CorpusId
    max_records: Annotated[StrictInt, Field(gt=0, le=10000)] = 100


class LookupResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    status: Literal["found", "absent-in-stream", "scan-limit-reached"]
    scanned: int
    metadata: FulltextMetadata | None = None


def _reported(info: dict[str, JsonValue] | None, key: str) -> str | None:
    value = info.get(key) if info else None
    return value if isinstance(value, str) else None


def metadata(row: ArchivePaper) -> FulltextMetadata:
    if row.source.dataset != "s2orc_v2" or not isinstance(row.paper, FulltextPaper):
        raise ValueError("full text metadata requires an s2orc_v2 record")
    paper = row.paper
    return FulltextMetadata(
        corpusid=paper.corpusid,
        source=row.source,
        text_state="present" if paper.body.text.strip() else "empty",
        text_utf8_bytes=len(paper.body.text.encode("utf-8")),
        license_reported=_reported(paper.openaccessinfo, "license"),
        oa_status_reported=_reported(paper.openaccessinfo, "status"),
    )


async def lookup(rows: AsyncGenerator[ArchivePaper], request: LookupRequest) -> LookupResult:
    """Close the supplied stream on success, failure or cancellation.

    Absence describes only the supplied stream (possibly a base prefix), never the
    current corpus. Later diffs and deletes are unsupported. First match wins;
    this is not a base/diff materializer. No byte offsets are produced: ordinary
    gzip requires decoding from a valid member start.
    """
    scanned = 0
    async with aclosing(rows):
        async for row in rows:
            scanned += 1
            item = metadata(row)
            if item.corpusid == request.corpusid:
                return LookupResult(status="found", scanned=scanned, metadata=item)
            if scanned >= request.max_records:
                return LookupResult(status="scan-limit-reached", scanned=scanned)
    return LookupResult(status="absent-in-stream", scanned=scanned)
