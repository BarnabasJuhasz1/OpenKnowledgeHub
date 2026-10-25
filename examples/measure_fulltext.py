"""Bounded local JSONL/gzip schema probe; no archive-completion or coverage claim."""

import argparse
import gzip
import hashlib
import json
import zlib
from pathlib import Path
from time import perf_counter
from typing import Literal

from pydantic import BaseModel, ConfigDict

from openknowledge_core.ingestion.consumers import FulltextPaper, parse_paper
from openknowledge_core.ingestion.errors import IngestionError
from openknowledge_core.ingestion.fulltext import LookupRequest


class LocalMatch(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    corpusid: int
    line: int
    text_utf8_bytes: int
    restriction: Literal["policy-review-required"] = "policy-review-required"
    display_allowed: Literal[False] = False
    derivation_allowed: Literal[False] = False


class Measurement(BaseModel):
    source_file: str
    source_size_bytes: int
    scanned_prefix_sha256: str
    records: int
    decoded_bytes: int
    text_utf8_bytes: int
    empty_texts: int
    reported_license_counts: dict[str, int]
    seconds: float
    stop_reason: str
    lookup_status: Literal["not-requested", "found", "absent-in-file", "incomplete-scan"]
    match: LocalMatch | None = None
    source_scope: str = "historic base only if supplied as such; later diffs/deletes unsupported"
    provenance: str = "local-file-only; release/generation/receipt unverified"


def measure(
    path: Path,
    max_records: int = 100,
    max_record_bytes: int = 8 * 1024 * 1024,
    max_decoded_bytes: int = 64 * 1024 * 1024,
    corpusid: int | None = None,
) -> Measurement:
    if not 1 <= max_records <= 10000 or not 1 <= max_record_bytes <= 8 * 1024 * 1024:
        raise ValueError("bounds: 1..10000 records, 1..8 MiB per record")
    if not 1 <= max_decoded_bytes <= 64 * 1024 * 1024:
        raise ValueError("decoded bound: 1..64 MiB")
    if corpusid is not None:
        LookupRequest(corpusid=corpusid)
    match: LocalMatch | None = None
    start = perf_counter()
    digest = hashlib.sha256()
    count = decoded = text_bytes = empty = 0
    licenses: dict[str, int] = {}
    stop = "record-limit"
    if not path.is_file():
        raise ValueError("expected a local regular file")
    with path.open("rb") as raw:
        magic = raw.read(2)
        raw.seek(0)
        stream = gzip.GzipFile(fileobj=raw) if magic == b"\x1f\x8b" else raw
        try:
            while count < max_records:
                remaining = max_decoded_bytes - decoded
                if remaining <= 0:
                    stop = "decoded-byte-limit"
                    break
                line = stream.readline(min(max_record_bytes, remaining) + 1)
                if not line:
                    stop = "eof"
                    break
                if len(line) > min(max_record_bytes, remaining):
                    raise ValueError("record or total decoded-byte limit exceeded")
                paper = parse_paper("s2orc_v2", line)
                assert isinstance(paper, FulltextPaper)
                digest.update(line)
                count += 1
                decoded += len(line)
                text_bytes += len(paper.body.text.encode("utf-8"))
                empty += not paper.body.text.strip()
                value = (paper.openaccessinfo or {}).get("license")
                license_key = json.dumps(value) if isinstance(value, str) else "unknown"
                licenses[license_key] = licenses.get(license_key, 0) + 1
                if paper.corpusid == corpusid:
                    match = LocalMatch(
                        corpusid=paper.corpusid,
                        line=count,
                        text_utf8_bytes=len(paper.body.text.encode("utf-8")),
                    )
                    stop = "found-prefix"
                    break
        finally:
            if stream is not raw:
                stream.close()
    return Measurement(
        source_file=str(path),
        source_size_bytes=path.stat().st_size,
        scanned_prefix_sha256=digest.hexdigest(),
        records=count,
        decoded_bytes=decoded,
        text_utf8_bytes=text_bytes,
        empty_texts=empty,
        reported_license_counts=licenses,
        seconds=perf_counter() - start,
        stop_reason=stop,
        match=match,
        lookup_status=(
            "not-requested"
            if corpusid is None
            else "found"
            if match is not None
            else "absent-in-file"
            if stop == "eof"
            else "incomplete-scan"
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--max-records", type=int, default=100)
    parser.add_argument("--corpusid", type=int)
    parser.add_argument("--max-record-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--max-decoded-bytes", type=int, default=64 * 1024 * 1024)
    args = parser.parse_args()
    try:
        result = measure(
            args.path,
            max_records=args.max_records,
            corpusid=args.corpusid,
            max_record_bytes=args.max_record_bytes,
            max_decoded_bytes=args.max_decoded_bytes,
        )
    except (OSError, EOFError, ValueError, zlib.error, IngestionError):
        parser.exit(2, "full text probe failed: invalid input, unreadable file or exceeded bound\n")
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
