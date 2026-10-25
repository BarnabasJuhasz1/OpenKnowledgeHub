"""Offline, single-shard probe; not a cloud loader or a resolved snapshot builder."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import struct
import time
from collections.abc import Generator
from contextlib import closing
from pathlib import Path
from tempfile import mkdtemp
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from openknowledge_contracts.manifests import Sha256Hex, ShardReceipt
from openknowledge_core.ingestion.consumers import EmbeddingPaper, parse_paper
from openknowledge_core.ingestion.errors import IngestionError
from openknowledge_core.ingestion.integrity import StreamCheck


class ProbeResult(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, hide_input_in_errors=True)

    format_version: Literal[1] = 1
    dtype: Literal["float32"] = "float32"
    dimension: Literal[768] = 768
    operation: Literal["base-shard"] = "base-shard"
    source: ShardReceipt
    batch_rows: int = Field(ge=1, le=1024)
    max_record_bytes: int = Field(ge=1, le=8 * 1024 * 1024)
    rows: int = Field(ge=0)
    parquet_bytes: int = Field(gt=0)
    parquet_sha256: Sha256Hex
    max_absolute_error: float = Field(ge=0, allow_inf_nan=False)
    elapsed_seconds: float = Field(ge=0, allow_inf_nan=False)


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(256 * 1024):
            result.update(block)
    return result.hexdigest()


def source_lines(source: Path, receipt: ShardReceipt, limit: int) -> Generator[bytes]:
    """Validate the exact compressed bytes decoded, including concatenated members."""
    check = StreamCheck()
    pending = bytearray()
    uncompressed = 0
    compressed = 0
    with source.open("rb") as stream:
        while chunk := stream.read(256 * 1024):
            compressed += len(chunk)
            if compressed > receipt.size:
                raise ValueError("source compressed bytes exceed receipt")
            for block in check.decode(chunk):
                uncompressed += len(block)
                if uncompressed > receipt.uncompressed_size:
                    raise ValueError("source uncompressed bytes exceed receipt")
                start = 0
                while start < len(block):
                    end = block.find(b"\n", start)
                    stop = len(block) if end < 0 else end + 1
                    if len(pending) + stop - start > limit:
                        raise ValueError("source line exceeds max_record_bytes")
                    pending.extend(block[start:stop])
                    if end >= 0:
                        yield bytes(pending)
                        pending.clear()
                    start = stop
        if pending:
            yield bytes(pending)
    report = check.finish(receipt.size)
    if (
        report.sha256,
        report.md5,
        report.records,
        report.gzip_members,
        report.uncompressed_size,
    ) != (
        receipt.sha256,
        receipt.md5,
        receipt.records,
        receipt.gzip_members,
        receipt.uncompressed_size,
    ):
        raise ValueError("source SHA256, MD5, counts or uncompressed bytes disagree with receipt")


def _batch(
    ids: list[int], lines: list[int], vectors: list[list[float]], schema: pa.Schema
) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"corpusid": cid, "source_line": line, "vector": vector}
            for cid, line, vector in zip(ids, lines, vectors, strict=True)
        ],
        schema=schema,
    )


def convert(
    source: Path,
    receipt_path: Path,
    destination: Path,
    *,
    batch_rows: int = 128,
    max_record_bytes: int = 1024 * 1024,
) -> ProbeResult:
    """Publish a complete directory atomically; failed attempts leave no success receipt.

    Input must be a complete locally available base shard. No sampling or
    truncation is performed. Repeated calls verify output hashes before reusing it.
    """
    if (
        type(batch_rows) is not int
        or type(max_record_bytes) is not int
        or not 1 <= batch_rows <= 1024
        or not 1 <= max_record_bytes <= 8 * 1024 * 1024
    ):
        raise ValueError("batch_rows must be 1..1024; max_record_bytes must be 1..8388608")
    if not source.is_file() or not receipt_path.is_file():
        raise ValueError("source and receipt must be existing regular files")
    receipt = ShardReceipt.from_json(receipt_path.read_bytes())
    if (
        receipt.dataset != "embeddings-specter_v2"
        or "diffs" in receipt.object_ref.path.name.split("/")
        or receipt.schema_ref.endswith("/deletes")
    ):
        raise ValueError("only embeddings-specter_v2 base shards are supported")
    if source.stat().st_size != receipt.size:
        raise ValueError("source size or SHA256 disagrees with receipt")
    schema = pa.schema(
        [
            ("corpusid", pa.uint64()),
            ("source_line", pa.uint64()),
            ("vector", pa.list_(pa.field("element", pa.float32()), 768)),
        ],
        metadata={
            b"format_version": b"1",
            b"dataset": receipt.dataset.encode(),
            b"release": receipt.release_id.encode(),
            b"model": b"specter_v2",
            b"source_receipt": receipt.model_dump_json().encode(),
            b"semantics": b"base shard; source order; duplicates unresolved; no tombstones",
        },
    )
    if destination.is_symlink():
        raise ValueError("destination must not be a symlink")
    if destination.exists():
        for name in ("receipt.json", "vectors.parquet"):
            artifact = destination / name
            if artifact.is_symlink():
                raise ValueError(f"existing destination {name} must not be a symlink")
            if not artifact.is_file():
                raise ValueError(f"existing destination {name} must be a regular file")
        for _ in source_lines(source, receipt, max_record_bytes):
            pass
        prior = ProbeResult.model_validate_json((destination / "receipt.json").read_bytes())
        if (
            prior.source != receipt
            or prior.batch_rows != batch_rows
            or prior.max_record_bytes != max_record_bytes
            or prior.rows != receipt.records
            or (destination / "vectors.parquet").stat().st_size != prior.parquet_bytes
            or digest(destination / "vectors.parquet") != prior.parquet_sha256
        ):
            raise ValueError("existing destination conflicts with source, options or output hash")
        with pq.ParquetFile(destination / "vectors.parquet") as parquet:
            if parquet.metadata.num_rows != prior.rows or not parquet.schema_arrow.equals(
                schema, check_metadata=True
            ):
                raise ValueError("existing destination schema or row count conflicts with receipt")
        return prior
    destination.parent.mkdir(parents=True, exist_ok=True)
    scratch = Path(mkdtemp(prefix=".embedding-probe-", dir=destination.parent))
    started = time.perf_counter()
    rows = 0
    uncompressed = 0
    error = 0.0
    try:
        with (
            closing(source_lines(source, receipt, max_record_bytes)) as source_stream,
            pq.ParquetWriter(scratch / "vectors.parquet", schema, compression="zstd") as writer,
        ):
            ids: list[int] = []
            lines: list[int] = []
            vectors: list[list[float]] = []
            for line in source_stream:
                uncompressed += len(line)
                try:
                    paper = parse_paper(receipt.dataset, line)
                except IngestionError as exc:
                    raise IngestionError(f"source line {rows + 1}: {exc}") from None
                if not isinstance(paper, EmbeddingPaper) or paper.corpusid > 2**64 - 1:
                    raise ValueError(f"source line {rows + 1} has unsupported corpusid")
                try:
                    rounded = [
                        struct.unpack("f", struct.pack("f", value))[0] for value in paper.vector
                    ]
                except OverflowError:
                    raise ValueError(f"source line {rows + 1} overflows float32") from None
                if not all(math.isfinite(value) for value in rounded):
                    raise ValueError(f"source line {rows + 1} overflows float32")
                error = max(
                    error, max(abs(a - b) for a, b in zip(paper.vector, rounded, strict=True))
                )
                if rows >= receipt.records:
                    raise ValueError("source records exceed receipt")
                rows += 1
                ids.append(paper.corpusid)
                lines.append(rows)
                vectors.append(rounded)
                if len(ids) == batch_rows:
                    writer.write_table(_batch(ids, lines, vectors, schema))
                    ids.clear()
                    lines.clear()
                    vectors.clear()
            if ids:
                writer.write_table(_batch(ids, lines, vectors, schema))
        if rows != receipt.records or uncompressed != receipt.uncompressed_size:
            raise ValueError("source records or uncompressed bytes disagree with receipt")
        output = scratch / "vectors.parquet"
        result = ProbeResult(
            source=receipt,
            batch_rows=batch_rows,
            max_record_bytes=max_record_bytes,
            rows=rows,
            parquet_bytes=output.stat().st_size,
            parquet_sha256=digest(output),
            max_absolute_error=error,
            elapsed_seconds=time.perf_counter() - started,
        )
        (scratch / "receipt.json").write_text(result.model_dump_json(indent=2) + "\n")
        os.rename(scratch, destination)
        return result
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--batch-rows", type=int, default=128)
    parser.add_argument("--max-record-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()
    try:
        result = convert(
            args.source,
            args.receipt,
            args.destination,
            batch_rows=args.batch_rows,
            max_record_bytes=args.max_record_bytes,
        )
    except (ValueError, OSError, IngestionError, pa.ArrowException) as exc:
        parser.exit(2, f"{parser.prog}: {exc}\n")
    print(json.dumps(result.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    main()
