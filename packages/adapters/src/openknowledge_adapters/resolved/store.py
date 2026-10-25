"""SQLite resolution and immutable local Parquet artifacts."""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import struct
import tempfile
from collections.abc import AsyncIterator, Generator, Iterable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from openknowledge_contracts.embeddings import EmbeddingMutation, ResolvedVectors
from openknowledge_core.embeddings.resolution import ResolutionPlan, ResolvedArtifact, ResolvedPart
from openknowledge_core.embeddings.serialization import vector_record_bytes

_SCHEMA = pa.schema(
    [
        pa.field("corpusid", pa.uint64(), nullable=False),
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("is_deleted", pa.bool_(), nullable=False),
        pa.field("vector", pa.list_(pa.float32()), nullable=False),
    ]
)
_BATCH_ROWS = 1024
_PART_ROWS = 262144


def _part_name(ordinal: int) -> str:
    return f"part-{ordinal:08d}.parquet"


def _file_digest(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            size += len(block)
            digest.update(block)
    return size, "sha256:" + digest.hexdigest()


def resolve_vectors(
    plan: ResolutionPlan, records: Iterable[EmbeddingMutation], destination: Path
) -> ResolvedArtifact:
    """Resolve an iterable without claiming verified source integrity/completeness.

    This low-level hook labels inputs unverified. Production callers should use
    resolve_parquet_vectors, which checks pinned archive and conversion documents.
    Equal-sequence duplicates compare Float32 values and deletion state; provenance
    derives from the bound plan and sequence, independent of shard order.
    """
    return write_resolved_artifact(plan, records, destination, "unverified-iterable")


class SQLiteResolutionWriter:
    """Incremental sink; obtain it through resolution_writer for failure cleanup."""

    def __init__(
        self,
        plan: ResolutionPlan,
        destination: Path,
        staging: Path,
        db: sqlite3.Connection,
        verification: Literal["unverified-iterable", "receipt-backed"],
        input_parquet_bytes: int | None,
    ) -> None:
        self.plan = plan
        self.destination = destination
        self.staging = staging
        self.db = db
        self.verification: Literal["unverified-iterable", "receipt-backed"] = verification
        self.input_parquet_bytes = input_parquet_bytes
        self.input_records = 0
        self.identity = plan.identity
        self.sequences = {release.sequence for release in plan.releases}
        self.finalized = False
        db.execute("PRAGMA cache_size=-4096")
        db.execute("PRAGMA temp_store=FILE")
        db.execute(
            "CREATE TABLE seen (id BLOB, sequence BLOB, digest BLOB, "
            "PRIMARY KEY (id, sequence)) WITHOUT ROWID"
        )
        db.execute(
            "CREATE TABLE latest (id BLOB PRIMARY KEY, sequence BLOB, "
            "deleted INTEGER, vector BLOB) WITHOUT ROWID"
        )

    def add(self, record: EmbeddingMutation) -> None:
        if self.finalized:
            raise ValueError("resolved writer is already finalized")
        plan, sequences, db = self.plan, self.sequences, self.db
        if record.source != plan.source or record.sequence not in sequences:
            raise ValueError("record source or sequence is outside the declared plan")
        # Binary Float32 scratch avoids canonical JSON expansion at corpus scale.
        value = struct.pack(f"<{len(record.vector)}f", *record.vector)
        key = (record.corpusid.to_bytes(8, "big"), record.sequence.to_bytes(8, "big"))
        self.input_records += 1
        state_digest = hashlib.sha256(bytes([record.is_deleted]) + value).digest()
        previous = db.execute("SELECT digest FROM seen WHERE id=? AND sequence=?", key).fetchone()
        if previous is not None:
            if previous[0] != state_digest:
                raise ValueError("conflicting equal-sequence embedding replay")
        else:
            db.execute("INSERT INTO seen VALUES (?, ?, ?)", (*key, state_digest))
            db.execute(
                "INSERT INTO latest VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
                "sequence=excluded.sequence, deleted=excluded.deleted, "
                "vector=excluded.vector "
                "WHERE excluded.sequence>latest.sequence",
                (*key, record.is_deleted, value),
            )

    def finalize(self) -> ResolvedArtifact:
        """Call only after all verified input streams have ended successfully."""
        if self.finalized:
            raise ValueError("resolved writer is already finalized")
        plan, destination, staging, db = self.plan, self.destination, self.staging, self.db
        identity, verification = self.identity, self.verification
        input_records, input_parquet_bytes = self.input_records, self.input_parquet_bytes
        digest = hashlib.sha256()
        count = live = 0
        parts: list[ResolvedPart] = []
        cursor = db.execute("SELECT id, sequence, deleted, vector FROM latest ORDER BY id")
        current = cursor.fetchone()
        while current is not None or not parts:
            part_path = staging / _part_name(len(parts))
            part_count = part_live = 0
            batch: list[dict[str, object]] = []
            with pq.ParquetWriter(part_path, _SCHEMA, compression="zstd") as writer:
                while current is not None and part_count < _PART_ROWS:
                    id_bytes, sequence_bytes, deleted, payload = current
                    sequence = int.from_bytes(sequence_bytes, "big")
                    vector = struct.unpack(f"<{len(payload) // 4}f", payload)
                    record = EmbeddingMutation(
                        source=plan.source,
                        corpusid=int.from_bytes(id_bytes, "big"),
                        sequence=sequence,
                        provenance=f"{identity}/sequence/{sequence}",
                        is_deleted=bool(deleted),
                        vector=vector,
                    )
                    encoded = vector_record_bytes(record)
                    if len(encoded) > 64 * 1024:
                        raise ValueError("canonical vector record exceeds 64 KiB")
                    digest.update(encoded)
                    count += 1
                    live += not record.is_deleted
                    part_count += 1
                    part_live += not record.is_deleted
                    batch.append(
                        {
                            "corpusid": record.corpusid,
                            "sequence": sequence,
                            "is_deleted": record.is_deleted,
                            "vector": list(vector),
                        }
                    )
                    if len(batch) == _BATCH_ROWS:
                        writer.write_table(pa.Table.from_pylist(batch, schema=_SCHEMA))
                        batch.clear()
                    current = cursor.fetchone()
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=_SCHEMA))
            size, physical_digest = _file_digest(part_path)
            parts.append(
                ResolvedPart(
                    ordinal=len(parts),
                    records=part_count,
                    live_records=part_live,
                    parquet_bytes=size,
                    sha256=physical_digest,
                )
            )
            if current is None:
                break
        db.commit()
        db.close()
        (staging / "work.sqlite").unlink()
        artifact = ResolvedArtifact(
            plan=plan,
            input_verification=verification,
            input_records=input_records,
            input_parquet_bytes=input_parquet_bytes,
            parquet_bytes=sum(part.parquet_bytes for part in parts),
            parts=tuple(parts),
            resolved=ResolvedVectors(
                source=plan.source,
                records=count,
                live_records=live,
                sha256="sha256:" + digest.hexdigest(),
                resolution_evidence=identity,
                completeness_evidence=plan.completeness_evidence,
                space_evidence=plan.space_evidence,
            ),
        )
        (staging / "manifest.json").write_bytes(artifact.model_dump_json().encode())
        for path in staging.iterdir():
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            path.chmod(0o444)
        # Exclusive reservation prevents replacing even an empty existing directory.
        # Manifest moves last, so partial output is never a finalized artifact.
        destination.mkdir()
        try:
            for part in parts:
                name = _part_name(part.ordinal)
                os.rename(staging / name, destination / name)
            os.rename(staging / "manifest.json", destination / "manifest.json")
            for directory in (destination, destination.parent):
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        except BaseException:
            shutil.rmtree(destination)
            raise
        self.finalized = True
        return artifact


@contextmanager
def resolution_writer(
    plan: ResolutionPlan,
    destination: Path,
    verification: Literal["unverified-iterable", "receipt-backed"],
    *,
    input_parquet_bytes: int | None = None,
) -> Generator[SQLiteResolutionWriter]:
    """Own the SQLite connection and remove transient scratch on every exit."""
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".resolution-", dir=destination.parent))
    try:
        with closing(sqlite3.connect(staging / "work.sqlite")) as db:
            yield SQLiteResolutionWriter(
                plan, destination, staging, db, verification, input_parquet_bytes
            )
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def write_resolved_artifact(
    plan: ResolutionPlan,
    records: Iterable[EmbeddingMutation],
    destination: Path,
    verification: Literal["unverified-iterable", "receipt-backed"],
    *,
    input_parquet_bytes: int | None = None,
) -> ResolvedArtifact:
    """Synchronous wrapper over the same incremental writer used by async inputs."""
    with resolution_writer(
        plan, destination, verification, input_parquet_bytes=input_parquet_bytes
    ) as writer:
        for record in records:
            writer.add(record)
        return writer.finalize()


def read_resolved_artifact(directory: Path) -> ResolvedArtifact:
    """Read finalized metadata; this alone does not verify Parquet content."""
    return ResolvedArtifact.model_validate_json((directory / "manifest.json").read_bytes())


def iter_resolved_vectors(
    directory: Path, expected: ResolvedArtifact
) -> Iterator[EmbeddingMutation]:
    """Verify metadata and stream canonical rows; full verification requires exhaustion.

    Consumers must not finalize a store until this iterator ends successfully.
    VectorPublicationService already enforces that rule.
    """
    if read_resolved_artifact(directory) != expected:
        raise ValueError("resolved artifact manifest differs from expected identity")
    digest = hashlib.sha256()
    count = live = 0
    previous = -1
    sequences = {r.sequence for r in expected.plan.releases}
    identity = expected.plan.identity
    for part in expected.parts:
        path = directory / _part_name(part.ordinal)
        if _file_digest(path) != (part.parquet_bytes, part.sha256):
            raise ValueError("resolved Parquet bytes differ from finalized manifest")
        part_count = part_live = 0
        with path.open("rb") as stream:
            parquet = pq.ParquetFile(stream)
            if not parquet.schema_arrow.equals(_SCHEMA, check_metadata=False):
                raise ValueError("resolved Parquet schema mismatch")
            if parquet.metadata.num_rows != part.records:
                raise ValueError("resolved Parquet row count mismatch")
            # Arrow's pinned stubs leave unused iter_batches parameters untyped.
            for batch in parquet.iter_batches(batch_size=_BATCH_ROWS):  # pyright: ignore[reportUnknownMemberType]
                for item in batch.to_pylist():
                    record = EmbeddingMutation(
                        source=expected.plan.source,
                        corpusid=item["corpusid"],
                        sequence=item["sequence"],
                        provenance=f"{identity}/sequence/{item['sequence']}",
                        is_deleted=item["is_deleted"],
                        vector=item["vector"],
                    )
                    if record.corpusid <= previous or record.sequence not in sequences:
                        raise ValueError(
                            "resolved artifact contains unordered or undeclared records"
                        )
                    previous = record.corpusid
                    encoded = vector_record_bytes(record)
                    if len(encoded) > 64 * 1024:
                        raise ValueError("canonical vector record exceeds 64 KiB")
                    digest.update(encoded)
                    count += 1
                    live += not record.is_deleted
                    part_count += 1
                    part_live += not record.is_deleted
                    if count > expected.resolved.records or live > expected.resolved.live_records:
                        raise ValueError("resolved artifact exceeds declared counts")
                    yield record
        if (
            part_count != part.records
            or part_live != part.live_records
            or _file_digest(path) != (part.parquet_bytes, part.sha256)
        ):
            raise ValueError("resolved part counts or digest mismatch")
    if (
        count != expected.resolved.records
        or live != expected.resolved.live_records
        or "sha256:" + digest.hexdigest() != expected.resolved.sha256
    ):
        raise ValueError("resolved artifact counts or digest mismatch")


def reuse_resolved_artifact(
    directory: Path, plan: ResolutionPlan, *, require_receipt_backed: bool = True
) -> ResolvedArtifact:
    """Explicit replay path: verify identity and exhaust all bytes before reuse."""
    artifact = read_resolved_artifact(directory)
    if artifact.plan != plan or (
        require_receipt_backed and artifact.input_verification != "receipt-backed"
    ):
        raise ValueError("resolved artifact differs from required plan or input verification")
    for _ in iter_resolved_vectors(directory, artifact):
        pass
    return artifact


async def stream_resolved_vectors(
    directory: Path, expected: ResolvedArtifact
) -> AsyncIterator[EmbeddingMutation]:
    """Async iterable hook shared by publication and load services."""
    for record in iter_resolved_vectors(directory, expected):
        yield record
