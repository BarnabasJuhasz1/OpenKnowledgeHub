"""Bounded disk staging over generation-pinned archive storage."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from openknowledge_adapters.parquet.local import convert, digest, parquet_schema
from openknowledge_contracts.embedding_artifacts import ConvertedShard, EmbeddingArtifactRow
from openknowledge_contracts.manifests import ObjectRef
from openknowledge_core.embedding_conversion.models import ConversionPlan
from openknowledge_core.embedding_conversion.service import artifact_path
from openknowledge_core.ingestion.archive_progress import ArchiveReader
from openknowledge_core.ingestion.errors import ArchiveConflict
from openknowledge_core.ingestion.models import CheckedStream
from openknowledge_core.ingestion.ports import ArtifactStore


class StoredParquetArtifacts:
    def __init__(self, store: ArtifactStore) -> None:
        self.store = store
        self.reader = ArchiveReader(store)

    async def _download(self, ref: ObjectRef, path: Path, size: int) -> None:
        stream = await self.store.open_read(ref)
        count = 0
        with path.open("wb") as target:
            async with aclosing(stream.chunks) as chunks:
                async for chunk in chunks:
                    count += len(chunk)
                    if count > size:
                        raise ArchiveConflict("artifact exceeds recorded size")
                    target.write(chunk)
        if count != size:
            raise ArchiveConflict("artifact size mismatch")

    async def verify(self, artifact: ConvertedShard) -> None:
        # Stream verification requires no second full disk copy.
        stream = await self.store.open_read(artifact.output)
        check = sha256()
        count = 0
        async with aclosing(stream.chunks) as chunks:
            async for chunk in chunks:
                count += len(chunk)
                if count > artifact.receipt.parquet_bytes:
                    raise ArchiveConflict("Parquet exceeds recorded size")
                check.update(chunk)
        if (count, check.hexdigest()) != (
            artifact.receipt.parquet_bytes,
            artifact.receipt.parquet_sha256,
        ):
            raise ArchiveConflict("Parquet bytes disagree with receipt")

    async def read(self, artifact: ConvertedShard) -> AsyncGenerator[EmbeddingArtifactRow]:
        """Verify pinned bytes, exact schema and rows before streaming bounded raw batches.

        Caller uses aclosing when stopping early. Duplicates are retained; this reader
        neither checks same-sequence agreement nor applies updates or tombstones.
        """
        with TemporaryDirectory(prefix="embedding-read-") as directory:
            path = Path(directory) / "vectors.parquet"
            await self._download(artifact.output, path, artifact.receipt.parquet_bytes)
            if digest(path) != artifact.receipt.parquet_sha256:
                raise ArchiveConflict("Parquet bytes disagree with receipt")
            try:
                with pq.ParquetFile(path) as parquet:
                    schema = parquet_schema(artifact.receipt.source, artifact.receipt.operation)
                    if (
                        not parquet.schema_arrow.equals(schema, check_metadata=True)
                        or parquet.metadata.num_rows != artifact.receipt.rows
                        or artifact.receipt.rows != artifact.receipt.source.records
                    ):
                        raise ArchiveConflict("Parquet schema or row count disagrees with receipt")
                    line = 0
                    # Pinned Arrow stubs leave unused row_groups/columns untyped.
                    for batch in parquet.iter_batches(  # pyright: ignore[reportUnknownMemberType]
                        batch_size=artifact.receipt.batch_rows
                    ):
                        for value in batch.to_pylist():
                            row = EmbeddingArtifactRow.model_validate(value)
                            line += 1
                            if row.source_line != line:
                                raise ArchiveConflict("Parquet source order is invalid")
                            yield row
                        await asyncio.sleep(0)
            except (pa.ArrowException, ValidationError):
                raise ArchiveConflict("invalid raw embeddings Parquet") from None

    async def convert(self, plan: ConversionPlan, shard: int) -> ConvertedShard:
        source = plan.shards[shard]
        path = artifact_path(plan, f"{shard}.json")
        prior = await self.store.stat(path)
        if prior is not None:
            result = await self.reader.read(await self.reader.reference(path), ConvertedShard)
            if (
                result.source_receipt,
                result.source_receipt_sha256,
                result.receipt.source,
                result.receipt.operation,
                result.receipt.batch_rows,
                result.receipt.max_record_bytes,
            ) != (
                source.reference.ref,
                source.reference.sha256,
                source.receipt,
                plan.command.part or "base-shard",
                plan.command.batch_rows,
                plan.command.max_record_bytes,
            ):
                raise ArchiveConflict("existing converted shard conflicts with plan")
            await self.verify(result)
            return result
        with TemporaryDirectory(prefix="embedding-conversion-") as directory:
            root = Path(directory)
            raw = root / "source.gz"
            receipt_path = root / "source.json"
            await self._download(source.receipt.object_ref, raw, source.receipt.size)
            receipt_path.write_bytes(source.receipt.to_json())
            worker = asyncio.create_task(
                asyncio.to_thread(
                    convert,
                    raw,
                    receipt_path,
                    root / "output",
                    batch_rows=plan.command.batch_rows,
                    max_record_bytes=plan.command.max_record_bytes,
                    operation=plan.command.part or "base-shard",
                )
            )
            try:
                receipt = await asyncio.shield(worker)
            except asyncio.CancelledError:
                # The synchronous writer still owns its staging directory until it ends.
                await worker
                raise
            output = root / "output" / "vectors.parquet"
            body: CheckedStream

            async def chunks() -> AsyncGenerator[bytes]:
                with output.open("rb") as stream:
                    while block := stream.read(256 * 1024):
                        yield block
                        await asyncio.sleep(0)
                body.completed = True

            body = CheckedStream(size=receipt.parquet_bytes, chunks=chunks())
            output_path = artifact_path(plan, f"{shard}-{receipt.parquet_sha256}.parquet")
            async with aclosing(body.chunks):
                await self.store.create_from_stream(
                    output_path, body, content_type="application/vnd.apache.parquet"
                )
            stored = await self.store.stat(output_path)
            if stored is None or digest(output) != receipt.parquet_sha256:
                raise ArchiveConflict("Parquet publication failed")
            result = ConvertedShard(
                source_receipt=source.reference.ref,
                source_receipt_sha256=source.reference.sha256,
                output=stored.ref,
                receipt=receipt,
            )
            await self.verify(result)
            await self.store.create(path, result.to_json(), content_type="application/json")
            existing = await self.reader.read(await self.reader.reference(path), ConvertedShard)
            # Concurrent attempts may differ in elapsed time but must pin identical bytes.
            if existing.model_dump(exclude={"receipt": {"elapsed_seconds"}}) != result.model_dump(
                exclude={"receipt": {"elapsed_seconds"}}
            ):
                raise ArchiveConflict("concurrent converted shard conflicts")
            await self.verify(existing)
            return existing
