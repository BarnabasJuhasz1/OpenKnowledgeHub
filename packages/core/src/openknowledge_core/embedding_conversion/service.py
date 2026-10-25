from hashlib import sha256

from openknowledge_contracts.embedding_artifacts import (
    ConversionManifest,
    ConvertedShard,
    EmbeddingInput,
)
from openknowledge_contracts.manifests import DatasetManifest, ObjectPath, ShardReceipt
from openknowledge_core.embedding_conversion.models import (
    ConversionPlan,
    ConversionSource,
    ConvertEmbeddings,
)
from openknowledge_core.embedding_conversion.ports import ParquetArtifacts
from openknowledge_core.ingestion.archive_progress import (
    ArchiveCompletion,
    ArchiveReader,
    HashedRef,
)
from openknowledge_core.ingestion.commands import DownloadDataset, DownloadDiff
from openknowledge_core.ingestion.errors import ArchiveConflict
from openknowledge_core.ingestion.ports import ArtifactStore


def artifact_path(plan: ConversionPlan, name: str) -> ObjectPath:
    identity = sha256(plan.model_dump_json().encode()).hexdigest()
    return ObjectPath.under(
        plan.command.bucket, plan.command.prefix, "embedding-parquet", "v1", identity, name
    )


class EmbeddingConversionService:
    def __init__(self, store: ArtifactStore, parquet: ParquetArtifacts) -> None:
        self.store = store
        self.parquet = parquet
        self.reader = ArchiveReader(store)

    async def prepare(self, command: ConvertEmbeddings) -> ConversionPlan:
        selection = DownloadDataset(
            release=command.release,
            dataset=command.dataset,
            bucket=command.bucket,
            prefix=command.prefix,
        )
        entries = ()
        if command.part is None:
            completion, manifest_ref, manifest = await self.reader.completed_base(selection)
            if command.completion is not None and command.completion != completion:
                raise ArchiveConflict("configured completion differs from completed base")
        else:
            assert command.completion is not None and command.from_release is not None
            diff = DownloadDiff(
                **selection.model_dump(),
                from_release=command.from_release,
            )
            completion = command.completion
            record = await self.reader.read(completion, ArchiveCompletion)
            await self.reader.validate(diff, completion, record)
            candidates = [
                ref for ref in record.manifests if ref.ref.path.name.split("/")[-2] == command.part
            ]
            # ArchiveReader.validate has checked all nonempty inventory parts. An empty
            # selected part has no manifest, and its completed inventory proves that absence.
            manifest_ref = candidates[0] if candidates else None
            manifest = (
                await self.reader.read(manifest_ref, DatasetManifest)
                if manifest_ref is not None
                else None
            )
        sources: list[ConversionSource] = []
        if manifest is not None:
            entries = manifest.entries
        for entry in entries:
            path = ObjectPath(
                bucket=entry.object_ref.path.bucket,
                name=entry.object_ref.path.name + ".receipt.json",
            )
            reference = await self.reader.reference(path)
            receipt = await self.reader.read(reference, ShardReceipt)
            if (
                receipt.object_ref,
                receipt.size,
                receipt.sha256,
                receipt.md5,
                receipt.crc32c,
                receipt.records,
                receipt.dataset,
                receipt.release_id,
            ) != (
                entry.object_ref,
                entry.size,
                entry.sha256,
                entry.md5,
                entry.crc32c,
                entry.records,
                command.dataset,
                command.release,
            ):
                raise ArchiveConflict("source receipt disagrees with completed manifest")
            schema = f"semantic-scholar/{command.dataset}@{command.release}"
            if command.part == "deletes":
                schema += "/deletes"
            if (
                receipt.schema_ref != schema
                or receipt.shard.file_name != entry.object_ref.path.name.split("/")[-1]
            ):
                raise ArchiveConflict("source receipt schema or shard identity mismatch")
            sources.append(ConversionSource(reference=reference, receipt=receipt))
        return ConversionPlan(
            command=command,
            source=EmbeddingInput(
                kind=command.part or "base",
                from_release=command.from_release,
                release=command.release,
                completion=completion.ref,
                completion_sha256=completion.sha256,
                manifest=manifest_ref.ref if manifest_ref is not None else None,
                manifest_sha256=manifest_ref.sha256 if manifest_ref is not None else None,
            ),
            shards=tuple(sources),
        )

    async def convert_shard(self, plan: ConversionPlan, shard: int) -> ConvertedShard:
        if not 0 <= shard < len(plan.shards):
            raise ArchiveConflict("shard index outside completed manifest")
        return await self.parquet.convert(plan, shard)

    async def complete(self, plan: ConversionPlan) -> HashedRef:
        entries: list[ConvertedShard] = []
        for index, source in enumerate(plan.shards):
            ref = await self.reader.reference(artifact_path(plan, f"{index}.json"))
            entry = await self.reader.read(ref, ConvertedShard)
            if (entry.source_receipt, entry.source_receipt_sha256, entry.receipt.source) != (
                source.reference.ref,
                source.reference.sha256,
                source.receipt,
            ):
                raise ArchiveConflict("converted shard source mismatch")
            if (
                entry.receipt.operation != (plan.command.part or "base-shard")
                or entry.receipt.batch_rows != plan.command.batch_rows
                or entry.receipt.max_record_bytes != plan.command.max_record_bytes
                or entry.receipt.rows != source.receipt.records
                or entry.output.path
                != artifact_path(plan, f"{index}-{entry.receipt.parquet_sha256}.parquet")
            ):
                raise ArchiveConflict("converted shard options, rows or output path mismatch")
            await self.parquet.verify(entry)
            entries.append(entry)
        document = ConversionManifest(source=plan.source, entries=tuple(entries))
        path = artifact_path(plan, "manifest.json")
        await self.store.create(path, document.to_json(), content_type="application/json")
        reference = await self.reader.reference(path)
        if await self.reader.read(reference, ConversionManifest) != document:
            raise ArchiveConflict("conversion manifest conflicts with verified outputs")
        completion_path = artifact_path(plan, "complete.json")
        await self.store.create(
            completion_path, reference.to_json(), content_type="application/json"
        )
        result = await self.reader.reference(completion_path)
        if await self.reader.read(result, HashedRef) != reference:
            raise ArchiveConflict("conversion completion mismatch")
        return result
