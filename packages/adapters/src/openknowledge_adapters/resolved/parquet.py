"""Receipt-backed local Parquet bridge using the packaged conversion contract.

Documents come from an ArtifactStore; raw rows come from the verified
ParquetArtifacts port. No implicit latest or archive listing is performed.
"""

from __future__ import annotations

from contextlib import aclosing
from pathlib import Path
from typing import Literal

from openknowledge_contracts.datasets import DatasetInventory, DiffInventory, ShardRef
from openknowledge_contracts.embedding_artifacts import ConversionManifest, ConvertedShard
from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_contracts.manifests import DatasetManifest, ShardReceipt
from openknowledge_core.embedding_conversion.ports import ParquetArtifacts
from openknowledge_core.embeddings.resolution import (
    ConversionSelection as ConversionSelection,
)
from openknowledge_core.embeddings.resolution import (
    ResolutionPlan,
    ResolvedArtifact,
)
from openknowledge_core.embeddings.resolution import (
    conversion_completeness_evidence as conversion_completeness_evidence,
)
from openknowledge_core.ingestion.archive_progress import (
    ArchiveCompletion,
    ArchiveReader,
    HashedRef,
)
from openknowledge_core.ingestion.commands import DownloadDataset, DownloadDiff
from openknowledge_core.ingestion.models import PinnedDataset
from openknowledge_core.ingestion.ports import ArtifactStore

from .store import resolution_writer


async def resolve_parquet_vectors(
    plan: ResolutionPlan,
    selections: tuple[ConversionSelection, ...],
    store: ArtifactStore,
    parquet: ParquetArtifacts,
    destination: Path,
) -> ResolvedArtifact:
    """Verify complete lineage and exhaust the conversion port's verified records.

    Completeness is relative to the explicitly selected archive inventories, whose
    original upstream completeness remains an external evidence requirement. Use
    bounded-sample scope for synthetic fixtures or partial upstream selections.
    Space/encoder compatibility is also an explicit external evidence requirement.
    """
    if len(selections) != len(plan.releases):
        raise ValueError("one archive selection is required for each declared release")
    if plan.completeness_evidence != conversion_completeness_evidence(selections, plan.scope):
        raise ValueError("completeness evidence must bind the selected archive inventories")
    reader = ArchiveReader(store)
    entries: list[tuple[int, ConvertedShard]] = []
    predecessor: HashedRef | None = None
    for release, selection in zip(plan.releases, selections, strict=True):
        if release.identity != selection.identity:
            raise ValueError("declared release identity differs from pinned conversion selection")
        completion = await reader.read(selection.completion, ArchiveCompletion)
        if (
            completion.dataset != plan.source.dataset
            or completion.release != release.release
            or completion.from_release != release.from_release
            or completion.predecessor != predecessor
            or completion.kind != ("base" if predecessor is None else "diff")
        ):
            raise ValueError("archive completion differs from declared lineage")
        parts: list[tuple[Literal["base", "updates", "deletes"], tuple[ShardRef, ...]]]
        if predecessor is None:
            inventory = await reader.read(completion.inventory, DatasetInventory)
            if (
                inventory.release_id != release.release
                or inventory.dataset.name != plan.source.dataset
            ):
                raise ValueError("base inventory differs from declared release")
            parts = [("base", inventory.shards)]
        else:
            diff = await reader.read(completion.inventory, DiffInventory)
            if (diff.from_release, diff.release_id, diff.dataset) != (
                release.from_release,
                release.release,
                plan.source.dataset,
            ):
                raise ValueError("diff inventory differs from declared lineage")
            parts = [("updates", diff.updates), ("deletes", diff.deletes)]
        # Inventory path determines the archive root; PinnedDataset validates part paths.
        inventory_path = completion.inventory.ref.path
        suffix = (
            f"{release.release}/{plan.source.dataset}/dataset.json"
            if predecessor is None
            else f"diffs/{release.from_release}/{release.release}/{plan.source.dataset}/diff.json"
        )
        if not inventory_path.name.endswith(suffix):
            raise ValueError("archive inventory path differs from lineage")
        prefix = inventory_path.name[: -len(suffix)].rstrip("/")
        command = (
            DownloadDataset(
                release=release.release,
                dataset=plan.source.dataset,
                bucket=inventory_path.bucket,
                prefix=prefix,
            )
            if release.from_release is None
            else DownloadDiff(
                release=release.release,
                from_release=release.from_release,
                dataset=plan.source.dataset,
                bucket=inventory_path.bucket,
                prefix=prefix,
            )
        )
        await reader.validate(command, selection.completion, completion)
        manifests: dict[str, tuple[HashedRef, DatasetManifest, PinnedDataset]] = {}
        nonempty = [(kind, shards) for kind, shards in parts if shards]
        if len(nonempty) != len(completion.manifests):
            raise ValueError("completion omits or adds inventory parts")
        for (kind, shards), reference in zip(nonempty, completion.manifests, strict=True):
            pinned = PinnedDataset(
                release_id=release.release,
                dataset=plan.source.dataset,
                bucket=inventory_path.bucket,
                prefix=prefix,
                shards=shards,
                from_release=release.from_release,
                part=None if kind == "base" else "updates" if kind == "updates" else "deletes",
            )
            if reference.ref.path != pinned.manifest_path:
                raise ValueError("archive manifest path differs from inventory")
            manifest = await reader.read(reference, DatasetManifest)
            if (
                manifest.dataset != plan.source.dataset
                or manifest.release_id != release.release
                or tuple(e.object_ref.path for e in manifest.entries)
                != tuple(pinned.object_path(s) for s in shards)
            ):
                raise ValueError("archive manifest differs from complete inventory")
            manifests[kind] = (reference, manifest, pinned)
        converted: dict[str, ConversionManifest] = {}
        for conversion in selection.conversions:
            manifest_ref = await reader.read(conversion, HashedRef)
            document = await reader.read(manifest_ref, ConversionManifest)
            source = document.source
            if source.kind in converted or source.kind not in {kind for kind, _ in parts}:
                raise ValueError("duplicate or unsupported converted part")
            if (
                source.completion != selection.completion.ref
                or source.completion_sha256 != selection.completion.sha256
                or source.release != release.release
                or source.from_release != release.from_release
            ):
                raise ValueError("conversion manifest differs from completed archive")
            if source.kind not in manifests:
                if (
                    source.manifest is not None
                    or source.manifest_sha256 is not None
                    or document.entries
                ):
                    raise ValueError(
                        "empty converted part must not invent a source manifest or rows"
                    )
                converted[source.kind] = document
                continue
            reference, manifest, pinned = manifests[source.kind]
            if (
                source.manifest != reference.ref
                or source.manifest_sha256 != reference.sha256
                or len(document.entries) != len(manifest.entries)
            ):
                raise ValueError("conversion manifest differs from completed archive")
            for entry, archived, shard in zip(
                document.entries, manifest.entries, pinned.shards, strict=True
            ):
                receipt = await reader.read(
                    HashedRef(ref=entry.source_receipt, sha256=entry.source_receipt_sha256),
                    ShardReceipt,
                )
                if (
                    entry.source_receipt.path != pinned.receipt_path(shard)
                    or receipt != entry.receipt.source
                    or receipt.shard != shard
                    or receipt.dataset != plan.source.dataset
                    or receipt.release_id != release.release
                    or entry.receipt.operation
                    != ("base-shard" if source.kind == "base" else source.kind)
                    or (
                        receipt.object_ref,
                        receipt.size,
                        receipt.sha256,
                        receipt.md5,
                        receipt.crc32c,
                        receipt.records,
                    )
                    != (
                        archived.object_ref,
                        archived.size,
                        archived.sha256,
                        archived.md5,
                        archived.crc32c,
                        archived.records,
                    )
                ):
                    raise ValueError("converted shard receipt differs from completed source")
                entries.append((release.sequence, entry))
            converted[source.kind] = document
        if not set(manifests).issubset(converted):
            raise ValueError("conversion omits a completed archive part")
        predecessor = selection.completion
    identity = plan.identity
    with resolution_writer(
        plan,
        destination,
        "receipt-backed",
        input_parquet_bytes=sum(entry.receipt.parquet_bytes for _, entry in entries),
    ) as writer:
        for sequence, entry in entries:
            deleted = entry.receipt.operation == "deletes"
            count = 0
            async with aclosing(parquet.read(entry)) as records:
                async for row in records:
                    count += 1
                    if row.source_line != count or (row.vector is None) != deleted:
                        raise ValueError(
                            "converted row order or deletion state differs from receipt"
                        )
                    writer.add(
                        EmbeddingMutation(
                            source=plan.source,
                            corpusid=row.corpusid,
                            sequence=sequence,
                            provenance=f"{identity}/sequence/{sequence}",
                            is_deleted=deleted,
                            vector=() if row.vector is None else tuple(row.vector),
                        )
                    )
            if count != entry.receipt.rows or count != entry.receipt.source.records:
                raise ValueError("converted row count differs from pinned source receipt")
        return writer.finalize()
