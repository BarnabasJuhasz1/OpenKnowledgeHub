"""Load one archived dataset of one release, or its diff to that release, into its table.

An insert is not atomic: a record that breaks the contract leaves the rows before it, and a
dropped connection leaves an unknown part. Both converge, because a rerun writes the same rows
again and the table keeps one row per key and version.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime

from openknowledge_contracts.datasets import (
    DIFF_INVENTORY_NAME,
    MANIFEST_NAME,
    ArchiveDocument,
    DatasetName,
    DiffInventory,
    DiffPart,
    ReleaseId,
)
from openknowledge_contracts.manifests import DatasetManifest, ManifestEntry, ObjectPath, Producer
from openknowledge_core.ingestion.commands import (
    LoadDataset,
    LoadDatasetResult,
    LoadDiff,
    LoadDiffResult,
)
from openknowledge_core.ingestion.errors import (
    BaseRefused,
    ConflictingKeys,
    DiffRefused,
    ManifestInvalid,
    ObjectChanged,
    RowCountMismatch,
)
from openknowledge_core.ingestion.models import (
    AppliedRelease,
    LoadObject,
    LoadOutcome,
    LoadRecord,
    PreparedDiff,
    PreparedLoad,
    TableState,
    dataset_file_path,
    diff_file_path,
    held_release,
)
from openknowledge_core.ingestion.ports import AppliedReleases, ArtifactStore, TableLoader
from openknowledge_core.ingestion.service import DOCUMENT_LIMIT, read_small

BASE_HINT = "run okp s2 download first"
DIFF_HINT = "run s2-batch-download-diff first"


class LoadDatasetService:
    def __init__(
        self,
        *,
        store: ArtifactStore,
        tables: TableLoader,
        applied: AppliedReleases,
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> None:
        self._store = store
        self._tables = tables
        self._applied = applied
        self._producer = producer
        self._clock = clock

    async def prepare(self, command: LoadDataset) -> PreparedLoad:
        """A table that holds a release takes only that base again, or a newer snapshot."""
        table = await self._tables.table(command.dataset)
        held = held_release(table, await self._applied.latest(table.name))
        if held is not None and (
            command.release < held or (table.updates == "diffs" and command.release != held)
        ):
            raise BaseRefused(
                f"{table.name} holds {held}, so it cannot take the base {command.release}; "
                "a different base needs an empty or new table"
            )
        path = dataset_file_path(
            command.bucket, command.prefix, command.release, command.dataset, MANIFEST_NAME
        )
        manifest = await self._manifest(path, command.release, command.dataset, BASE_HINT)
        return PreparedLoad(manifest_path=path, manifest=manifest, table=table)

    async def prepare_diff(self, command: LoadDiff) -> PreparedDiff:
        """A table takes a diff from the release it holds, or again once it holds the diff's."""
        table = await self._tables.table(command.dataset)
        if table.updates == "snapshot":
            raise DiffRefused(
                f"{table.name} loads every release of {command.dataset} whole, never a diff; "
                "load the release with okp s2 load"
            )
        held = held_release(table, await self._applied.latest(table.name))
        if held not in (command.from_release, command.release):
            raise DiffRefused(
                f"{table.name} holds {held or 'no recorded release'}, so it cannot take the "
                f"diff from {command.from_release} to {command.release}"
            )
        path = diff_file_path(
            command.bucket,
            command.prefix,
            command.from_release,
            command.release,
            command.dataset,
            DIFF_INVENTORY_NAME,
        )
        inventory = await self._read(path, DiffInventory, DIFF_HINT)
        if (inventory.from_release, inventory.release_id, inventory.dataset) != (
            command.from_release,
            command.release,
            command.dataset,
        ):
            raise ManifestInvalid(f"{path} is the inventory of another diff or dataset")
        updates = await self._part(command, table, "updates", len(inventory.updates))
        deletes = await self._part(command, table, "deletes", len(inventory.deletes))
        return PreparedDiff(
            from_release=command.from_release,
            release_id=command.release,
            dataset=command.dataset,
            table=table,
            inventory_path=path,
            updates=updates,
            deletes=deletes,
            identical_duplicates=0 if updates is None else await self._repeated(updates),
        )

    async def load_object(self, prepared: PreparedLoad, entry: ManifestEntry) -> LoadOutcome:
        manifest, table, operation = prepared.manifest, prepared.table, prepared.operation
        path = entry.object_ref.path
        record = await self._tables.loaded(manifest.dataset, path)
        if table.has_rows and record is not None and _current(record, entry, prepared):
            return LoadOutcome.ALREADY_LOADED
        stored = await self._store.stat(path)
        if stored is None or stored.ref != entry.object_ref:
            raise ObjectChanged(
                f"{path} is gone or no longer the generation {prepared.manifest_path} names; "
                f"rerun the download that wrote {prepared.manifest_path}, which checks it "
                "against its receipt"
            )
        written = await self._tables.load(
            LoadObject(
                dataset=manifest.dataset,
                release=manifest.release_id,
                object_ref=entry.object_ref,
                operation=operation,
            )
        )
        if written != entry.records:
            raise RowCountMismatch(
                f"loading {path} wrote {written} rows, but its receipt counts {entry.records} "
                "records; it stays unloaded"
            )
        await self._tables.record(
            LoadRecord(
                target=table.name,
                target_uuid=table.uuid,
                source_object=str(path),
                generation=entry.object_ref.generation,
                sha256=entry.sha256,
                source_release=manifest.release_id,
                operation=operation,
                records=entry.records,
                written_rows=written,
                load_sql_sha256=table.sql_sha256(operation),
                software_version=self._producer.software_version,
                image_digest=self._producer.image_digest,
                loaded_at=self._clock(),
            )
        )
        return LoadOutcome.LOADED

    async def finish(
        self, prepared: PreparedLoad, outcomes: Iterable[LoadOutcome]
    ) -> LoadDatasetResult:
        """A base release holds each key once, so its live rows must equal its records.

        Only when they differ are the objects read again, to tell identical repeats, which are
        counted, from conflicting ones. Only a release that passes flags the keys it no longer
        lists in a snapshot table and is recorded as the release the table holds.
        """
        manifest, table = prepared.manifest, prepared.table
        records = _record_count(prepared)
        live = await self._tables.live_rows(manifest.dataset, manifest.release_id)
        identical = 0 if live == records else await self._identical(prepared, records, live)
        removed = 0
        if table.updates == "snapshot":
            removed = await self._tables.mark_removed(manifest.dataset, manifest.release_id)
        await self._record(table, manifest.release_id, None)
        counts = Counter(outcomes)
        return LoadDatasetResult(
            release_id=manifest.release_id,
            dataset=manifest.dataset,
            table=table.name,
            objects=len(manifest.entries),
            loaded=counts[LoadOutcome.LOADED],
            already_loaded=counts[LoadOutcome.ALREADY_LOADED],
            live_rows=live,
            identical_duplicates=identical,
            removed=removed,
            manifest=prepared.manifest_path,
        )

    async def finish_diff(
        self, prepared: PreparedDiff, outcomes: Iterable[LoadOutcome]
    ) -> LoadDiffResult:
        """The table is never counted, so every object of the diff must have an outcome."""
        counts = Counter(outcomes)
        objects = sum(len(part.manifest.entries) for part in prepared.parts)
        if counts.total() != objects:
            raise RowCountMismatch(
                f"the diff from {prepared.from_release} to {prepared.release_id} has {objects} "
                f"objects, but {counts.total()} were loaded or found loaded; nothing is recorded"
            )
        await self._record(prepared.table, prepared.release_id, prepared.from_release)
        return LoadDiffResult(
            from_release=prepared.from_release,
            release_id=prepared.release_id,
            dataset=prepared.dataset,
            table=prepared.table.name,
            objects=objects,
            loaded=counts[LoadOutcome.LOADED],
            already_loaded=counts[LoadOutcome.ALREADY_LOADED],
            upserted=_record_count(prepared.updates),
            deleted=_record_count(prepared.deletes),
            identical_duplicates=prepared.identical_duplicates,
            manifests=tuple(part.manifest_path for part in prepared.parts),
        )

    async def _part(
        self, command: LoadDiff, table: TableState, part: DiffPart, files: int
    ) -> PreparedLoad | None:
        if not files:
            return None
        path = diff_file_path(
            command.bucket,
            command.prefix,
            command.from_release,
            command.release,
            command.dataset,
            part,
            MANIFEST_NAME,
        )
        manifest = await self._manifest(path, command.release, command.dataset, DIFF_HINT)
        if len(manifest.entries) != files:
            raise ManifestInvalid(
                f"{path} lists {len(manifest.entries)} objects, but the diff's "
                f"{DIFF_INVENTORY_NAME} lists {files} files for {part}"
            )
        return PreparedLoad(
            manifest_path=path,
            manifest=manifest,
            table=table,
            operation="delete" if part == "deletes" else "upsert",
        )

    async def _manifest(
        self, path: ObjectPath, release: ReleaseId, dataset: DatasetName, missing: str
    ) -> DatasetManifest:
        manifest = await self._read(path, DatasetManifest, missing)
        if (manifest.release_id, manifest.dataset) != (release, dataset):
            raise ManifestInvalid(f"{path} is the manifest of another release or dataset")
        return manifest

    async def _read[D: ArchiveDocument](self, path: ObjectPath, kind: type[D], missing: str) -> D:
        stored = await self._store.stat(path)
        if stored is None:
            raise ManifestInvalid(f"{path} does not exist; {missing}")
        data = await read_small(self._store, stored, DOCUMENT_LIMIT)
        try:
            return kind.from_json(data or b"")
        except ValueError:
            raise ManifestInvalid(f"{path} is not a valid {kind.__name__}") from None

    async def _identical(self, prepared: PreparedLoad, records: int, live: int) -> int:
        """The records that repeat a key with its values, which must be every row not live."""
        identical = await self._repeated(prepared)
        if identical != records - live:
            raise RowCountMismatch(
                f"{prepared.table.name} holds {live} live rows of {prepared.manifest.release_id}, "
                f"but {prepared.manifest_path} counts {records} records, and {identical} of them "
                "repeat a key identically"
            )
        return identical

    async def _repeated(self, prepared: PreparedLoad) -> int:
        """The records that repeat a key with its values; a key repeated with others fails."""
        manifest, path = prepared.manifest, prepared.manifest_path
        found = await self._tables.duplicates(
            manifest.dataset, [entry.object_ref.path for entry in manifest.entries]
        )
        if found.conflicting_keys:
            raise ConflictingKeys(
                f"keys repeat in {path} with other values ({found.conflicting_keys}, such as "
                f"{', '.join(found.examples)}); the row each keeps depends on load order"
            )
        return found.identical_rows

    async def _record(
        self, table: TableState, release: ReleaseId, from_release: ReleaseId | None
    ) -> None:
        await self._applied.record(
            AppliedRelease(
                target=table.name,
                target_uuid=table.uuid,
                release=release,
                from_release=from_release,
                software_version=self._producer.software_version,
                image_digest=self._producer.image_digest,
                applied_at=self._clock(),
            )
        )


def _current(record: LoadRecord, entry: ManifestEntry, prepared: PreparedLoad) -> bool:
    """A record counts only for this object, this table and the SQL of this operation."""
    table, operation = prepared.table, prepared.operation
    return (
        record.target_uuid == table.uuid
        and record.operation == operation
        and record.load_sql_sha256 == table.sql_sha256(operation)
        and record.generation == entry.object_ref.generation
        and record.sha256 == entry.sha256
    )


def _record_count(prepared: PreparedLoad | None) -> int:
    return 0 if prepared is None else sum(entry.records for entry in prepared.manifest.entries)
