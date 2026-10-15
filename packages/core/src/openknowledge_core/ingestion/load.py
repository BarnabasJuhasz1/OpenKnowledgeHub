"""Load one archived dataset of one release into its table.

An insert is not atomic: a record that breaks the contract leaves the rows before it, and a
dropped connection leaves an unknown part. Both converge, because a rerun writes the same rows
again and the table keeps one row per key and version.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime

from openknowledge_contracts.datasets import MANIFEST_NAME
from openknowledge_contracts.manifests import DatasetManifest, ManifestEntry, Producer
from openknowledge_core.ingestion.commands import LoadDataset, LoadDatasetResult
from openknowledge_core.ingestion.errors import ManifestInvalid, ObjectChanged, RowCountMismatch
from openknowledge_core.ingestion.models import (
    LoadObject,
    LoadOutcome,
    LoadRecord,
    PreparedLoad,
    dataset_file_path,
)
from openknowledge_core.ingestion.ports import ArtifactStore, TableLoader
from openknowledge_core.ingestion.service import DOCUMENT_LIMIT, read_small


class LoadDatasetService:
    def __init__(
        self,
        *,
        store: ArtifactStore,
        tables: TableLoader,
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> None:
        self._store = store
        self._tables = tables
        self._producer = producer
        self._clock = clock

    async def prepare(self, command: LoadDataset) -> PreparedLoad:
        path = dataset_file_path(
            command.bucket, command.prefix, command.release, command.dataset, MANIFEST_NAME
        )
        stored = await self._store.stat(path)
        if stored is None:
            raise ManifestInvalid(f"{path} does not exist; run okp s2 download first")
        data = await read_small(self._store, stored, DOCUMENT_LIMIT)
        try:
            manifest = DatasetManifest.from_json(data or b"")
        except ValueError:
            raise ManifestInvalid(f"{path} is not a valid manifest") from None
        if (manifest.release_id, manifest.dataset) != (command.release, command.dataset):
            raise ManifestInvalid(f"{path} is the manifest of another release or dataset")
        table = await self._tables.table(command.dataset)
        return PreparedLoad(manifest_path=path, manifest=manifest, table=table)

    async def load_object(self, prepared: PreparedLoad, entry: ManifestEntry) -> LoadOutcome:
        manifest, table = prepared.manifest, prepared.table
        path = entry.object_ref.path
        record = await self._tables.loaded(manifest.dataset, path)
        if table.has_rows and record is not None and _current(record, entry, prepared):
            return LoadOutcome.ALREADY_LOADED
        stored = await self._store.stat(path)
        if stored is None or stored.ref != entry.object_ref:
            raise ObjectChanged(
                f"{path} is gone or no longer the generation {prepared.manifest_path} names; "
                "rerun okp s2 download, which checks it against its receipt"
            )
        written = await self._tables.load(
            LoadObject(
                dataset=manifest.dataset, release=manifest.release_id, object_ref=entry.object_ref
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
                records=entry.records,
                written_rows=written,
                load_sql_sha256=table.load_sql_sha256,
                software_version=self._producer.software_version,
                image_digest=self._producer.image_digest,
                loaded_at=self._clock(),
            )
        )
        return LoadOutcome.LOADED

    async def finish(
        self, prepared: PreparedLoad, outcomes: Iterable[LoadOutcome]
    ) -> LoadDatasetResult:
        """A base release holds each key once, so its live rows must equal its records."""
        manifest = prepared.manifest
        records = sum(entry.records for entry in manifest.entries)
        live = await self._tables.live_rows(manifest.dataset, manifest.release_id)
        if live != records:
            raise RowCountMismatch(
                f"{prepared.table.name} holds {live} live rows of {manifest.release_id}, but "
                f"{prepared.manifest_path} counts {records} records, so keys repeat in the release"
            )
        counts = Counter(outcomes)
        return LoadDatasetResult(
            release_id=manifest.release_id,
            dataset=manifest.dataset,
            table=prepared.table.name,
            objects=len(manifest.entries),
            loaded=counts[LoadOutcome.LOADED],
            already_loaded=counts[LoadOutcome.ALREADY_LOADED],
            live_rows=live,
            manifest=prepared.manifest_path,
        )


def _current(record: LoadRecord, entry: ManifestEntry, prepared: PreparedLoad) -> bool:
    """A record counts only for this object, this table and the load SQL of this contract."""
    table = prepared.table
    return (
        record.target_uuid == table.uuid
        and record.load_sql_sha256 == table.load_sql_sha256
        and record.generation == entry.object_ref.generation
        and record.sha256 == entry.sha256
    )
