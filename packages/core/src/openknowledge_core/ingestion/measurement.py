"""Measurements for one explicit load, with acknowledged rows separate from unknown writes."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from openknowledge_contracts.manifests import ManifestEntry
from openknowledge_core.ingestion.commands import LoadDataset
from openknowledge_core.ingestion.models import PreparedLoad


class LoadCounters(BaseModel):
    planned_objects: int = 0
    planned_rows: int = 0
    planned_compressed_source_bytes: int | None = None
    attempted_unique_objects: int = 0
    insert_attempts: int = 0
    attempted_compressed_source_bytes: int = 0
    acknowledged_written_rows: int = 0
    attempts_without_acknowledged_row_count: int = 0


class LoadObservation:
    def __init__(self) -> None:
        self.counters = LoadCounters()
        self._objects: set[str] = set()

    async def planned(self, prepared: PreparedLoad) -> None:
        self.counters.planned_objects = len(prepared.manifest.entries)
        self.counters.planned_rows = sum(entry.records for entry in prepared.manifest.entries)
        self.counters.planned_compressed_source_bytes = sum(
            entry.size for entry in prepared.manifest.entries
        )

    async def started(self, entry: ManifestEntry) -> None:
        self._objects.add(entry.object_ref.model_dump_json())
        self.counters.attempted_unique_objects = len(self._objects)
        self.counters.insert_attempts += 1
        self.counters.attempted_compressed_source_bytes += entry.size
        self.counters.attempts_without_acknowledged_row_count += 1

    async def written(self, rows: int) -> None:
        self.counters.acknowledged_written_rows += rows
        self.counters.attempts_without_acknowledged_row_count -= 1


class LoadSnapshot(BaseModel):
    captured_at: datetime
    active_table_bytes: int | None = None
    server_memory_tracking_bytes: int | None = None
    table_running_merges: int | None = None
    table_merge_memory_bytes: int | None = None
    server_merged_rows_total: int | None = None
    server_merged_uncompressed_bytes_total: int | None = None
    errors: list[str] = Field(default_factory=list)


class LoadMeasurementReport(BaseModel):
    report_kind: Literal["load"] = "load"
    schema_version: Literal[1] = 1
    command: LoadDataset
    validation_kind: Literal["explicit-load", "synthetic-validation"]
    status: Literal["completed", "failed", "cancelled"]
    error_type: str | None = None
    elapsed_seconds: float
    acknowledged_rows_per_second: float
    counters: LoadCounters
    coverage: Literal["full-manifest", "partial-or-resumed", "not-prepared"]
    before: LoadSnapshot | None
    after: LoadSnapshot | None
    delta: dict[str, int | None]
    measurement_errors: list[str]
    metric_meanings: dict[str, str] = Field(
        default_factory=lambda: {
            "elapsed_seconds": (
                "Wall time of the existing load flow; excludes telemetry snapshots."
            ),
            "acknowledged_rows_per_second": (
                "Acknowledged source-object insert rows / elapsed seconds, including retries; "
                "unknown partial writes and snapshot removal writes excluded."
            ),
            "active_table_bytes": (
                "Active parts bytes_on_disk at each snapshot; asynchronous merges can "
                "change this independently."
            ),
            "server_memory_tracking_bytes": (
                "Server-wide MemoryTracking gauge at snapshot time; neither query nor server peak."
            ),
            "table_merge_memory_bytes": (
                "Memory of currently running merges for the selected table at snapshot "
                "time; not peak."
            ),
            "server_merged_rows_total": (
                "Server-wide cumulative merged rows; delta includes other workloads and "
                "may reset on restart."
            ),
            "server_merged_uncompressed_bytes_total": (
                "Server-wide cumulative uncompressed merged bytes; delta includes other "
                "workloads and may reset."
            ),
            "attempted_compressed_source_bytes": (
                "Manifest compressed bytes per insert attempt, including retries; not "
                "measured network traffic."
            ),
            "attempts_without_acknowledged_row_count": (
                "Failed/interrupted attempts may have written an unknown number of rows."
            ),
            "coverage": (
                "full-manifest means every planned object was attempted in a completed "
                "flow; partial-or-resumed includes checkpoint skips."
            ),
        }
    )


class IncompleteLoadMeasurementReport(BaseModel):
    report_kind: Literal["incomplete"] = "incomplete"
    schema_version: Literal[1] = 1
    command: LoadDataset
    validation_kind: Literal["explicit-load", "synthetic-validation"]
    status: Literal["completed", "failed", "cancelled"]
    error_type: str | None
    elapsed_seconds: float
    counters: LoadCounters
    measurement_errors: list[str]


type MeasurementReport = Annotated[
    LoadMeasurementReport | IncompleteLoadMeasurementReport,
    Field(discriminator="report_kind"),
]
