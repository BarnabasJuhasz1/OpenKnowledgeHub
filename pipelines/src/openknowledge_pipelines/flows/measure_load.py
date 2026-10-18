"""Measure the existing explicit load flow without changing its exception or checkpoints."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Literal, TextIO

from openknowledge_core.ingestion.commands import LoadDataset, LoadDatasetResult
from openknowledge_core.ingestion.measurement import (
    IncompleteLoadMeasurementReport,
    LoadMeasurementReport,
    LoadObservation,
    LoadSnapshot,
)
from openknowledge_core.ingestion.ports import LoadTelemetry

DELTA_FIELDS = (
    "active_table_bytes",
    "server_memory_tracking_bytes",
    "table_running_merges",
    "table_merge_memory_bytes",
    "server_merged_rows_total",
    "server_merged_uncompressed_bytes_total",
)


async def _snapshot(
    telemetry: LoadTelemetry, dataset: str, errors: list[str]
) -> LoadSnapshot | None:
    try:
        async with asyncio.timeout(10):
            snapshot = await telemetry.snapshot(dataset)
        errors.extend(snapshot.errors)
        return snapshot
    except Exception as exc:
        # Optional telemetry cannot prevent a load or replace its failure.
        errors.append(f"snapshot: {type(exc).__name__}")
        return None


async def measured_load(
    run: Callable[[], Awaitable[LoadDatasetResult]],
    *,
    command: LoadDataset,
    observation: LoadObservation,
    telemetry: LoadTelemetry,
    report_file: TextIO,
    validation_kind: Literal["explicit-load", "synthetic-validation"],
    warn: Callable[[str], None],
) -> LoadDatasetResult:
    errors: list[str] = []
    before = await _snapshot(telemetry, command.dataset, errors)
    start = perf_counter()
    try:
        return await run()
    finally:
        elapsed = perf_counter() - start
        primary = sys.exception()
        try:
            after = await _snapshot(telemetry, command.dataset, errors)
            delta: dict[str, int | None] = {}
            for field in DELTA_FIELDS:
                first = getattr(before, field) if before is not None else None
                last = getattr(after, field) if after is not None else None
                difference = last - first if first is not None and last is not None else None
                if field.endswith("_total") and difference is not None and difference < 0:
                    errors.append(f"{field}: counter reset; delta unavailable")
                    difference = None
                delta[field] = difference
            counts = observation.counters
            report = LoadMeasurementReport(
                command=command,
                validation_kind=validation_kind,
                status=(
                    "cancelled"
                    if isinstance(primary, asyncio.CancelledError)
                    else "failed"
                    if primary is not None
                    else "completed"
                ),
                error_type=type(primary).__name__ if primary is not None else None,
                elapsed_seconds=elapsed,
                acknowledged_rows_per_second=(
                    counts.acknowledged_written_rows / elapsed if elapsed else 0
                ),
                counters=counts,
                coverage=(
                    "not-prepared"
                    if counts.planned_compressed_source_bytes is None
                    else "full-manifest"
                    if primary is None and counts.planned_objects == counts.attempted_unique_objects
                    else "partial-or-resumed"
                ),
                before=before,
                after=after,
                delta=delta,
                measurement_errors=errors,
            )
            try:
                report_file.write(report.model_dump_json(indent=2) + "\n")
                report_file.flush()
            except OSError as exc:
                warn(
                    f"Measurement report could not be written ({type(exc).__name__}); "
                    f"load status is {report.status}."
                )
            if errors:
                warn("Load measurement is partial: " + "; ".join(errors))
        except (Exception, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError) and primary is None:
                raise
            # This boundary covers optional reporting only, never the load itself.
            try:
                warn(f"Load measurement report is incomplete ({type(exc).__name__}).")
            except Exception:
                # Failed secondary diagnostics must not replace the primary outcome.
                pass
            try:
                fallback = IncompleteLoadMeasurementReport(
                    command=command,
                    validation_kind=validation_kind,
                    status=(
                        "cancelled"
                        if isinstance(primary, asyncio.CancelledError)
                        else "failed"
                        if primary is not None
                        else "completed"
                    ),
                    error_type=type(primary).__name__ if primary is not None else None,
                    elapsed_seconds=elapsed,
                    counters=observation.counters,
                    measurement_errors=[*errors, f"report finalization: {type(exc).__name__}"],
                )
                report_file.seek(0)
                report_file.truncate()
                report_file.write(fallback.model_dump_json() + "\n")
                report_file.flush()
            except Exception:
                # The original reporting failure was already sent to diagnostics.
                pass
