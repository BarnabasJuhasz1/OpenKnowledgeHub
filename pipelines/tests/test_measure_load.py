import asyncio
import io
import json
from typing import Literal

import pytest
from pydantic import TypeAdapter

from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.ingestion import (
    DownloadDatasetService,
    LoadDataset,
    LoadDatasetResult,
    LoadDatasetService,
)
from openknowledge_core.ingestion.errors import TableRejected, TableUnavailable
from openknowledge_core.ingestion.measurement import (
    LoadObservation,
    LoadSnapshot,
    MeasurementReport,
)
from openknowledge_pipelines.flows.measure_load import measured_load
from tests.fakes.ports import (
    COMMAND,
    PAPERS,
    PRODUCER,
    FakeApplied,
    FakeSource,
    FakeStore,
    FakeTables,
    clock,
)


class Telemetry:
    def __init__(self, broken: bool) -> None:
        self.calls = 0
        self.broken = broken

    async def snapshot(self, dataset: str) -> LoadSnapshot:
        self.calls += 1
        if self.broken:
            raise TableUnavailable("telemetry unavailable")
        return LoadSnapshot(
            captured_at=clock(),
            active_table_bytes=100 * self.calls,
            server_memory_tracking_bytes=1000,
            table_running_merges=0,
            table_merge_memory_bytes=0,
            server_merged_rows_total=10,
            server_merged_uncompressed_bytes_total=200,
        )


@pytest.mark.parametrize("scenario", ["success", "replay", "failed", "telemetry-failed"])
async def test_measurement_preserves_real_loader_semantics(
    scenario: Literal["success", "replay", "failed", "telemetry-failed"],
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
) -> None:
    for name, data in PAPERS.items():
        source.add("papers", name, data)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    await service.write_manifest(pinned)
    observation = LoadObservation()
    tables, applied = FakeTables(store), FakeApplied()
    loader = LoadDatasetService(
        store=store,
        tables=tables,
        applied=applied,
        producer=PRODUCER,
        clock=clock,
        observer=observation,
    )
    command = LoadDataset(**COMMAND.model_dump())

    async def run() -> LoadDatasetResult:
        prepared = await loader.prepare(command)
        outcomes = [
            await loader.load_object(prepared, entry) for entry in prepared.manifest.entries
        ]
        return await loader.finish(prepared, outcomes)

    if scenario == "replay":
        await run()
        observation = LoadObservation()
        loader = LoadDatasetService(
            store=store,
            tables=tables,
            applied=applied,
            producer=PRODUCER,
            clock=clock,
            observer=observation,
        )
    failure = TableRejected("synthetic insert failure")
    if scenario == "failed":
        tables.failures.append(failure)
    report_file = io.StringIO()
    warnings: list[str] = []
    telemetry = Telemetry(scenario == "telemetry-failed")
    if scenario == "failed":
        with pytest.raises(TableRejected) as raised:
            await measured_load(
                run,
                command=command,
                observation=observation,
                telemetry=telemetry,
                report_file=report_file,
                validation_kind="synthetic-validation",
                warn=warnings.append,
            )
        assert raised.value is failure
        assert applied.records == []
    else:
        result = await measured_load(
            run,
            command=command,
            observation=observation,
            telemetry=telemetry,
            report_file=report_file,
            validation_kind="synthetic-validation",
            warn=warnings.append,
        )
        assert result.already_loaded == (2 if scenario == "replay" else 0)
        assert len(applied.records) == (2 if scenario == "replay" else 1)
    report = json.loads(report_file.getvalue())
    counters = report["counters"]
    assert telemetry.calls == 2
    assert report["elapsed_seconds"] > 0
    assert counters["planned_compressed_source_bytes"] == sum(map(len, PAPERS.values()))
    assert counters["acknowledged_written_rows"] == (0 if scenario in ("failed", "replay") else 51)
    assert report["status"] == ("failed" if scenario == "failed" else "completed")
    assert report["coverage"] == (
        "partial-or-resumed" if scenario in ("failed", "replay") else "full-manifest"
    )
    if scenario == "telemetry-failed":
        assert report["before"] is None and report["after"] is None
        assert report["measurement_errors"] and warnings
    else:
        assert report["delta"]["active_table_bytes"] == 100
    if scenario == "failed":
        assert counters["attempts_without_acknowledged_row_count"] == 1


async def test_report_io_failure_cannot_replace_load_failure() -> None:
    class BrokenReport(io.StringIO):
        def write(self, s: str) -> int:
            raise OSError("disk unavailable")

    failure = TableRejected("primary insert failure")

    async def run() -> LoadDatasetResult:
        raise failure

    warnings: list[str] = []
    with pytest.raises(TableRejected) as raised:
        await measured_load(
            run,
            command=LoadDataset(**COMMAND.model_dump()),
            observation=LoadObservation(),
            telemetry=Telemetry(broken=True),
            report_file=BrokenReport(),
            validation_kind="synthetic-validation",
            warn=warnings.append,
        )
    assert raised.value is failure
    assert any("report could not be written" in warning for warning in warnings)


async def test_unexpected_telemetry_failure_does_not_replace_primary() -> None:
    class BrokenTelemetry:
        async def snapshot(self, dataset: str) -> LoadSnapshot:
            raise RuntimeError("unexpected telemetry failure")

    failure = TableRejected("primary load failure")

    async def run() -> LoadDatasetResult:
        raise failure

    report = io.StringIO()
    warnings: list[str] = []
    with pytest.raises(TableRejected) as raised:
        await measured_load(
            run,
            command=LoadDataset(**COMMAND.model_dump()),
            observation=LoadObservation(),
            telemetry=BrokenTelemetry(),
            report_file=report,
            validation_kind="synthetic-validation",
            warn=warnings.append,
        )
    assert raised.value is failure
    partial = json.loads(report.getvalue())
    assert partial["status"] == "failed"
    assert partial["measurement_errors"] == ["snapshot: RuntimeError"] * 2
    assert warnings


async def test_serialization_failure_preserves_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openknowledge_core.ingestion.measurement import LoadMeasurementReport

    def broken_json(self: LoadMeasurementReport, **kwargs: object) -> str:
        raise RuntimeError("serialization failure")

    monkeypatch.setattr(LoadMeasurementReport, "model_dump_json", broken_json)
    cancellation = asyncio.CancelledError()

    async def run() -> LoadDatasetResult:
        raise cancellation

    report = io.StringIO()
    warnings: list[str] = []
    with pytest.raises(asyncio.CancelledError) as raised:
        await measured_load(
            run,
            command=LoadDataset(**COMMAND.model_dump()),
            observation=LoadObservation(),
            telemetry=Telemetry(broken=False),
            report_file=report,
            validation_kind="synthetic-validation",
            warn=warnings.append,
        )
    assert raised.value is cancellation
    typed = TypeAdapter(MeasurementReport).validate_json(report.getvalue())
    assert typed.report_kind == "incomplete"
    fallback = json.loads(report.getvalue())
    assert fallback["status"] == "cancelled"
    assert fallback["measurement_errors"] == ["report finalization: RuntimeError"]
    assert warnings


@pytest.mark.parametrize("status", ["failed", "cancelled", "completed"])
async def test_cancellation_during_final_snapshot_preserves_primary(status: str) -> None:
    entered = asyncio.Event()

    class WaitingTelemetry(Telemetry):
        async def snapshot(self, dataset: str) -> LoadSnapshot:
            if self.calls:
                entered.set()
                await asyncio.Event().wait()
            return await super().snapshot(dataset)

    failure = (
        TableRejected("primary load failure")
        if status == "failed"
        else asyncio.CancelledError()
        if status == "cancelled"
        else None
    )

    async def run() -> LoadDatasetResult:
        if failure is not None:
            raise failure
        return LoadDatasetResult(
            release_id=COMMAND.release,
            dataset=COMMAND.dataset,
            table="papers",
            objects=0,
            loaded=0,
            already_loaded=0,
            live_rows=0,
            identical_duplicates=0,
            removed=0,
            manifest=ObjectPath.under(COMMAND.bucket, COMMAND.prefix, "manifest.json"),
        )

    report = io.StringIO()
    task = asyncio.create_task(
        measured_load(
            run,
            command=LoadDataset(**COMMAND.model_dump()),
            observation=LoadObservation(),
            telemetry=WaitingTelemetry(broken=False),
            report_file=report,
            validation_kind="synthetic-validation",
            warn=lambda message: None,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(type(failure) if failure is not None else asyncio.CancelledError) as raised:
        await task
    if failure is None:
        assert report.getvalue() == ""
        return
    assert raised.value is failure
    typed = TypeAdapter(MeasurementReport).validate_json(report.getvalue())
    assert typed.status == status
