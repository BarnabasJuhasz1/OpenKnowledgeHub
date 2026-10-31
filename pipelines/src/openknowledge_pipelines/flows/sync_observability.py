"""Bounded diagnostics; never publish settings, source records or exception messages."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from logging import Logger, LoggerAdapter, getLogger
from time import monotonic
from typing import Literal

from prefect import get_run_logger
from prefect.artifacts import MarkdownArtifact
from prefect.context import FlowRunContext
from prefect.exceptions import MissingContextError, TerminationSignal
from pydantic import BaseModel, Field

from openknowledge_contracts.manifests import Producer
from openknowledge_core.ingestion.coordinator import SyncResult

HEARTBEAT_SECONDS = 300
ARTIFACT_TIMEOUT_SECONDS = 10
Status = Literal["completed", "failed", "interrupted"]


def _status(error: BaseException | None) -> Status:
    if isinstance(error, (asyncio.CancelledError, TerminationSignal)):
        # In async flows Prefect delivers SIGTERM to user code as a cancellation, so the
        # two cannot be told apart here. Prefect owns the final orchestration state.
        return "interrupted"
    return "failed" if error is not None else "completed"


def logger() -> Logger | LoggerAdapter[Logger]:
    try:
        return get_run_logger()
    except MissingContextError:
        return getLogger(__name__)


class StageResult(BaseModel):
    name: str
    seconds: float
    status: Status


class SyncObservation(BaseModel):
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    producer: Producer | None = None
    result: SyncResult = Field(default_factory=SyncResult)
    stages: list[StageResult] = Field(default_factory=lambda: [])

    @asynccontextmanager
    async def stage(self, name: str) -> AsyncGenerator[None]:
        started = monotonic()
        log = logger()
        log.info("%s: started", name)

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(HEARTBEAT_SECONDS)
                log.info(
                    "%s: still waiting after %.0fs (elapsed time only; progress unverified)",
                    name,
                    monotonic() - started,
                )

        task = asyncio.create_task(heartbeat())
        status: Status = "completed"
        try:
            yield
        except BaseException as exc:
            status = _status(exc)
            raise
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            seconds = monotonic() - started
            self.stages.append(StageResult(name=name, seconds=seconds, status=status))
            log.info("%s: %s after %.1fs", name, status, seconds)

    async def publish(self, error: BaseException | None) -> None:
        try:
            async with asyncio.timeout(ARTIFACT_TIMEOUT_SECONDS):
                await self._publish(error)
        except (Exception, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError) and error is None:
                raise
            # Diagnostics cannot replace the sync outcome or expose response bodies.
            logger().warning("Sync artifact unavailable (%s)", type(exc).__name__)

    async def _publish(self, error: BaseException | None) -> None:
        # Direct .fn() callers have no run to attach to. Production flows always have one.
        context = FlowRunContext.get()
        if context is None or context.flow_run is None:
            return
        finished = datetime.now(UTC)
        status = _status(error)
        lines = [
            "# S2 sync results",
            "",
            f"- Status: {status}",
            "- Status describes observed execution; Prefect owns the final orchestration state.",
            f"- Run: {context.flow_run.id}",
            f"- Deployment: {context.flow_run.deployment_id or 'local'}",
            f"- Started (UTC): {self.started_at.isoformat()}",
            f"- Finished (UTC): {finished.isoformat()}",
            f"- Elapsed seconds: {(finished - self.started_at).total_seconds():.1f}",
            f"- Error type: {type(error).__name__ if error is not None else 'none'}",
            f"- Software version: {self.producer.software_version if self.producer else 'unknown'}",
            f"- Image digest: {self.producer.image_digest if self.producer else 'unknown'}",
            "",
            "## Datasets",
            "",
            "Only observed datasets are listed; an interrupted run may leave work unstarted.",
            "",
            "| Dataset | Start | Target | Final | Outcome | Archived steps | Loaded steps |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for item in self.result.datasets:
            outcome = item.outcome
            if outcome == "failed" and item.reason is None:
                stage = next(
                    (
                        s
                        for s in reversed(self.stages)
                        if s.name in (f"sync {item.dataset}", f"archive {item.dataset}")
                    ),
                    None,
                )
                outcome = "unstarted" if stage is None else stage.status
            lines.append(
                f"| {item.dataset} | {item.start or '—'} | {item.target or '—'} | "
                f"{item.final or '—'} | {outcome} | "
                f"{len(item.archived)} | {len(item.loaded)} |"
            )
        lines.extend(["", "## Read tables", ""])
        edges = self.result.read_tables
        if edges is not None:
            lines.extend(
                [
                    f"Source releases: {edges.source_releases}",
                    "",
                    "| Table | Rows inserted | Build and publication seconds |",
                    "| --- | --- | --- |",
                    *(
                        f"| {table} | {built.rows} | {built.seconds:.1f} |"
                        for table, built in edges.tables.items()
                    ),
                    "",
                    "| Source | Path | Touched |",
                    "| --- | --- | --- |",
                    *(
                        f"| {source} | {path} | {edges.touched.get(source, '—')} |"
                        for source, path in edges.paths.items()
                    ),
                ]
            )
        else:
            lines.append(
                "Rebuild failed; publication may be partial. Check logs and applied releases."
                if self.result.read_tables_failure is not None
                else "No completed rebuild recorded (already current, skipped, or interrupted)."
            )
        lines.extend(
            [
                "",
                "## Stages",
                "",
                "Durations include waits. Nested stages overlap; do not sum them.",
                "",
                "| Stage | Seconds | Status |",
                "| --- | --- | --- |",
            ]
        )
        lines.extend(f"| {s.name} | {s.seconds:.1f} | {s.status} |" for s in self.stages)
        await MarkdownArtifact(
            key="s2-sync-results",
            markdown="\n".join(lines),
            description="Dataset outcomes, edge counts, stage timings and run metadata.",
        ).acreate()
