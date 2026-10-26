import asyncio
import logging
import signal
from unittest.mock import AsyncMock

import pytest
from prefect import flow
from prefect.artifacts import MarkdownArtifact
from prefect.exceptions import TerminationSignal

from openknowledge_pipelines.flows import sync_observability as diagnostics


@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (None, "completed"),
        (RuntimeError("secret"), "failed"),
        (asyncio.CancelledError(), "cancelled"),
        (TerminationSignal(signal.SIGTERM), "interrupted"),
    ],
)
async def test_stage_heartbeat_is_sparse_and_stops_on_every_exit(
    failure: BaseException | None,
    expected_status: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    beat = asyncio.Event()
    resume = asyncio.Event()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 1:
            return
        beat.set()
        await resume.wait()

    # The first tick completes, then the second blocks, giving an exact heartbeat count.
    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(diagnostics, "logger", lambda: logging.getLogger(__name__))
    caplog.set_level(logging.INFO)
    observation = diagnostics.SyncObservation()

    async def run() -> None:
        async with observation.stage("citation edges: build citation_edges_out"):
            await beat.wait()
            if failure is not None:
                raise failure

    if failure is None:
        await run()
    else:
        with pytest.raises(type(failure)) as raised:
            await run()
        assert raised.value is failure
    assert sleeps == [300, 300]
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 3  # start, one heartbeat, end
    assert "elapsed time only; progress unverified" in messages[1]
    assert observation.stages[0].status == expected_status
    resume.set()
    assert not any(
        "SyncObservation.stage.<locals>.heartbeat" in getattr(task.get_coro(), "__qualname__", "")
        for task in asyncio.all_tasks()
    )


@pytest.mark.parametrize(
    "failure", [None, RuntimeError("primary secret"), asyncio.CancelledError()]
)
@pytest.mark.usefixtures("prefect_server")
async def test_artifact_outage_preserves_sync_outcome_and_sanitizes_warning(
    failure: BaseException | None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        MarkdownArtifact, "acreate", AsyncMock(side_effect=OSError("response secret"))
    )

    @flow(persist_result=False)
    async def run() -> None:
        observation = diagnostics.SyncObservation()
        try:
            if failure is not None:
                raise failure
        finally:
            await observation.publish(failure)

    # .fn() inside a parent flow exercises cancellation without Prefect converting its state.
    @flow(persist_result=False)
    async def parent() -> None:
        if failure is None:
            await run.fn()
        else:
            with pytest.raises(type(failure)) as raised:
                await run.fn()
            assert raised.value is failure

    await parent()
    warnings = [
        record.getMessage()
        for record in caplog.records
        if "artifact unavailable" in record.getMessage()
    ]
    assert warnings == ["Sync artifact unavailable (OSError)"]
