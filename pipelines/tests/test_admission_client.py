"""Exercise the pinned HTTP client's retry behavior without a server substitute."""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_URL, PREFECT_CLIENT_MAX_RETRIES, temporary_settings

from openknowledge_pipelines import admission


@pytest.mark.parametrize(
    "failure",
    [
        "acquire",
        "release",
        "body",
        "body-release",
        "cancel-release",
        "inactive",
        "cancel",
        "acquire-maintenance",
        "release-maintenance",
        "acquire-busy",
        "release-busy",
        "acquire-timeout",
        "release-timeout",
        "close-on-release",
        "read-busy",
    ],
)
async def test_uncertainty_never_replays_or_compensates_counter_mutations(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    slots = 0
    increments = 0
    decrements = 0
    reads = 0
    identity = "00000000-0000-0000-0000-000000000001"

    def send(request: httpx.Request) -> httpx.Response:
        nonlocal slots, increments, decrements, reads
        assert PREFECT_CLIENT_MAX_RETRIES.value() == 0
        if request.url.path.endswith("/csrf-token"):
            return httpx.Response(404)
        if request.url.path.endswith("/increment"):
            increments += 1
            slots += 1
            if failure == "acquire-maintenance":
                return httpx.Response(
                    503, headers={"Prefect-Maintenance": "true", "Retry-After": "0"}
                )
            if failure == "acquire-timeout":
                raise httpx.ReadTimeout("response timed out after commit")
            if failure == "acquire":
                raise httpx.ReadError("response lost after commit")
            if failure == "acquire-busy":
                return httpx.Response(503, json={"exception_message": "Service Unavailable"})
        elif request.url.path.endswith("/decrement"):
            assert request.method == "POST"
            assert request.url.path == "/api/v2/concurrency_limits/decrement"
            assert json.loads(request.content) == {"names": [admission.NAME], "slots": 1}
            decrements += 1
            slots -= 1
            if failure == "release-timeout":
                raise httpx.ReadTimeout("response timed out after commit")
            if failure in ("close-on-release", "read-busy", "body-release"):
                return httpx.Response(
                    200, json=[{"id": identity, "name": admission.NAME, "limit": 0}]
                )
            if failure == "release-maintenance":
                return httpx.Response(
                    503, headers={"Prefect-Maintenance": "true", "Retry-After": "0"}
                )
            if failure == "release-busy":
                return httpx.Response(503, json={"exception_message": "Service Unavailable"})
            raise httpx.ReadError("response lost after commit")
        else:
            reads += 1
            if failure == "read-busy" and reads == 1:
                return httpx.Response(503, json={"exception_message": "Service Unavailable"})
            return httpx.Response(
                200,
                json={
                    "id": identity,
                    "name": admission.NAME,
                    "limit": admission.OPEN_LIMIT,
                    "active_slots": slots,
                    "slot_decay_per_second": 0,
                    "active": failure != "inactive" or reads == 1,
                },
            )
        return httpx.Response(
            200, json=[{"id": identity, "name": admission.NAME, "limit": admission.OPEN_LIMIT}]
        )

    @asynccontextmanager
    async def client(httpx_settings: dict[str, object]) -> AsyncGenerator[PrefectClient]:
        async with PrefectClient(
            "http://127.0.0.1:1/api",
            httpx_settings={**httpx_settings, "transport": httpx.MockTransport(send)},
        ) as instance:
            yield instance

    monkeypatch.setattr(admission, "get_client", client)

    async def work() -> None:
        async with admission.execution(release_on_exception=failure.endswith("-release")):
            if failure in ("body", "body-release"):
                raise RuntimeError("external job may survive")
            if failure in ("cancel", "cancel-release"):
                raise asyncio.CancelledError

    success = failure in ("close-on-release", "read-busy")
    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        if success:
            await work()
        else:
            expected = asyncio.CancelledError if failure.startswith("cancel") else RuntimeError
            with pytest.raises(expected):
                await work()
    released = failure.startswith("release") or success or failure == "body-release"
    assert increments == 1
    assert decrements == int(released)
    assert slots == (0 if released else 1)
    if failure == "read-busy":
        assert reads == 4


def test_status_does_not_loop_on_maintenance(monkeypatch: pytest.MonkeyPatch) -> None:
    from prefect.client.orchestration import SyncPrefectClient

    calls = 0

    def send(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        # A second call makes a regression fail immediately instead of waiting.
        assert calls == 1
        return httpx.Response(503, headers={"Prefect-Maintenance": "true", "Retry-After": "0"})

    def client(sync_client: bool, httpx_settings: dict[str, object]) -> SyncPrefectClient:
        assert sync_client
        return SyncPrefectClient(
            "http://127.0.0.1:1/api",
            httpx_settings={**httpx_settings, "transport": httpx.MockTransport(send)},
        )

    monkeypatch.setattr(admission, "get_client", client)
    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        with pytest.raises(admission.AdmissionError, match="request refused"):
            admission.status()
    assert calls == 1
