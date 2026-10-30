"""Nonleased admission survives runner replacement and uncertain external work."""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Literal
from uuid import UUID, uuid4

import httpx
from prefect.client.orchestration import PrefectClient, SyncPrefectClient, get_client
from prefect.client.schemas.actions import (
    GlobalConcurrencyLimitCreate,
    GlobalConcurrencyLimitUpdate,
)
from prefect.client.schemas.responses import GlobalConcurrencyLimitResponse
from prefect.exceptions import ObjectNotFound
from prefect.settings import PREFECT_API_URL, PREFECT_CLIENT_MAX_RETRIES, temporary_settings
from pydantic import BaseModel

NAME = "deployment-admission"
OPEN_LIMIT = 1_000_000
_LOG = logging.getLogger(__name__)


class AdmissionError(RuntimeError):
    """A sanitized refusal safe to show at the command boundary."""


class _Refused(AdmissionError):
    """The API answered that it applied nothing: a missing limit or a closed gate."""


class _DatabaseUnavailable(AdmissionError):
    """A read may retry this response; a mutation may have committed."""


class AdmissionStatus(BaseModel):
    supported: bool = True
    closed: bool
    active_slots: int


def _require_server() -> None:
    if not PREFECT_API_URL.value():
        raise AdmissionError("Admission requires the shared PREFECT_API_URL")


def _validate(current: GlobalConcurrencyLimitResponse) -> None:
    if not current.active or current.slot_decay_per_second != 0:
        raise AdmissionError(
            "Admission must be active without slot decay; operator repair required"
        )
    if current.limit not in (0, OPEN_LIMIT):
        raise AdmissionError("Unexpected admission capacity; operator repair required")


def _check_response(response: httpx.Response) -> None:
    # Prefect retries maintenance responses indefinitely, even with max_retries=0.
    # Abort rejected requests before its retry loop. Preserve the GET responses
    # used for missing-limit detection and discovery of disabled CSRF protection.
    request = response.request
    expected = request.method == "GET" and (
        response.status_code == 404
        or (request.url.path.endswith("/csrf-token") and response.status_code == 422)
    )
    if response.is_success or expected:
        return
    if response.headers.get("Prefect-Maintenance") != "true":
        if response.status_code == 503 and _body(response) == _DATABASE_UNAVAILABLE:
            raise _DatabaseUnavailable("Admission API database busy")
        if response.status_code in (404, 423):
            raise _Refused("Admission limit missing or closed")
    raise AdmissionError(
        "Admission API request refused; state may be uncertain; inspect status before retrying"
    )


# Prefect's handler for retryable database errors answers 503 with this body. It covers
# lost connections too, so it does not prove a rollback.
_DATABASE_UNAVAILABLE = {"exception_message": "Service Unavailable"}


def _body(response: httpx.Response) -> object:
    try:
        response.read()
        return response.json()
    except ValueError:
        return None


async def _check_async_response(response: httpx.Response) -> None:
    if not response.is_success:
        await response.aread()
    _check_response(response)


async def _retrying[T](call: Callable[[], Awaitable[T]]) -> T:
    for delay in (0.5, 1, 2):
        try:
            return await call()
        except _DatabaseUnavailable:
            await asyncio.sleep(delay)
    return await call()


def _retried[T](call: Callable[[], T]) -> T:
    for delay in (0.5, 1, 2):
        try:
            return call()
        except _DatabaseUnavailable:
            time.sleep(delay)
    return call()


@asynccontextmanager
async def execution(*, release_on_exception: bool = False) -> AsyncGenerator[None]:
    """Hold one admission slot for the work inside.

    A failure retains the slot, since it may leave Batch, native or database work alive.
    Search may release on Exception: it awaits its requests and its writes are idempotent.
    Cancellation and other BaseExceptions still retain the slot, as does uncertain acquisition.
    """
    _require_server()
    owner = uuid4().hex
    # The pinned client's retry loop reads this setting on each send. Never replay a
    # counter mutation on an uncertain answer: a response can be lost after the database
    # commits it. Only a refusal proves nothing was applied.
    async with get_client(
        httpx_settings={"timeout": 10, "event_hooks": {"response": [_check_async_response]}}
    ) as client:
        _LOG.warning("Admission acquisition started owner=%s", owner)
        sent = incremented = False
        try:
            with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
                async with asyncio.timeout(30):
                    current = await _retrying(
                        lambda: client.read_global_concurrency_limit_by_name(NAME)
                    )
                    _validate(current)
                    # Prefect 3.8.8 answers an increment of a closed limit with a 500 (it
                    # divides by the capacity), which proves nothing; refuse before sending.
                    if current.limit == 0:
                        raise AdmissionError("Admission closed")
                    sent = True
                    response = await client.increment_concurrency_slots([NAME], 1, "concurrency")
                    incremented = True
                    if response.json() != [
                        {"id": str(current.id), "name": NAME, "limit": OPEN_LIMIT}
                    ]:
                        raise AdmissionError("Admission acquisition unconfirmed")
                    # Prefect returns inactive limits too, even when no slot was taken.
                    # Check again before work; never compensate an uncertain acquisition.
                    confirmed = await _retrying(
                        lambda: client.read_global_concurrency_limit_by_name(NAME)
                    )
                    _validate(confirmed)
                    if confirmed.id != current.id or confirmed.active_slots < 1:
                        raise AdmissionError("Admission acquisition unconfirmed")
        except BaseException as exc:
            if isinstance(exc, Exception) and (
                not sent or (not incremented and isinstance(exc, _Refused))
            ):
                _LOG.warning("Admission refused owner=%s", owner)
                raise AdmissionError(
                    "Admission closed, unavailable or invalid; execution refused"
                ) from None
            _LOG.error("Admission acquisition unconfirmed; reconcile owner=%s", owner)
            if not isinstance(exc, Exception):
                raise
            raise AdmissionError(
                f"Admission closed, unavailable or invalid; execution refused; "
                f"reconcile uncertain occupancy (owner={owner})"
            ) from None
        _LOG.warning("Admission acquired owner=%s", owner)
        try:
            yield
        except BaseException as exc:
            if release_on_exception and isinstance(exc, Exception):
                await _release(client, current.id, owner)
            else:
                _LOG.error("Execution failed or interrupted; admission retained owner=%s", owner)
            raise
        await _release(client, current.id, owner)


async def _release(client: PrefectClient, identity: UUID, owner: str) -> None:
    try:
        with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
            async with asyncio.timeout(30):
                confirmed = await _retrying(
                    lambda: client.read_global_concurrency_limit_by_name(NAME)
                )
                _validate(confirmed)
                if confirmed.id != identity:
                    raise AdmissionError("Admission identity changed")
                # Occupancy averaging divides by capacity in Prefect 3.8.8.
                # Omit that optional statistic so release also works closed.
                response = await client.request(
                    "POST",
                    "/v2/concurrency_limits/decrement",
                    json={"names": [NAME], "slots": 1},
                )
                # Closing can race this request after the pre-read.
                if response.json() not in (
                    [{"id": str(identity), "name": NAME, "limit": capacity}]
                    for capacity in (0, OPEN_LIMIT)
                ):
                    raise AdmissionError("Admission release unconfirmed")
    except BaseException as exc:
        _LOG.error("Admission release unconfirmed; reconcile owner=%s", owner)
        if not isinstance(exc, Exception):
            raise
        raise AdmissionError(
            f"Admission release uncertain; operator reconciliation required (owner={owner})"
        ) from None
    _LOG.warning("Admission released owner=%s", owner)


def ensure_closed() -> None:
    """Initial install requires explicit operator admission after verification."""
    try:
        with _control() as client:
            try:
                current = _retried(lambda: client.read_global_concurrency_limit_by_name(NAME))
            except ObjectNotFound:
                client.create_global_concurrency_limit(
                    GlobalConcurrencyLimitCreate(name=NAME, limit=0)
                )
            else:
                _validate(current)
    except AdmissionError:
        raise
    except Exception:
        raise AdmissionError(
            "Cannot initialize shared admission; check the control API before starting serve"
        ) from None


@contextmanager
def _control() -> Generator[SyncPrefectClient]:
    _require_server()
    with (
        temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}),
        get_client(
            sync_client=True,
            httpx_settings={"timeout": 10, "event_hooks": {"response": [_check_response]}},
        ) as client,
    ):
        yield client


def status(action: Literal["status", "close", "open"] = "status") -> AdmissionStatus:
    with _control() as client:
        current = _retried(lambda: client.read_global_concurrency_limit_by_name(NAME))
        _validate(current)
        if action == "open" and current.active_slots != 0:
            raise AdmissionError("Admission still occupied; reconcile work before reopening")
        if action in ("close", "open"):
            update = GlobalConcurrencyLimitUpdate(limit=0 if action == "close" else OPEN_LIMIT)
            _retried(lambda: client.update_global_concurrency_limit(NAME, update))
            current = _retried(lambda: client.read_global_concurrency_limit_by_name(NAME))
            _validate(current)
        return AdmissionStatus(closed=current.limit == 0, active_slots=current.active_slots)


def release(slots: int) -> AdmissionStatus:
    """Free all held slots after operator reconciliation, only while closed."""
    with _control() as client:
        current = _retried(lambda: client.read_global_concurrency_limit_by_name(NAME))
        _validate(current)
        if current.limit != 0:
            raise AdmissionError("Close admission before releasing slots")
        if current.active_slots != slots:
            raise AdmissionError(f"Admission holds {current.active_slots} slots, not {slots}")
        update = GlobalConcurrencyLimitUpdate(active_slots=0)
        _retried(lambda: client.update_global_concurrency_limit(NAME, update))
        current = _retried(lambda: client.read_global_concurrency_limit_by_name(NAME))
        _validate(current)
        return AdmissionStatus(closed=current.limit == 0, active_slots=current.active_slots)
