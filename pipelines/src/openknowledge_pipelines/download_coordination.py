"""Serialize both download deployments, including remote jobs that outlive their flow."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from time import monotonic
from uuid import UUID

from prefect.client.orchestration import PrefectClient, get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.client.schemas.responses import (
    GlobalConcurrencyLimitResponse,
    MinimalConcurrencyLimitResponse,
)
from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound, PrefectHTTPStatusError
from prefect.settings import PREFECT_API_URL, PREFECT_CLIENT_MAX_RETRIES, temporary_settings
from pydantic import TypeAdapter, ValidationError

DOWNLOAD_LIMIT = "s2-downloads"
log = logging.getLogger(__name__)
RECOVERY = "verify every submitted Batch job is terminal before an operator clears the active slot"


@asynccontextmanager
async def exclusive_download() -> AsyncGenerator[None, None]:
    """Reserve a durable slot, releasing it only when the entire download succeeds.

    The reservation has no lease or decay: a crashed flow must not admit another
    download while its remote job may still be running.
    """
    if not PREFECT_API_URL.value():
        raise RuntimeError("Batch downloads require a shared, persistent PREFECT_API_URL")
    async with get_client() as client:
        original = await _read_or_create(client)
        _check(original, original.id)
        if original.active_slots != 0:
            raise RuntimeError(f"{DOWNLOAD_LIMIT}: downloads are blocked; {RECOVERY}")
        started = monotonic()
        try:
            # These counter mutations are not idempotent. Retrying a lost reply could
            # reserve twice, or clear a later download's reservation after our release.
            with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
                response = await client.increment_concurrency_slots(
                    names=[DOWNLOAD_LIMIT], slots=1, mode="concurrency"
                )
        except PrefectHTTPStatusError as exc:
            if exc.response.status_code == 423:
                raise RuntimeError(f"{DOWNLOAD_LIMIT}: downloads are blocked; {RECOVERY}") from None
            raise
        _check_reply(response.json(), original)
        acquired = await client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
        _check(acquired, original.id)
        if acquired.active_slots != 1:
            raise RuntimeError(f"{DOWNLOAD_LIMIT}: the shared slot was not reserved; {RECOVERY}")
        try:
            yield
        except BaseException:
            log.error("%s stays occupied: %s", DOWNLOAD_LIMIT, RECOVERY)
            raise
        current = await client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
        _check(current, original.id)
        if current.active_slots != 1:
            raise RuntimeError(f"{DOWNLOAD_LIMIT}: the reservation changed; {RECOVERY}")
        with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
            response = await client.release_concurrency_slots(
                names=[DOWNLOAD_LIMIT], slots=1, occupancy_seconds=monotonic() - started
            )
        _check_reply(response.json(), original)


async def _read_or_create(client: PrefectClient) -> GlobalConcurrencyLimitResponse:
    try:
        return await client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)
    except ObjectNotFound:
        try:
            await client.create_global_concurrency_limit(
                GlobalConcurrencyLimitCreate(
                    name=DOWNLOAD_LIMIT, limit=1, active=True, slot_decay_per_second=0
                )
            )
        except ObjectAlreadyExists:
            pass
        return await client.read_global_concurrency_limit_by_name(DOWNLOAD_LIMIT)


def _check(limit: GlobalConcurrencyLimitResponse, expected_id: UUID) -> None:
    if (
        limit.id != expected_id
        or limit.name != DOWNLOAD_LIMIT
        or not limit.active
        or limit.slot_decay_per_second != 0
        or limit.limit != 1
    ):
        raise RuntimeError(
            f"{DOWNLOAD_LIMIT}: downloads are blocked; the same shared limit must be active, "
            f"have no slot decay and limit=1; {RECOVERY}"
        )


def _check_reply(payload: object, expected: GlobalConcurrencyLimitResponse) -> None:
    try:
        [reply] = TypeAdapter(list[MinimalConcurrencyLimitResponse]).validate_python(payload)
    except (ValidationError, ValueError):
        raise RuntimeError(f"{DOWNLOAD_LIMIT}: invalid counter response; {RECOVERY}") from None
    if reply.id != expected.id or reply.name != DOWNLOAD_LIMIT or reply.limit != 1:
        raise RuntimeError(
            f"{DOWNLOAD_LIMIT}: the counter response names another limit; {RECOVERY}"
        )
