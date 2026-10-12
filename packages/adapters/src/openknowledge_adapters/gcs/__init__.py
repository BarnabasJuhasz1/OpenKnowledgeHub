"""Google Cloud Storage over gcloud-aio-storage."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import aiohttp
from gcloud.aio.auth import Token
from gcloud.aio.storage import Storage

from openknowledge_adapters.gcs.config import GcsConfig
from openknowledge_adapters.gcs.store import GcsArtifactStore, no_credentials, token_failure
from openknowledge_core.ingestion.errors import IngestionError

GCS_API_ROOT = "https://www.googleapis.com"


@asynccontextmanager
async def connect(config: GcsConfig) -> AsyncGenerator[GcsArtifactStore, None]:
    """One storage client per run, with credentials checked first unless ``api_root`` is set."""
    emulated = bool(config.api_root)
    # Owned here, because gcloud-aio leaks the session it opens when reading credentials fails.
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=not emulated)) as session:
        try:
            # An explicit root keeps gcloud-aio from reading STORAGE_EMULATOR_HOST.
            storage = Storage(
                api_root=config.api_root or GCS_API_ROOT,
                api_is_dev=emulated,
                session=session,
                token=cast(Token, _NoToken()) if emulated else None,
            )
        except Exception:  # a credentials file that is missing or of a type gcloud-aio cannot read
            storage = None
        if storage is None:
            raise no_credentials()
        async with storage:
            if not emulated:
                await _check_credentials(storage)
            yield GcsArtifactStore(
                storage,
                request_timeout=config.request_timeout_seconds,
                min_transfer_rate=config.min_transfer_rate,
            )


async def _check_credentials(storage: Storage) -> None:
    error: IngestionError | None = None
    try:
        token = await storage.token.get()
    except aiohttp.ClientResponseError as exc:  # not chained, since its request can hold a token
        error = token_failure(exc.status)
    except Exception:
        error = no_credentials()
    else:
        error = None if token else no_credentials()
    if error is not None:
        raise error


class _NoToken:
    """Stands in for credentials an emulator never asks for, so none are read."""

    async def get(self) -> None:
        return None


__all__ = ["GcsArtifactStore", "GcsConfig", "connect"]
