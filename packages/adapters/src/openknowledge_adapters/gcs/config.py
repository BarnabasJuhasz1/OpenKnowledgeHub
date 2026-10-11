from __future__ import annotations

from pydantic import BaseModel, ConfigDict, PositiveInt


class GcsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    request_timeout_seconds: PositiveInt
    min_transfer_rate: PositiveInt
    """The slowest transfer allowed, in bytes per second, so timeouts grow with an object's size."""
    api_root: str | None = None
    """An emulator or test server that gets requests without credentials, or None for GCS itself."""
