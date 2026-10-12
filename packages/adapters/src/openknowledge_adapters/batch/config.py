from __future__ import annotations

from pydantic import BaseModel, ConfigDict, PositiveFloat, PositiveInt


class BatchConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    project: str
    region: str
    service_account: str
    network: str
    subnetwork: str
    image: str
    """The image every task runs, pinned by digest."""
    machine_type: str
    max_run_seconds: PositiveInt
    poll_seconds: PositiveFloat
    environment: dict[str, str]
    secrets: dict[str, str]
    """Variables Batch fills from these Secret Manager versions, so no value reaches the job."""
