from __future__ import annotations

from pydantic import Field, HttpUrl, NonNegativeFloat, NonNegativeInt, PositiveFloat, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from openknowledge_contracts.manifests import ImageDigest
from openknowledge_core.ingestion.models import ApiKey


class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OKP_", extra="ignore", frozen=True, hide_input_in_errors=True
    )

    s2_api_key: ApiKey = Field(validation_alias="S2_API_KEY")
    s2_api_url: HttpUrl = HttpUrl("https://api.semanticscholar.org/datasets/v1/")
    s2_request_interval_seconds: PositiveFloat = 1.0
    s2_attempts: PositiveInt = 8
    s2_backoff_seconds: PositiveFloat = 1.0
    s2_timeout_seconds: PositiveFloat = 30.0
    download_connect_timeout_seconds: PositiveFloat = 30.0
    download_read_timeout_seconds: PositiveFloat = 300.0
    gcs_request_timeout_seconds: PositiveInt = 60
    gcs_min_transfer_bytes_per_second: PositiveInt = 256 * 1024
    # Both task settings cover every task of the s2-download flow, not only the shard tasks.
    task_retries: NonNegativeInt = 2
    task_retry_delay_seconds: NonNegativeFloat = 30.0
    image_digest: ImageDigest | None = None
    """The image digest okp records in receipts and manifests."""
