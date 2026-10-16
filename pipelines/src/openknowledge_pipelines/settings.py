from __future__ import annotations

from pydantic import (
    Field,
    HttpUrl,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    SecretStr,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from openknowledge_contracts.manifests import BucketName, ImageDigest
from openknowledge_core.ingestion.models import ApiKey


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OKP_", extra="ignore", frozen=True, hide_input_in_errors=True
    )

    gcs_request_timeout_seconds: PositiveInt = 60
    gcs_min_transfer_bytes_per_second: PositiveInt = 256 * 1024
    gcs_api_root: str | None = None
    """A GCS emulator, which okp calls without credentials."""
    # Both task settings cover every task of a flow.
    task_retries: NonNegativeInt = 2
    task_retry_delay_seconds: NonNegativeFloat = 30.0
    image_digest: ImageDigest | None = None
    """The image digest okp records in receipts, manifests and load records."""


class PipelineSettings(CommonSettings):
    s2_api_key: ApiKey = Field(validation_alias="S2_API_KEY")
    s2_api_url: HttpUrl = HttpUrl("https://api.semanticscholar.org/datasets/v1/")
    s2_request_interval_seconds: PositiveFloat = 1.0
    s2_attempts: PositiveInt = 8
    s2_backoff_seconds: PositiveFloat = 1.0
    s2_timeout_seconds: PositiveFloat = 30.0
    download_connect_timeout_seconds: PositiveFloat = 30.0
    download_read_timeout_seconds: PositiveFloat = 300.0


class LoadSettings(CommonSettings):
    clickhouse_url: HttpUrl
    clickhouse_user: str = "openknowledge"
    clickhouse_password: SecretStr = Field(validation_alias="CLICKHOUSE_PASSWORD")
    clickhouse_raw_bucket: BucketName
    """The bucket the server's ``raw`` named collection reads."""
    clickhouse_timeout_seconds: PositiveFloat = 3600.0


class BatchSettings(PipelineSettings):
    image_digest: ImageDigest  # pyright: ignore[reportIncompatibleVariableOverride, reportGeneralTypeIssues]
    """Batch tasks run the image at this digest."""
    batch_project: str
    batch_region: str
    batch_service_account: str
    batch_network: str
    batch_subnetwork: str
    batch_image_repository: str
    batch_s2_key_secret: str
    """The Secret Manager version Batch puts in each task's S2_API_KEY."""
    batch_machine_type: str = "e2-standard-2"
    batch_max_run_seconds: PositiveInt = 6 * 60 * 60
    batch_poll_seconds: PositiveFloat = 30.0
