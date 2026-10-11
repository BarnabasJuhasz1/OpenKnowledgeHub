from __future__ import annotations

from pydantic import BaseModel, ConfigDict, HttpUrl, PositiveFloat, PositiveInt

from openknowledge_core.ingestion.models import ApiKey


class SemanticScholarConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    api_key: ApiKey
    api_url: HttpUrl
    request_interval_seconds: PositiveFloat
    attempts: PositiveInt
    backoff_seconds: PositiveFloat
    timeout_seconds: PositiveFloat
    download_connect_timeout_seconds: PositiveFloat
    download_read_timeout_seconds: PositiveFloat
