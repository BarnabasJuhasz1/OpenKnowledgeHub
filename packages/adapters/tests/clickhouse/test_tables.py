import httpx
import pytest

from openknowledge_adapters import clickhouse
from openknowledge_core.ingestion.errors import (
    ContractViolation,
    CredentialRejected,
    IngestionError,
    TableRejected,
    TableUnavailable,
)

CONFIG = clickhouse.ClickHouseConfig.model_validate(
    {
        "url": "http://clickhouse:8123",
        "user": "openknowledge",
        "password": "hunter2",
        "raw_bucket": "raw",
        "timeout_seconds": 5,
    }
)
VIOLATION = (
    "Code: 395. DB::Exception: papers: corpusid breaks the contract: while executing "
    "'FUNCTION throwIf(...)'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 26.8.22.13)"
)


@pytest.mark.parametrize(
    ("status", "code", "body", "error", "message"),
    [
        (500, "395", VIOLATION, ContractViolation, "papers: corpusid breaks the contract; "),
        (403, "516", "Code: 516. DB::Exception: wrong password", CredentialRejected, "ClickHouse"),
        (503, "", "", TableUnavailable, "ClickHouse query"),
        (404, "60", "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)", TableRejected, ""),
        (0, "", "", TableUnavailable, "ClickHouse query"),
    ],
)
async def test_errors_map_by_exception_code(
    status: int, code: str, body: str, error: type[IngestionError], message: str
) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert "hunter2" not in str(request.url)
        assert request.headers["X-ClickHouse-Key"] == "hunter2"
        if not status:
            raise httpx.ConnectError("refused")
        return httpx.Response(status, headers={"X-ClickHouse-Exception-Code": code}, text=body)

    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as tables:
        with pytest.raises(error) as raised:
            await tables.live_rows("papers", "2026-10-06")
    assert str(raised.value).startswith(message)
    assert "hunter2" not in str(raised.value)
