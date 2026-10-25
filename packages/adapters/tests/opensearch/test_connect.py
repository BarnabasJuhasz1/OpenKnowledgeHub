"""Both per-run adapters preserve shared transport/auth configuration and closure."""

from base64 import b64encode
from typing import Any

import pytest
from opensearchpy import AsyncConnection, AsyncOpenSearch

from openknowledge_adapters import opensearch


@pytest.mark.parametrize("vectors", [False, True])
async def test_connections_share_configuration_and_close_on_failure(
    monkeypatch: pytest.MonkeyPatch, vectors: bool
) -> None:
    captured: dict[str, Any] = {}
    closed: list[bool] = []

    class Client(AsyncOpenSearch):
        async def close(self) -> None:
            closed.append(True)
            await super().close()

    def client(**kwargs: Any) -> AsyncOpenSearch:  # noqa: ANN401
        captured.update(kwargs)
        return Client(**kwargs)

    monkeypatch.setattr(opensearch, "AsyncOpenSearch", client)
    config = opensearch.OpenSearchConfig.model_validate(
        {
            "url": "https://[::1]:9443/search",
            "user": "user",
            "password": "secret",
            "timeout_seconds": 7,
        }
    )
    opened = opensearch.connect_vectors if vectors else opensearch.connect
    with pytest.raises(ValueError, match="fixture failure"):
        async with opened(config, connection_class=AsyncConnection):
            raise ValueError("fixture failure")
    assert captured["hosts"] == [
        {"host": "::1", "port": 9443, "use_ssl": True, "url_prefix": "/search"}
    ]
    assert captured["headers"] == {"Authorization": "Basic " + b64encode(b"user:secret").decode()}
    assert captured["verify_certs"] is True and captured["http_compress"] is True
    assert captured["timeout"] == 7 and "http_auth" not in captured
    assert closed == [True]
