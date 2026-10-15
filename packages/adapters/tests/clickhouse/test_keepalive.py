from types import SimpleNamespace

import pytest

from openknowledge_adapters import clickhouse


@pytest.mark.parametrize(
    ("available", "expected"),
    [
        ({"TCP_KEEPIDLE": 4, "TCP_KEEPINTVL": 5}, [(3, 4, 60), (3, 5, 30)]),
        ({"TCP_KEEPALIVE": 6, "TCP_KEEPINTVL": 5}, [(3, 6, 60), (3, 5, 30)]),
        ({"TCP_KEEPIDLE": 4, "TCP_KEEPALIVE": 6}, [(3, 4, 60)]),
        ({"TCP_KEEPALIVE": 6}, [(3, 6, 60)]),
        ({"TCP_KEEPINTVL": 5}, [(3, 5, 30)]),
        ({}, []),
    ],
)
def test_keepalive_uses_only_available_platform_options(
    monkeypatch: pytest.MonkeyPatch,
    available: dict[str, int],
    expected: list[tuple[int, int, int]],
) -> None:
    platform_socket = SimpleNamespace(SOL_SOCKET=1, SO_KEEPALIVE=2, IPPROTO_TCP=3, **available)
    monkeypatch.setattr(clickhouse, "socket", platform_socket)
    assert clickhouse.keepalive_options() == [(1, 2, 1), *expected]
