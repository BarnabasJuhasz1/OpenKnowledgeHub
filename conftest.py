"""Loaded by every test folder: Prefect's telemetry off and no connection off this machine."""

import ipaddress
import os
import socket
from collections.abc import Generator
from typing import Any, cast

import pytest

# pipelines/tests/conftest.py imports Prefect before openknowledge_pipelines can turn this off.
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")


def _local(host: object) -> bool:
    name = host.decode() if isinstance(host, bytes) else str(host)
    if host is None or name == "localhost":
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


@pytest.fixture(autouse=True, scope="session")
def _no_network() -> Generator[None]:
    """Session scope, so the guard is in place before any session fixture starts."""
    connect, getaddrinfo = socket.socket.connect, socket.getaddrinfo

    def guarded_connect(self: socket.socket, address: Any) -> None:  # noqa: ANN401
        target: object = address
        host = cast("tuple[object, ...]", target)[0] if isinstance(target, tuple) else None
        if not _local(host):
            raise OSError(f"tests do not use the network: refused {host!r}")
        connect(self, address)

    def guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        if not _local(host):
            raise OSError(f"tests do not use the network: refused to resolve {host!r}")
        return getaddrinfo(host, *args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(socket.socket, "connect", guarded_connect)
        patch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
        yield
