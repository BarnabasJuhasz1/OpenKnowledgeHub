import gzip
import logging
import pickle
from collections.abc import AsyncIterator
from contextlib import aclosing

import httpx
import pytest
from pydantic import SecretStr

from openknowledge_adapters.semantic_scholar import SemanticScholar
from openknowledge_adapters.semantic_scholar.downloads import (
    SignedLinkDownloads,
    install_log_redaction,
    redact,
)
from openknowledge_contracts.datasets import ShardRef
from openknowledge_core.ingestion.errors import LinkExpired, SourceRejected, SourceUnavailable
from openknowledge_core.ingestion.models import ByteStream, ShardLink
from tests.fakes.s2_http import FakeS2

URL = "https://files.test/staging/2026-10-06/papers/part-0.gz?Signature=topsecret&Expires=9"
BARE = "https://files.test/staging/2026-10-06/papers/part-0.gz"


def link(url: str = URL) -> ShardLink:
    shard = ShardRef(source_path="/staging/2026-10-06/papers/part-0.gz", file_name="part-0.gz")
    return ShardLink(shard=shard, url=SecretStr(url))


async def read_all(stream: ByteStream) -> bytes:
    async with aclosing(stream.chunks) as chunks:
        return b"".join([chunk async for chunk in chunks])


async def test_streams_the_bytes_and_their_declared_length(
    client: SemanticScholar, s2: FakeS2
) -> None:
    payload = gzip.compress(b'{"id": 1}\n' * 100, mtime=0)
    s2.add("papers", "part-0.gz", payload)
    listing = await client.catalog.list_dataset("2026-10-06", "papers")
    stream = await client.downloads.open(listing.links[0])
    assert stream.expected_size == len(payload)
    assert await read_all(stream) == payload


async def test_bytes_are_stored_as_served_whatever_the_content_encoding() -> None:
    payload = gzip.compress(b"json lines", mtime=0)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept-encoding"] == "identity"
        assert "x-api-key" not in request.headers
        headers = {"content-encoding": "gzip", "content-length": str(len(payload))}
        return httpx.Response(200, headers=headers, stream=httpx.ByteStream(payload))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        assert await read_all(await SignedLinkDownloads(http).open(link())) == payload


async def test_an_expired_link_raises_without_its_query(
    client: SemanticScholar, s2: FakeS2
) -> None:
    s2.add("papers", "part-0.gz", b"data")
    listing = await client.catalog.list_dataset("2026-10-06", "papers")
    s2.expire_links()
    with pytest.raises(LinkExpired) as info:
        await client.downloads.open(listing.links[0])
    assert str(info.value) == f"the file host refused {BARE} with HTTP 403"


@pytest.mark.parametrize(
    ("status", "error"), [(503, SourceUnavailable), (429, SourceUnavailable), (404, SourceRejected)]
)
async def test_statuses_map_to_core_errors(status: int, error: type[Exception]) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(error) as info:
            await SignedLinkDownloads(http).open(link())
    assert "topsecret" not in str(info.value)


async def test_a_connection_failure_names_no_query() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        with pytest.raises(SourceUnavailable) as info:
            await SignedLinkDownloads(http).open(link())
    assert str(info.value) == f"the file host did not answer for {BARE} (ConnectError); rerun later"
    assert info.value.__context__ is None


async def test_a_cut_mid_stream_is_transient_and_names_no_query(
    client: SemanticScholar, s2: FakeS2
) -> None:
    s2.add("papers", "part-0.gz", b"x" * 1000)
    s2.cut_next.add("part-0.gz")
    listing = await client.catalog.list_dataset("2026-10-06", "papers")
    stream = await client.downloads.open(listing.links[0])
    with pytest.raises(SourceUnavailable, match="stopped sending") as info:
        await read_all(stream)
    assert "Signature" not in str(info.value)
    assert info.value.__context__ is None


async def test_httpx_log_lines_carry_no_query_string(
    client: SemanticScholar, s2: FakeS2, caplog: pytest.LogCaptureFixture
) -> None:
    s2.add("papers", "part-0.gz", b"data")
    listing = await client.catalog.list_dataset("2026-10-06", "papers")
    caplog.set_level(logging.INFO, logger="httpx")
    await read_all(await client.downloads.open(listing.links[0]))
    lines = [record.getMessage() for record in caplog.records if record.name == "httpx"]
    assert lines
    assert all(BARE in line for line in lines)
    assert not any("Signature" in line for line in lines)


def test_installing_redaction_caps_an_unset_httpx_logger() -> None:
    logger = logging.getLogger("httpx")
    logger.setLevel(logging.NOTSET)
    install_log_redaction()
    assert logger.level == logging.WARNING


def test_redaction_keeps_httpcore_debug_lines_off(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    install_log_redaction()
    assert not logging.getLogger("httpcore.http11").isEnabledFor(logging.DEBUG)


def test_redact_drops_the_query_and_userinfo() -> None:
    assert redact(URL) == BARE
    assert redact("https://user:pw@files.test:8443/p?Signature=s") == "https://files.test:8443/p"
    assert redact("https://[2001:db8::1]:99/p?Signature=s") == "https://[2001:db8::1]:99/p"


def test_errors_stay_redacted_after_pickling() -> None:
    error = pickle.loads(pickle.dumps(LinkExpired(f"HTTP 403 from {redact(URL)}")))
    assert str(error) == f"HTTP 403 from {BARE}"


async def test_closing_an_unread_stream_closes_the_response() -> None:
    closed: list[bool] = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"data"

        async def aclose(self) -> None:
            closed.append(True)

    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=Body()))
    async with httpx.AsyncClient(transport=transport) as http:
        stream = await SignedLinkDownloads(http).open(link())
        await stream.chunks.aclose()
    assert closed == [True]


async def test_a_link_httpx_cannot_parse_is_refused_without_its_query() -> None:
    async with httpx.AsyncClient() as http:
        with pytest.raises(SourceRejected) as info:
            await SignedLinkDownloads(http).open(link(URL.replace("files.test", "files.test:9x")))
    assert "topsecret" not in str(info.value)
