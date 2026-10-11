"""Reads shards from their signed links on the file host, without the API key or the rate limit."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from urllib.parse import urlsplit

import httpx

from openknowledge_core.ingestion.errors import (
    TRANSIENT_STATUSES,
    IngestionError,
    LinkExpired,
    SourceRejected,
    SourceUnavailable,
)
from openknowledge_core.ingestion.models import ByteStream, ShardLink

READ_SIZE = 1024 * 1024
EXPIRED_LINK_STATUS = 403  # how S3 answers a presigned link past its expiry


class SignedLinkDownloads:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def open(self, link: ShardLink) -> ByteStream:
        """The bytes are stored as served, so the request asks for no content encoding."""
        url = link.url.get_secret_value()
        try:
            request = self._http.build_request("GET", url, headers={"accept-encoding": "identity"})
            response = await self._http.send(request, stream=True)
        except httpx.TransportError as exc:
            error: IngestionError = SourceUnavailable(
                f"the file host did not answer for {redact(url)} ({type(exc).__name__}); "
                "rerun later"
            )
        except (httpx.HTTPError, httpx.InvalidURL, UnicodeError) as exc:
            error = SourceRejected(
                f"the link to {redact(url)} could not be used ({type(exc).__name__}); rerun later"
            )
        else:
            if response.status_code == 200:
                chunks = self._chunks(response, url)
                await anext(chunks)  # started, so closing it unread still closes the response
                return ByteStream(expected_size=_content_length(response), chunks=chunks)
            await response.aclose()
            error = _status_error(response.status_code, url)
        raise error

    async def _chunks(self, response: httpx.Response, url: str) -> AsyncGenerator[bytes, None]:
        stream = response.aiter_raw(READ_SIZE)
        error: IngestionError | None = None
        try:
            yield b""  # taken by open, which starts the generator
            while True:
                try:
                    chunk = await anext(stream)
                except StopAsyncIteration:
                    break
                except httpx.TransportError as exc:
                    error = SourceUnavailable(
                        f"the file host stopped sending {redact(url)} ({type(exc).__name__}); "
                        "rerun later"
                    )
                    break
                except httpx.HTTPError as exc:
                    error = SourceRejected(
                        f"the file host sent {redact(url)} in a form that could not be read "
                        f"({type(exc).__name__}); rerun later"
                    )
                    break
                yield chunk
        finally:
            await response.aclose()
        if error is not None:
            raise error


def redact(url: str) -> str:
    """The URL reduced to scheme, host and path: no query string, where a signature lives."""
    parts = urlsplit(url)
    host = parts.netloc.rpartition("@")[-1]  # drops userinfo, keeps the port and IPv6 brackets
    return f"{parts.scheme}://{host}{parts.path}"


def install_log_redaction() -> None:
    """Keep query strings out of httpx's log lines and turn off httpcore's, which print headers."""
    logger = logging.getLogger("httpx")
    logger.addFilter(_REDACT_URLS)
    if logger.level == logging.NOTSET:
        logger.setLevel(logging.WARNING)
    # httpcore formats its debug lines before a filter could redact them, so they stay off.
    logging.getLogger("httpcore").setLevel(logging.INFO)


class _RedactUrls(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.args, tuple):
                record.args = tuple(
                    redact(str(arg)) if isinstance(arg, httpx.URL) or "://" in str(arg) else arg
                    for arg in record.args
                )
        except Exception:
            return False  # a line that cannot be redacted is not logged
        return True


_REDACT_URLS = _RedactUrls()


def _content_length(response: httpx.Response) -> int | None:
    value = response.headers.get("content-length", "")
    return int(value) if value.isdecimal() else None


def _status_error(status: int, url: str) -> IngestionError:
    if status == EXPIRED_LINK_STATUS:
        return LinkExpired(f"the file host refused {redact(url)} with HTTP {status}")
    if status in TRANSIENT_STATUSES:
        return SourceUnavailable(
            f"the file host answered HTTP {status} for {redact(url)}; rerun later"
        )
    return SourceRejected(f"the file host refused {redact(url)} with HTTP {status}; rerun later")
