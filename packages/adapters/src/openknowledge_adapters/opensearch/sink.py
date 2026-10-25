"""The search sink over OpenSearch: index creation, versioned bulk writes, refresh and count.

Every bulk item carries its version with ``external_gte``, so the same operations can be sent
again. The client's own retries cover a dropped connection and the 502 to 504 statuses; a bulk
request sent twice converges for the same reason, and so does an item refused with 429 that
opensearch-py's bulk helper sends again. A failure repeats what OpenSearch said only as an error
type and the first line of a reason, never a whole answer, because an answer from the wrong host
can echo the request and its credentials.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Awaitable, Collection, Mapping, Sequence
from typing import Any, cast

from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import (
    AuthenticationException,
    AuthorizationException,
    SerializationError,
    SSLError,
    TransportError,
)
from opensearchpy.exceptions import (
    ConnectionError as OpenSearchConnectionError,
)
from opensearchpy.helpers import async_streaming_bulk

from openknowledge_core.ingestion.errors import TRANSIENT_STATUSES, IngestionError
from openknowledge_core.search.errors import BulkRefused, SearchRejected, SearchUnavailable
from openknowledge_core.search.models import BulkOperation, DeleteDocument, IndexName

VERSION_TYPE = "external_gte"
# Refused items a failure names, at most.
NAMED = 3
# An item OpenSearch could not take right now, which a rerun sends again.
TOO_MANY_REQUESTS = 429
# The bulk helper sends the items refused with 429, or the whole request so refused, again up to
# RETRIES times, waiting INITIAL_BACKOFF seconds before the first and twice as long before each
# next, up to MAX_BACKOFF.
RETRIES = 3
INITIAL_BACKOFF = 2.0
MAX_BACKOFF = 60.0
NOT_OPENSEARCH = "an answer that is not OpenSearch's"
_ERROR_TYPE = re.compile(r"[A-Za-z0-9_.]{1,100}")
_REASON_LIMIT = 300


class OpenSearchSink:
    def __init__(
        self,
        client: AsyncOpenSearch,
        *,
        initial_backoff: float = INITIAL_BACKOFF,
        max_backoff: float = MAX_BACKOFF,
    ) -> None:
        self._client = client
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff

    async def exists(self, index: IndexName) -> bool:
        return bool(await self._call("look up", index, self._client.indices.exists(index=index)))

    async def create_index(
        self, index: IndexName, mapping: Mapping[str, Any], settings: Mapping[str, Any]
    ) -> None:
        body = {"mappings": dict(mapping), "settings": dict(settings)}
        await self._call("create", index, self._client.indices.create(index=index, body=body))

    async def bulk(self, index: IndexName, operations: Sequence[BulkOperation]) -> None:
        """Send every operation in one request and check every item the helper reports refused.

        The helper sends the items refused with 429 again, ``RETRIES`` times at most, so a batch
        whose only refusals are still 429 after that is transient, and a rerun sends it again.
        Any other refusal fails the pass for good.
        """
        if not operations:
            return
        actions: list[dict[str, Any]] = []
        for operation in operations:
            action = {
                "_index": index,
                "_id": operation.id,
                "version": operation.version,
                "version_type": VERSION_TYPE,
            }
            if isinstance(operation, DeleteDocument):
                actions.append({"_op_type": "delete", **action})
            else:
                actions.append(
                    {"_op_type": "index", **action, "_source": operation.document.source()}
                )
        sent = {operation.id for operation in operations}
        refused = await self._call("bulk write to", index, self._refused(index, actions, sent))
        if not refused:
            return
        named = "; ".join(f"corpusid {id_}: {reason}" for id_, _, reason in refused[:NAMED])
        message = f"{index}: {len(refused)} of {len(operations)} bulk items refused; {named}"
        if all(status == TOO_MANY_REQUESTS for _, status, _ in refused):
            raise SearchUnavailable(f"{message}; rerun")
        raise BulkRefused(message)

    async def set_refresh_interval(self, index: IndexName, interval: str | None) -> None:
        body = {"index": {"refresh_interval": interval}}
        request = self._client.indices.put_settings(body=body, index=index)
        await self._call("set the refresh interval of", index, request)

    async def refresh(self, index: IndexName) -> None:
        await self._call("refresh", index, self._client.indices.refresh(index=index))

    async def count(self, index: IndexName) -> int:
        answer: object = await self._call("count", index, self._client.count(index=index))
        count = cast("dict[str, object]", answer).get("count") if isinstance(answer, dict) else None
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SearchRejected(f"OpenSearch answered the count of {index} without a count")
        return count

    async def _refused(
        self, index: IndexName, actions: list[dict[str, Any]], sent: Collection[str]
    ) -> list[tuple[str, int, str]]:
        """Every item of one bulk request that stays refused, a 429 once the helper resent it.

        The service sizes each batch, so neither the chunk size nor the byte limit splits it.
        The helper yields each action once, applied or refused, and stops at the shorter of the
        answer and the batch, so a count short of the batch means an answer that lost items.
        """
        refused: list[tuple[str, int, str]] = []
        answered = 0
        malformed = False
        try:
            async for _, item in async_streaming_bulk(
                self._client,
                actions,
                chunk_size=len(actions),
                max_chunk_bytes=sys.maxsize,
                max_retries=RETRIES,
                initial_backoff=self._initial_backoff,
                max_backoff=self._max_backoff,
                raise_on_error=False,
            ):
                answered += 1
                if (refusal := _refusal(item, sent)) is not None:  # an error refuses any status
                    refused.append(refusal)
        except (KeyError, TypeError, AttributeError):  # the helper read an answer of another shape
            malformed = True
        if malformed or answered != len(actions):
            raise SearchRejected(
                f"OpenSearch answered a bulk write to {index} without one item per operation"
            )
        return refused

    async def _call[T](self, what: str, index: str, request: Awaitable[T]) -> T:
        """Await one client call, mapping its failures.

        The failure is raised outside the handler, unchained, so neither its message nor its
        context can carry the request and its credentials.
        """
        error: IngestionError
        try:
            return await request
        except SSLError as exc:
            error = SearchRejected(
                f"OpenSearch's certificate was refused ({type(exc).__name__}); cannot {what} "
                f"{index}; check OKP_OPENSEARCH_URL and the certificate"
            )
        except OpenSearchConnectionError as exc:  # a timeout is one of these
            error = SearchUnavailable(
                f"OpenSearch {what} {index} failed ({type(exc).__name__}); rerun"
            )
        except AuthenticationException:
            error = SearchRejected(
                f"OpenSearch refused the user or password; cannot {what} {index}"
            )
        except AuthorizationException:
            error = SearchRejected(
                f"OpenSearch denied the user to {what} {index}; check its permissions"
            )
        except TransportError as exc:
            status = exc.status_code
            failure = (
                f"OpenSearch {what} {index} failed with status {status}: "
                f"{_said(exc.info, exc.error)}"
            )
            if status in TRANSIENT_STATUSES:
                error = SearchUnavailable(f"{failure}; rerun")
            else:
                error = SearchRejected(failure)
        except (SerializationError, RecursionError):  # a 2xx body that does not parse
            error = SearchRejected(f"OpenSearch {what} {index} failed: {NOT_OPENSEARCH}")
        raise error


def _refusal(item: object, sent: Collection[str]) -> tuple[str, int, str] | None:
    """A refused item's id, status and reason, or None when applied, as a not found delete is.

    The id is named only when the batch sent it, since the answer's text is not trusted.
    """
    if not isinstance(item, dict):
        return "unknown", 0, "an item that is not an object"
    results = list(cast("dict[str, object]", item).values())
    result = results[0] if results else None
    if not isinstance(result, dict):
        return "unknown", 0, "an item without a result"
    found = cast("dict[str, object]", result)
    error = found.get("error")
    if error is None:
        return None
    id_ = found.get("_id")
    status = found.get("status")
    if isinstance(status, bool) or not isinstance(status, int):
        status = 0
    return id_ if isinstance(id_, str) and id_ in sent else "unknown", status, _reason(error)


def _said(info: object, error: object) -> str:
    """What OpenSearch said, as its error type and the first line of its reason, cut short.

    opensearch-py 3.2's ``AsyncHttpConnection`` raises without the answer's content type, so
    ``info`` stays empty and ``error`` holds the answer's whole text. That text is read only
    when it parses as an OpenSearch error object; the wrong host could fill any other answer
    with the request itself, so such an answer is only named, never repeated.
    """
    if not isinstance(info, dict) and isinstance(error, str):
        try:
            info = json.loads(error)
        except (ValueError, RecursionError):
            info = None
    if isinstance(info, dict):
        found = cast("dict[str, object]", info).get("error")
        if found is not None:
            return _reason(found)
    return NOT_OPENSEARCH


def _reason(error: object) -> str:
    if isinstance(error, dict):
        detail = cast("dict[str, object]", error)
        kind = _ERROR_TYPE.match(str(detail.get("type", "")))
        reason = detail.get("reason")
        text = _line(reason) if isinstance(reason, str) else "no reason given"
        return f"{kind.group() if kind else 'error'}: {text}"
    if isinstance(error, str):
        return _line(error)
    return "an error that is not an object"


def _line(text: str) -> str:
    return (text.strip().splitlines() or [""])[0][:_REASON_LIMIT]
