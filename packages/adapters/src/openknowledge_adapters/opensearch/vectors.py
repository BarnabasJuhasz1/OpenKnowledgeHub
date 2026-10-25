"""Separate create-only vector publications, with persisted completion/read gates."""

from __future__ import annotations

import json
from collections.abc import Awaitable
from typing import Any, cast

from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from opensearchpy.exceptions import SerializationError, SSLError, TransportError
from pydantic import ValidationError

from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_core.ingestion.errors import TRANSIENT_STATUSES
from openknowledge_core.search.errors import SearchRejected, SearchUnavailable
from openknowledge_core.search.vector_models import (
    SearchVectorNeighbors,
    VectorHit,
    VectorNeighbors,
    VectorPublication,
)


class OpenSearchVectors:
    """One exclusively owned index per attempt. Caller owns the per-run client lifecycle.

    No alias/selection mutation, cleanup, adoption or append-to-complete operation is
    provided. A failed/ambiguous write abandons the attempt; use a new name on retry.
    """

    def __init__(self, client: AsyncOpenSearch) -> None:
        self._client = client

    async def begin(self, publication: VectorPublication) -> None:
        properties: dict[str, Any] = {
            "source": {"type": "object", "enabled": False},
            "corpusid": {"type": "keyword"},
            "sequence": {"type": "keyword", "index": False},
            "provenance": {"type": "keyword", "index": False},
            "is_deleted": {"type": "boolean"},
            "vector": {
                "type": "knn_vector",
                "dimension": publication.resolved.source.dimension,
                "data_type": "float",
                "method": {"name": "hnsw", "engine": "lucene", "space_type": "cosinesimil"},
            },
        }
        answer = await self._request(
            self._client.indices.create(
                index=publication.index,
                body={
                    "settings": {
                        "index": {
                            "knn": True,
                            "knn.derived_source.enabled": False,
                            "number_of_shards": 1,
                            "number_of_replicas": 0,
                            "refresh_interval": "-1",
                        }
                    },
                    "mappings": {
                        "dynamic": "strict",
                        "_source": {"enabled": True},
                        "_meta": _metadata(publication, "building"),
                        "properties": properties,
                    },
                },
            )
        )
        _acknowledged(answer)
        if answer.get("shards_acknowledged") is not True:
            raise SearchRejected("vector index shards were not acknowledged; abandon attempt")

    async def write(
        self, publication: VectorPublication, records: tuple[EmbeddingMutation, ...]
    ) -> None:
        if not records or len(records) > 128:
            raise SearchRejected("vector writes require 1 to 128 records")
        lines: list[str] = []
        for record in records:
            if record.source != publication.resolved.source or not record.provenance.strip():
                raise SearchRejected("vector write source or provenance mismatch")
            lines.append(json.dumps({"create": {"_id": str(record.corpusid)}}))
            document = record.model_dump(mode="json")
            # Empty arrays are not vectors. Tombstones remain stored, but never in ANN.
            if record.is_deleted:
                del document["vector"]
            encoded = json.dumps(document, separators=(",", ":"))
            if len(encoded.encode("utf-8")) > 64 * 1024:
                raise SearchRejected("vector document exceeds 64 KiB")
            lines.append(encoded)
        answer = await self._request(
            self._client.bulk(index=publication.index, body="\n".join(lines) + "\n")
        )
        items = _array(answer.get("items"))
        if len(items) != len(records):
            raise SearchRejected("vector bulk did not answer every record")
        for record, raw in zip(records, items, strict=True):
            item = _object(_object(raw).get("create"))
            if item.get("_id") != str(record.corpusid) or item.get("_index") != publication.index:
                raise SearchRejected("vector bulk answered a different record or index")
            if item.get("status") == 429:
                raise SearchUnavailable(
                    "vector bulk was busy; abandon attempt and retry a new index"
                )
            if item.get("status") != 201 or item.get("error") is not None:
                raise SearchRejected("vector create failed or conflicted; abandon attempt")
        if answer.get("errors") is not False:
            raise SearchRejected("vector bulk did not confirm success")

    async def complete(self, publication: VectorPublication) -> None:
        await self._identity(publication, "building")
        answer = await self._request(
            self._client.indices.add_block(index=publication.index, block="write")
        )
        _acknowledged(answer)
        if answer.get("shards_acknowledged") is not True or answer.get("indices") != [
            {"name": publication.index, "blocked": True}
        ]:
            raise SearchRejected("vector write block not confirmed on every shard")
        refreshed = await self._request(self._client.indices.refresh(index=publication.index))
        _shards(refreshed)
        await self._counts(publication)
        _acknowledged(
            await self._request(
                self._client.indices.put_mapping(
                    index=publication.index, body={"_meta": _metadata(publication, "complete")}
                )
            )
        )
        await self._selected(publication)

    async def neighbors(self, command: SearchVectorNeighbors) -> VectorNeighbors:
        publication = command.publication
        await self._selected(publication)
        # Do not send a query vector until its exact archived provenance has been checked.
        answer = await self._request(
            self._client.mget(index=publication.index, body={"ids": [str(command.seed.corpusid)]})
        )
        docs = _array(answer.get("docs"))
        if len(docs) != 1:
            raise SearchRejected("vector seed lookup returned an incomplete answer")
        doc = _object(docs[0])
        if (
            doc.get("_index") != publication.index
            or doc.get("_id") != str(command.seed.corpusid)
            or "error" in doc
        ):
            raise SearchRejected("vector seed lookup identity mismatch or error")
        if doc.get("found") is False:
            return VectorNeighbors(publication=publication, hits=())
        if doc.get("found") is not True:
            raise SearchRejected("vector seed lookup omitted found status")
        stored = _record(doc.get("_source"))
        if stored.source != command.expected_source or stored.corpusid != command.seed.corpusid:
            raise SearchRejected("stored vector seed source mismatch")
        if stored.is_deleted:
            return VectorNeighbors(publication=publication, hits=())
        if stored != command.seed:
            raise SearchRejected("query seed differs from archived vector or provenance")
        answer = await self._request(
            self._client.search(
                index=publication.index,
                params={"allow_partial_search_results": "false"},
                body={
                    "size": command.k,
                    "_source": False,
                    "query": {
                        "knn": {
                            "vector": {
                                "vector": list(command.seed.vector),
                                "k": command.k,
                                "filter": {
                                    "bool": {
                                        "filter": [{"term": {"is_deleted": False}}],
                                        "must_not": [{"ids": {"values": [str(stored.corpusid)]}}],
                                    }
                                },
                            }
                        }
                    },
                },
            )
        )
        if answer.get("timed_out") is not False:
            raise SearchRejected("vector search timed out or omitted completion status")
        _shards(answer)
        raw_hits = _array(_object(answer.get("hits")).get("hits"))
        if len(raw_hits) > command.k:
            raise SearchRejected("vector search returned malformed hits")
        hits: list[VectorHit] = []
        seen: set[int] = set()
        for raw in raw_hits:
            hit = _object(raw)
            id_ = hit.get("_id")
            if (
                not isinstance(id_, str)
                or len(id_) > 20
                or not id_.isascii()
                or not id_.isdecimal()
            ):
                raise SearchRejected("vector hit omitted a canonical corpusid")
            corpusid = int(id_)
            if str(corpusid) != id_ or corpusid in seen or corpusid == stored.corpusid:
                raise SearchRejected("vector hit repeated, noncanonical or self")
            if hit.get("_index") != publication.index:
                raise SearchRejected("vector hit came from another index")
            hits.append(_hit({"corpusid": corpusid, "score": hit.get("_score")}))
            seen.add(corpusid)
        return VectorNeighbors(publication=publication, hits=tuple(hits))

    async def _identity(self, publication: VectorPublication, state: str) -> None:
        mapping = await self._request(self._client.indices.get_mapping(index=publication.index))
        if set(mapping) != {publication.index}:
            raise SearchRejected("vector selection requires exactly one physical index")
        found = _object(_object(mapping[publication.index]).get("mappings"))
        if found.get("_meta") != _metadata(publication, state):
            raise SearchRejected("vector publication identity, evidence or completion mismatch")

    async def _selected(self, publication: VectorPublication) -> None:
        await self._identity(publication, "complete")
        settings = await self._request(
            self._client.indices.get_settings(
                index=publication.index, params={"flat_settings": "true"}
            )
        )
        if set(settings) != {publication.index}:
            raise SearchRejected("vector settings resolved another index")
        found = _object(_object(settings[publication.index]).get("settings"))
        if found.get("index.blocks.write") not in (True, "true"):
            raise SearchRejected("selected vector index is not write-blocked")
        await self._counts(publication)

    async def _counts(self, publication: VectorPublication) -> None:
        count = await self._request(self._client.count(index=publication.index))
        _shards(count)
        if type(count.get("count")) is not int or count["count"] != publication.resolved.records:
            raise SearchRejected("vector stored count differs from resolved snapshot")
        live = await self._request(
            self._client.count(
                index=publication.index, body={"query": {"term": {"is_deleted": False}}}
            )
        )
        _shards(live)
        if type(live.get("count")) is not int or live["count"] != publication.resolved.live_records:
            raise SearchRejected("vector stored live count differs from resolved snapshot")

    async def _request(self, request: Awaitable[Any]) -> dict[str, Any]:
        error: Exception
        try:
            return _object(await request)
        except SSLError:
            error = SearchRejected("vector index certificate refused; check configuration")
        except OpenSearchConnectionError:
            error = SearchUnavailable("vector index connection failed; abandon write attempt")
        except TransportError as exc:
            error = (
                SearchUnavailable("vector index temporarily unavailable; retry a new write attempt")
                if exc.status_code in TRANSIENT_STATUSES
                else SearchRejected("vector index request refused; check configuration/permissions")
            )
        except (SerializationError, RecursionError):
            error = SearchRejected("vector index returned an invalid response")
        # No transport payload, URL, request vector, credential or error reason is repeated.
        raise error


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SearchRejected("vector index response has an invalid object")
    return cast("dict[str, Any]", value)


def _metadata(publication: VectorPublication, state: str) -> dict[str, Any]:
    return {"vector_publication": publication.model_dump(mode="json"), "state": state}


def _acknowledged(answer: dict[str, Any]) -> None:
    if answer.get("acknowledged") is not True:
        raise SearchRejected("vector index operation was not acknowledged")


def _shards(answer: dict[str, Any]) -> None:
    shards = _object(answer.get("_shards"))
    total, successful, failed = (shards.get(key) for key in ("total", "successful", "failed"))
    if (
        type(total) is not int
        or type(successful) is not int
        or type(failed) is not int
        or total < 1
        or successful != total
        or failed != 0
    ):
        raise SearchRejected("vector index operation did not complete on every shard")


def _record(raw: object) -> EmbeddingMutation:
    try:
        return EmbeddingMutation.model_validate(raw)
    except ValidationError:
        pass
    raise SearchRejected("stored vector record is invalid")


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise SearchRejected("vector index response has an invalid array")
    return cast("list[object]", value)


def _hit(raw: object) -> VectorHit:
    try:
        return VectorHit.model_validate(raw)
    except ValidationError:
        pass
    raise SearchRejected("vector hit has invalid id or score")
