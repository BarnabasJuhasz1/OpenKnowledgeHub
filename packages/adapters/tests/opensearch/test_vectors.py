"""Exercise the real pinned client serialization without a server/socket."""

import json
from collections.abc import AsyncGenerator, Collection, Mapping
from typing import Any, cast

import pytest
from opensearchpy import AsyncConnection, AsyncOpenSearch

from openknowledge_adapters.opensearch.vectors import OpenSearchVectors
from openknowledge_contracts.embeddings import EmbeddingMutation
from openknowledge_core.search.errors import SearchRejected, SearchUnavailable
from openknowledge_core.search.vector_models import SearchVectorNeighbors
from openknowledge_core.search.vectors import VectorPublicationService
from tests.fakes.vector_publication import SOURCE, publication, record

ROWS = (record(1), record(2), record(3, deleted=True), record(2**64 - 1))
PUBLICATION = publication(ROWS)
INDEX = PUBLICATION.index
SHARDS = {"total": 1, "successful": 1, "failed": 0}


class Engine:
    """Protocol response fixture, not a simulation of ANN correctness or OpenSearch state."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str, object]] = []
        self.metadata: dict[str, Any] = {}
        self.documents: dict[str, dict[str, Any]] = {}
        self.blocked = False
        self.exists = False
        self.fail: str | None = None

    def respond(self, method: str, url: str, body: bytes | None) -> tuple[int, object]:
        if url.endswith("/_bulk"):
            assert body is not None
            lines = [json.loads(line) for line in body.splitlines()]
            self.requests.append((method, url, lines))
            items: list[dict[str, Any]] = []
            for action, document in zip(lines[::2], lines[1::2], strict=True):
                assert set(action) == {"create"}
                id_ = action["create"]["_id"]
                status = 429 if self.fail == "busy" else 201
                if id_ in self.documents or self.blocked:
                    status = 409
                if status == 201:
                    self.documents[id_] = document
                items.append({"create": {"_index": INDEX, "_id": id_, "status": status}})
            return 200, {
                "errors": any(item["create"]["status"] != 201 for item in items),
                "items": items,
            }
        value: dict[str, Any] = json.loads(body) if body else {}
        self.requests.append((method, url, value))
        if method == "PUT" and url == f"/{INDEX}":
            if self.exists:
                return 400, {"error": "already exists: sensitive marker"}
            self.exists = True
            self.metadata = value["mappings"]["_meta"]
            return 200, {"acknowledged": True, "shards_acknowledged": True}
        if url.endswith("/_mapping"):
            if method == "PUT":
                self.metadata = value["_meta"]
                return 200, {"acknowledged": True}
            return 200, {INDEX: {"mappings": {"_meta": self.metadata}}}
        if url.endswith("/_block/write"):
            self.blocked = True
            return 200, {
                "acknowledged": True,
                "shards_acknowledged": self.fail != "block",
                "indices": [{"name": INDEX, "blocked": True}],
            }
        if url.endswith("/_refresh"):
            shards = {**SHARDS, "successful": 0, "failed": 1} if self.fail == "shards" else SHARDS
            return 200, {"_shards": shards}
        if url.endswith("/_count"):
            count = (
                len(self.documents)
                if not body
                else sum(not row["is_deleted"] for row in self.documents.values())
            )
            if self.fail == "count":
                count += 1
            return 200, {"count": count, "_shards": SHARDS}
        if url.endswith("/_settings"):
            return 200, {INDEX: {"settings": {"index.blocks.write": str(self.blocked).lower()}}}
        if url.endswith("/_mget"):
            id_ = value["ids"][0]
            doc: dict[str, object] = {"_index": INDEX, "_id": id_, "found": id_ in self.documents}
            if id_ in self.documents:
                doc["_source"] = self.documents[id_]
            return 200, {"docs": [doc]}
        if url.endswith("/_search"):
            return 200, {
                "timed_out": self.fail == "timeout",
                "_shards": SHARDS,
                "hits": {"hits": [{"_index": INDEX, "_id": "2", "_score": 1.0}]},
            }
        raise AssertionError((method, url))

    def connection(self) -> type[AsyncConnection]:
        engine = self

        class Connection(AsyncConnection):
            async def perform_request(
                self,
                method: str,
                url: str,
                params: Mapping[str, Any] | None = None,
                body: bytes | None = None,
                timeout: int | float | None = None,  # noqa: ASYNC109
                ignore: Collection[int] = (),
                headers: Mapping[str, str] | None = None,
            ) -> tuple[int, dict[str, str], str]:
                if url.endswith("/_search"):
                    assert params is not None
                    assert params.get("allow_partial_search_results") in (False, "false")
                status, answer = engine.respond(method, url, body)
                if status >= 400:
                    self._raise_error(status, json.dumps(answer))
                return status, {"content-type": "application/json"}, json.dumps(answer)

            async def close(self) -> None:
                return None

        return Connection


async def rows() -> AsyncGenerator[EmbeddingMutation]:
    for row in ROWS:
        yield row


async def test_publish_seals_then_reads_separate_vector_index_with_real_client() -> None:
    engine = Engine()
    async with AsyncOpenSearch(connection_class=engine.connection(), max_retries=0) as client:
        service = VectorPublicationService(OpenSearchVectors(client))
        assert await service.publish(PUBLICATION, rows()) == PUBLICATION
        assert engine.blocked and engine.metadata["state"] == "complete"
        created = cast("dict[str, Any]", engine.requests[0][2])
        assert created["settings"]["index"]["knn.derived_source.enabled"] is False
        assert created["mappings"]["_source"] == {"enabled": True}
        assert "vector" not in engine.documents["3"]
        command = SearchVectorNeighbors(
            publication=PUBLICATION, expected_source=SOURCE, seed=ROWS[0], k=1
        )
        assert (await service.neighbors(command)).hits[0].corpusid == 2
    assert all(INDEX in path for _, path, _ in engine.requests)
    search = cast("dict[str, Any]", engine.requests[-1][2])
    assert search["query"]["knn"]["vector"]["filter"]["bool"]["filter"] == [
        {"term": {"is_deleted": False}}
    ]


@pytest.mark.parametrize(
    "failure", ["incomplete", "unblocked", "evidence", "provenance", "count", "timeout"]
)
async def test_reader_fails_closed_before_serving_untrusted_results(failure: str) -> None:
    engine = Engine()
    async with AsyncOpenSearch(connection_class=engine.connection(), max_retries=0) as client:
        service = VectorPublicationService(OpenSearchVectors(client))
        await service.publish(PUBLICATION, rows())
        if failure == "incomplete":
            engine.metadata["state"] = "building"
        elif failure == "unblocked":
            engine.blocked = False
        elif failure == "evidence":
            engine.metadata["vector_publication"]["resolved"]["completeness_evidence"] = "other"
        elif failure == "provenance":
            engine.documents["1"]["provenance"] = "other"
        else:
            engine.fail = failure
        start = len(engine.requests)
        with pytest.raises(SearchRejected):
            await service.neighbors(
                SearchVectorNeighbors(publication=PUBLICATION, expected_source=SOURCE, seed=ROWS[0])
            )
        if failure != "timeout":
            assert not any(path.endswith("/_search") for _, path, _ in engine.requests[start:])


@pytest.mark.parametrize("deleted", [True, False])
async def test_absent_or_tombstoned_seed_does_not_replay_old_live_vector(deleted: bool) -> None:
    engine = Engine()
    async with AsyncOpenSearch(connection_class=engine.connection(), max_retries=0) as client:
        service = VectorPublicationService(OpenSearchVectors(client))
        await service.publish(PUBLICATION, rows())
        seed = record(3 if deleted else 99)
        result = await service.neighbors(
            SearchVectorNeighbors(publication=PUBLICATION, expected_source=SOURCE, seed=seed)
        )
        assert result.hits == ()
        assert not any(path.endswith("/_search") for _, path, _ in engine.requests)


async def test_replay_cannot_overwrite_completed_index_or_make_busy_attempt_complete() -> None:
    engine = Engine()
    async with AsyncOpenSearch(connection_class=engine.connection(), max_retries=0) as client:
        service = VectorPublicationService(OpenSearchVectors(client))
        await service.publish(PUBLICATION, rows())
        with pytest.raises(SearchRejected) as raised:
            await service.publish(PUBLICATION, rows())
        assert "sensitive marker" not in str(raised.value)
        assert engine.documents["3"]["is_deleted"] is True
        with pytest.raises(SearchRejected):
            await OpenSearchVectors(client).write(PUBLICATION, (record(3),))
        assert engine.documents["3"]["is_deleted"] is True
    engine = Engine()
    engine.fail = "busy"
    async with AsyncOpenSearch(connection_class=engine.connection(), max_retries=0) as client:
        with pytest.raises(SearchUnavailable):
            await VectorPublicationService(OpenSearchVectors(client)).publish(PUBLICATION, rows())
    assert engine.metadata["state"] == "building" and not engine.blocked


@pytest.mark.parametrize("failure", ["count", "shards", "block"])
async def test_failed_seal_never_persists_completion(failure: str) -> None:
    engine = Engine()
    engine.fail = failure
    async with AsyncOpenSearch(connection_class=engine.connection(), max_retries=0) as client:
        with pytest.raises(SearchRejected):
            await VectorPublicationService(OpenSearchVectors(client)).publish(PUBLICATION, rows())
    assert engine.metadata["state"] == "building"
