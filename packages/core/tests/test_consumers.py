import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import aclosing

import pytest

from openknowledge_contracts.manifests import ObjectRef
from openknowledge_core.ingestion.archive_progress import ArchiveProgress, completion_path
from openknowledge_core.ingestion.consumers import (
    ArchiveConsumer,
    ConsumeBase,
    EmbeddingPaper,
    FulltextPaper,
    parse_paper,
)
from openknowledge_core.ingestion.errors import IngestionError
from openknowledge_core.ingestion.models import ByteStream
from openknowledge_core.ingestion.service import DownloadDatasetService
from tests.fakes.ports import PRODUCER, FakeSource, FakeStore, clock, gz

FULLTEXT = {
    "corpusid": 12,
    "title": None,
    "authors": ["A. Example"],
    "openaccessinfo": {"license": None, "extra": "retained"},
    "body": {"text": "Synthetic text.", "annotations": {"sentence": '[{"start":"0","end":15}]'}},
    "bibliography": {"text": "", "annotations": {"bib_id": None}},
}
EMBEDDING = {"corpusid": 34, "model": "specter_v2", "vector": json.dumps([0.25] * 768)}


async def completed(
    command: ConsumeBase,
    store: FakeStore,
    payload: bytes,
) -> None:
    source = FakeSource()
    source.add(command.dataset, "part.gz", payload)
    service = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
    )
    pinned = await service.pin_dataset(command)
    await service.archive_shard(pinned, pinned.shards[0])
    await ArchiveProgress(store, service, PRODUCER, clock).finalize(command)


class TrackedStore(FakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.payload_chunks = 0
        self.closed = 0

    async def open_read(self, ref: ObjectRef) -> ByteStream:
        original = await super().open_read(ref)
        if not ref.path.name.endswith(".gz"):
            return original

        async def chunks() -> AsyncGenerator[bytes]:
            try:
                async with aclosing(original.chunks):
                    async for block in original.chunks:
                        for start in range(0, len(block), 7):
                            self.payload_chunks += 1
                            yield block[start : start + 7]
            finally:
                self.closed += 1

        return ByteStream(chunks=chunks(), expected_size=original.expected_size)


@pytest.mark.parametrize(
    "dataset,payload", [("s2orc_v2", FULLTEXT), ("embeddings-specter_v2", EMBEDDING)]
)
async def test_completed_base_streams_with_lineage_and_closes_early(
    dataset: str,
    payload: dict[str, object],
) -> None:
    command = ConsumeBase(dataset=dataset, release="2026-10-06", bucket="raw", max_records=1)
    store = TrackedStore()
    # Independent gzip members make it observable that later records were not fetched.
    data = gz((json.dumps(payload) + "\n").encode()) * 20
    await completed(command, store, data)
    before = dict(store.objects)
    async with aclosing(ArchiveConsumer(store).stream(command)) as stream:
        row = await anext(stream)
        assert row.paper.corpusid == payload["corpusid"]
        assert row.source.dataset == dataset and row.source.release == command.release
        assert row.source.object_ref.generation > 0 and row.source.line == 1
        assert row.source.completion.ref.path == completion_path(command)
        assert row.source.manifest.ref.path.name.endswith("manifest.json")
    assert store.closed == 1
    assert store.payload_chunks * 7 < len(data)
    assert store.objects == before
    # Exhaustion exercises the original integrity check across all concatenated members.
    whole = command.model_copy(update={"max_records": None})
    assert len([row async for row in ArchiveConsumer(store).stream(whole)]) == 20


async def test_completion_gate_and_malformed_payload_cleanup() -> None:
    store = TrackedStore()
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-06", bucket="raw")
    with pytest.raises(IngestionError, match="missing archive object"):
        await anext(ArchiveConsumer(store).stream(command))
    assert store.payload_chunks == 0
    await completed(command, store, gz(b'{"corpusid":1,"body":null}\n'))
    with pytest.raises(IngestionError, match="line 1.*body"):
        await anext(ArchiveConsumer(store).stream(command))
    assert store.closed == 1


async def test_record_bound_and_cancelled_consumer_release_stream() -> None:
    store = TrackedStore()
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-06", bucket="raw")
    await completed(command, store, gz((json.dumps(FULLTEXT) + "\n").encode()))
    with pytest.raises(IngestionError, match="max_record_bytes=10"):
        await anext(
            ArchiveConsumer(store).stream(command.model_copy(update={"max_record_bytes": 10}))
        )
    entered = asyncio.Event()

    async def consume() -> None:
        async with aclosing(ArchiveConsumer(store).stream(command)) as stream:
            await anext(stream)
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(consume())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.closed == 2


def test_observed_payloads_preserve_metadata_and_reject_unsupported_shapes() -> None:
    paper = parse_paper("s2orc_v2", json.dumps(FULLTEXT).encode())
    assert isinstance(paper, FulltextPaper)
    assert paper.body.annotations == {"sentence": '[{"start":"0","end":15}]'}
    embedding = parse_paper("embeddings-specter_v2", json.dumps(EMBEDDING).encode())
    assert isinstance(embedding, EmbeddingPaper) and embedding.vector == [0.25] * 768
    for invalid in (
        {**EMBEDDING, "model": "unknown"},
        {**EMBEDDING, "vector": "[1]"},
        {**EMBEDDING, "vector": json.dumps([float("nan")] * 768)},
        {**EMBEDDING, "schema_version": 2},
        {**EMBEDDING, "corpusid": True},
    ):
        with pytest.raises(IngestionError):
            parse_paper("embeddings-specter_v2", json.dumps(invalid).encode())
