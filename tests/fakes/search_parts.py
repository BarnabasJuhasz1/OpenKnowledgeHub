"""Spawn-safe search workloads; no services or production credentials required."""

import asyncio
import json
import os
import time
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path

from opensearchpy.serializer import JSONSerializer

from openknowledge_adapters.clickhouse.search import PAPER_FIELDS, source_rows
from openknowledge_core.search import SearchIndexService, SourceRows
from openknowledge_core.search.commands import IndexPapers, IndexPapersResult
from openknowledge_core.search.models import BulkOperation, DeleteDocument
from tests.fakes.ports import FakeSearchSink, FakeSearchSource, paper_row


class GeneratedSource(FakeSearchSource):
    async def base(self, release: str, low: int, high: int) -> AsyncGenerator[SourceRows]:
        async def rows() -> AsyncGenerator[SourceRows]:
            paper = paper_row(
                1,
                41000,
                title="Parallel indexing of scientific literature — a study",
                authors=tuple({"name": f"Author {i}"} for i in range(5)),
                externalids={"DOI": "10.1234/example", "ArXiv": "2601.00001"},
                s2fieldsofstudy=("Computer Science", "Mathematics"),
                citationcount=42,
            ).model_dump(mode="json")
            joined = {
                **{name: paper[name] for name in PAPER_FIELDS},
                "paper_version": 41000,
                "paper_release": release,
                "paper_deleted": False,
                "abstract_version": 41000,
                "abstract_release": release,
                "abstract_deleted": False,
                "abstract": "Scientific abstract text. " * 80,
                "tldr_version": 41000,
                "tldr_release": release,
                "tldr_deleted": False,
                "tldr": "A short summary.",
            }
            for corpusid in range(low, high + 1):
                yield source_rows(json.loads(json.dumps({**joined, "corpusid": corpusid})))

        return rows()


class SerializingSink(FakeSearchSink):
    async def bulk(self, index: str, operations: Sequence[BulkOperation]) -> None:
        serializer = JSONSerializer()
        for operation in operations:
            serializer.dumps(
                {
                    "index": {
                        "_index": index,
                        "_id": operation.id,
                        "version": operation.version,
                        "version_type": "external_gte",
                    }
                }
            )
            if not isinstance(operation, DeleteDocument):
                serializer.dumps(operation.document.source())


async def generated_index(command: IndexPapers) -> IndexPapersResult:
    source, sink = GeneratedSource(), SerializingSink()
    sink.indices[command.index] = {}
    return await SearchIndexService(source=source, sink=sink, shards=1).index(command)


def index_generated(command: IndexPapers) -> IndexPapersResult:
    return asyncio.run(generated_index(command))


def failed_worker(command: IndexPapers) -> IndexPapersResult:
    os._exit(17)


def delayed_worker(directory: str, command: IndexPapers) -> IndexPapersResult:
    Path(directory, f"{command.part}.started").write_text(str(os.getpid()))
    time.sleep(0.5)
    Path(directory, f"{command.part}.finished").touch()
    return IndexPapersResult(
        index=command.index,
        release=command.release,
        from_release=None,
        part=command.part,
        parts=command.parts,
        created=False,
        written=1,
        deleted=0,
        requests=1,
    )


def crash_with_sibling(directory: str, mode: str, command: IndexPapers) -> IndexPapersResult:
    import signal

    Path(directory, f"{command.part}.started").write_text(str(os.getpid()))
    deadline = time.monotonic() + 15
    while not all(Path(directory, f"{part}.started").exists() for part in (1, 2)):
        if time.monotonic() > deadline:
            raise RuntimeError("Sibling did not start")
        time.sleep(0.01)
    if command.part == 1:
        if mode == "sigkill":
            os.kill(os.getpid(), signal.SIGKILL)
        os._exit(17)
    # Pool failure must terminate this child and join it, rather than just resolve futures.
    time.sleep(30)
    raise RuntimeError("Sibling was not terminated")


def domain_failure(command: IndexPapers) -> IndexPapersResult:
    from openknowledge_core.search.errors import SearchUnavailable

    raise SearchUnavailable("corpusid 12: overloaded; rerun")
