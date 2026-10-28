"""Completed archive conversion across actual streaming storage and Parquet boundaries."""

import json
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager
from io import BytesIO
from typing import Literal

import pyarrow.parquet as pq
import pytest

from openknowledge_adapters import gcs
from openknowledge_adapters.parquet import StoredParquetArtifacts
from openknowledge_contracts.embedding_artifacts import ConversionManifest
from openknowledge_core.embedding_conversion.models import ConversionPlan, ConvertEmbeddings
from openknowledge_core.embedding_conversion.service import (
    EmbeddingConversionService,
    artifact_path,
)
from openknowledge_core.ingestion.archive_progress import ArchiveProgress, HashedRef
from openknowledge_core.ingestion.commands import DownloadDiff
from openknowledge_core.ingestion.errors import ArchiveConflict, ObjectChanged
from openknowledge_core.ingestion.ports import ArtifactStore
from openknowledge_core.ingestion.service import DownloadDatasetService
from tests.fakes.gcs_json_api import FakeGcs, running
from tests.fakes.ports import PRODUCER, FakeSource, FakeStore, clock, gz


def rows(*ids: int) -> bytes:
    return b"".join(
        json.dumps(
            {
                "corpusid": cid,
                "model": "specter_v2",
                "vector": json.dumps([0.1] * 768),
            }
        ).encode()
        + b"\n"
        for cid in ids
    )


@asynccontextmanager
async def conversion_store(
    http: bool,
) -> AsyncGenerator[tuple[FakeStore | ArtifactStore, FakeGcs | None]]:
    if not http:
        yield FakeStore(), None
        return
    async with (
        running(FakeGcs()) as server,
        gcs.connect(
            gcs.GcsConfig(
                api_root=server.base_url,
                request_timeout_seconds=30,
                min_transfer_rate=1024**2,
            )
        ) as store,
    ):
        yield store, server


@pytest.mark.parametrize("http", [False, True])
@pytest.mark.parametrize("part", [None, "updates", "deletes"])
async def test_completed_parts_replay_and_output_integrity(
    part: Literal["updates", "deletes"] | None,
    http: bool,
) -> None:
    command = ConvertEmbeddings(release="2026-10-13", bucket="raw")
    source = FakeSource()
    source.add(command.dataset, "part.gz", gz(rows(2**64 - 1, 7, 7)))
    async with conversion_store(http) as (store, server):
        downloads = DownloadDatasetService(
            catalog=source,
            downloads=source,
            store=store,
            producer=PRODUCER,
            clock=clock,
        )
        progress = ArchiveProgress(store, downloads, PRODUCER, clock)
        base = await downloads.pin_dataset(command)
        await downloads.archive_shard(base, base.shards[0])
        anchor = await progress.finalize(command)
        if part is not None:
            source.add_diff(
                command.dataset,
                command.release,
                "2026-10-20",
                {"update.gz": gz(rows(8, 8))},
                {"delete.gz": gz(b'{"corpusid":7}\n{"corpusid":7}\n')},
            )
            diff = DownloadDiff(
                release="2026-10-20",
                from_release=command.release,
                dataset=command.dataset,
                bucket=command.bucket,
            )
            pinned = await downloads.pin_diff(diff)
            for selection in (pinned.updates, pinned.deletes):
                for shard in selection.shards:
                    await downloads.archive_shard(selection, shard)
            completion = await progress.finalize(diff, anchor)
            command = ConvertEmbeddings(**diff.model_dump(), part=part, completion=completion)
        opened = list(source.opened)
        listings = source.listings
        service = EmbeddingConversionService(store, StoredParquetArtifacts(store))
        plan = await service.prepare(command)
        with pytest.raises(ArchiveConflict):
            await service.complete(plan)
        assert await store.stat(artifact_path(plan, "complete.json")) is None
        artifact = await service.convert_shard(plan, 0)
        assert await service.convert_shard(plan, 0) == artifact
        completed = await service.complete(plan)
        manifest_ref = await service.reader.read(completed, HashedRef)
        manifest = await service.reader.read(manifest_ref, ConversionManifest)
        assert manifest.entries == (artifact,)
        async with aclosing(service.parquet.read(artifact)) as stream:
            decoded = [row async for row in stream]
        assert [row.source_line for row in decoded] == list(range(1, artifact.receipt.rows + 1))
        assert all((row.vector is None) == (part == "deletes") for row in decoded)
        data = (
            store.data(artifact.output.path)
            if isinstance(store, FakeStore)
            else server.data(artifact.output.path.bucket, artifact.output.path.name)
            if server is not None
            else b""
        )
        table = pq.ParquetFile(BytesIO(data)).read()  # pyright: ignore[reportUnknownMemberType]
        assert table.column("corpusid").to_pylist() == (
            [7, 7] if part == "deletes" else [8, 8] if part == "updates" else [2**64 - 1, 7, 7]
        )
        assert table.column_names == (
            ["corpusid", "source_line"]
            if part == "deletes"
            else ["corpusid", "source_line", "vector"]
        )
        assert table.column("source_line").to_pylist() == list(range(1, table.num_rows + 1))
        assert source.opened == opened and source.listings == listings
        if isinstance(store, FakeStore):
            store.put(artifact.output.path, b"corrupt")
        else:
            assert server is not None
            server.put_object(artifact.output.path.bucket, artifact.output.path.name, b"corrupt")
        with pytest.raises((ArchiveConflict, ObjectChanged)):
            await service.convert_shard(plan, 0)
        with pytest.raises((ArchiveConflict, ObjectChanged)):
            await service.complete(plan)


async def test_failed_source_never_publishes_shard_or_completion() -> None:
    from tests.fakes.ports import FakeStore

    store = FakeStore()
    source = FakeSource()
    command = ConvertEmbeddings(release="2026-10-13", bucket="raw")
    source.add(command.dataset, "part.gz", gz(b'{"corpusid":7}\n'))
    downloads = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
    )
    base = await downloads.pin_dataset(command)
    await downloads.archive_shard(base, base.shards[0])
    await ArchiveProgress(store, downloads, PRODUCER, clock).finalize(command)
    service = EmbeddingConversionService(store, StoredParquetArtifacts(store))
    plan = await service.prepare(command)
    from openknowledge_core.ingestion.errors import IngestionError

    with pytest.raises(IngestionError):
        await service.convert_shard(plan, 0)
    assert await store.stat(artifact_path(plan, "0.json")) is None
    with pytest.raises(ArchiveConflict):
        await service.complete(plan)
    assert await store.stat(artifact_path(plan, "complete.json")) is None


async def memory_plan() -> tuple[EmbeddingConversionService, ConversionPlan, FakeStore]:
    store = FakeStore()
    source = FakeSource()
    command = ConvertEmbeddings(release="2026-10-13", bucket="raw")
    source.add(command.dataset, "part.gz", gz(rows(7)))
    downloads = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
    )
    base = await downloads.pin_dataset(command)
    await downloads.archive_shard(base, base.shards[0])
    await ArchiveProgress(store, downloads, PRODUCER, clock).finalize(command)
    service = EmbeddingConversionService(store, StoredParquetArtifacts(store))
    return service, await service.prepare(command), store


@pytest.mark.parametrize("conflicting", [False, True])
async def test_concurrent_receipt_must_match_every_deterministic_field(conflicting: bool) -> None:
    service, plan, store = await memory_plan()
    artifact = await service.convert_shard(plan, 0)
    path = artifact_path(plan, "0.json")
    store.remove(path)
    replacement = artifact.model_copy(
        update={
            "receipt": artifact.receipt.model_copy(
                update={
                    "rows": artifact.receipt.rows + int(conflicting),
                    "elapsed_seconds": 999.0,
                }
            )
        }
    )
    store.appears_before_create[str(path)] = replacement.to_json()
    if conflicting:
        with pytest.raises(ArchiveConflict, match="concurrent converted shard"):
            await service.convert_shard(plan, 0)
    else:
        assert await service.convert_shard(plan, 0) == replacement


async def test_reader_rejects_wrong_schema_even_when_hash_matches() -> None:
    import hashlib

    import pyarrow as pa

    from openknowledge_contracts.manifests import ObjectRef

    service, plan, store = await memory_plan()
    artifact = await service.convert_shard(plan, 0)
    data = BytesIO()
    table = pa.Table.from_pylist([{"corpusid": 7}], schema=pa.schema([("corpusid", pa.uint64())]))
    with pq.ParquetWriter(data, table.schema) as writer:
        writer.write_table(table)
    payload = data.getvalue()
    generation = store.put(artifact.output.path, payload)
    changed = artifact.model_copy(
        update={
            "output": ObjectRef(path=artifact.output.path, generation=generation),
            "receipt": artifact.receipt.model_copy(
                update={
                    "parquet_bytes": len(payload),
                    "parquet_sha256": hashlib.sha256(payload).hexdigest(),
                }
            ),
        }
    )
    async with aclosing(service.parquet.read(changed)) as stream:
        with pytest.raises(ArchiveConflict, match="schema or row count"):
            await anext(stream)


@pytest.mark.parametrize("part", ["updates", "deletes"])
async def test_empty_completed_diff_part_publishes_empty_manifest(
    part: Literal["updates", "deletes"],
) -> None:
    service, plan, store = await memory_plan()
    source = FakeSource()
    source.add_diff(plan.command.dataset, plan.command.release, "2026-10-20", {}, {})
    downloads = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
    )
    diff = DownloadDiff(
        release="2026-10-20",
        from_release=plan.command.release,
        dataset=plan.command.dataset,
        bucket=plan.command.bucket,
    )
    await downloads.pin_diff(diff)
    progress = ArchiveProgress(store, downloads, PRODUCER, clock)
    anchor = HashedRef(ref=plan.source.completion, sha256=plan.source.completion_sha256)
    completion = await progress.finalize(diff, anchor)
    empty = await service.prepare(
        ConvertEmbeddings(**diff.model_dump(), part=part, completion=completion)
    )
    assert empty.shards == () and empty.source.manifest is None
    result = await service.complete(empty)
    manifest_ref = await service.reader.read(result, HashedRef)
    assert (await service.reader.read(manifest_ref, ConversionManifest)).entries == ()


async def test_thousand_shard_plan_fits_and_worker_reads_only_selected_receipt() -> None:
    """Real archive metadata and conversion, using small synthetic shards and no source network."""
    from openknowledge_core.embedding_conversion.service import PLAN_LIMIT

    store = FakeStore()
    source = FakeSource()
    command = ConvertEmbeddings(release="2026-10-13", bucket="raw")
    payload = gz(rows(7))
    for index in range(1000):
        source.add(command.dataset, f"part-{index:04}.gz", payload)
    downloads = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
    )
    base = await downloads.pin_dataset(command)
    for shard in base.shards:
        await downloads.archive_shard(base, shard)
    await ArchiveProgress(store, downloads, PRODUCER, clock).finalize(command)
    service = EmbeddingConversionService(store, StoredParquetArtifacts(store))
    store.reads.clear()
    plan = await service.prepare(command)
    assert len([path for path in store.reads if path.endswith(".receipt.json")]) == 1000
    assert len(plan.to_json()) < 1024 * 1024 <= PLAN_LIMIT
    reference = await service.publish_plan(plan)
    persisted = await service.reader.read(reference, ConversionPlan)
    assert persisted == plan
    assert all(set(value) == {"ref", "sha256"} for value in plan.model_dump()["shards"])

    store.reads.clear()
    selected = 777
    result = await service.convert_shard(persisted, selected)
    assert result.source_receipt == plan.shards[selected].ref
    assert [path for path in store.reads if path.endswith(".receipt.json")] == [
        str(plan.shards[selected].ref.path),
    ]
    assert len(store.reads) <= 12  # bounded archive documents + selected receipt/raw/output
    wrong_position = plan.model_copy(update={"shards": tuple(reversed(plan.shards))})
    with pytest.raises(ArchiveConflict, match="position"):
        await service.convert_shard(wrong_position, selected)
    with pytest.raises(ArchiveConflict, match="shard count"):
        await service.convert_shard(plan.model_copy(update={"shards": plan.shards[:-1]}), selected)
    import hashlib

    from openknowledge_contracts.manifests import ObjectRef, ShardReceipt

    receipt_path = plan.shards[selected].ref.path
    receipt = ShardReceipt.from_json(store.data(receipt_path))
    changed = receipt.model_copy(update={"records": receipt.records + 1}).to_json()
    generation = store.put(receipt_path, changed)
    references = list(plan.shards)
    references[selected] = HashedRef(
        ref=ObjectRef(path=receipt_path, generation=generation),
        sha256=hashlib.sha256(changed).hexdigest(),
    )
    with pytest.raises(ArchiveConflict, match="disagrees with completed manifest"):
        await service.convert_shard(plan.model_copy(update={"shards": tuple(references)}), selected)
    with pytest.raises((ArchiveConflict, ObjectChanged)):
        await service.convert_shard(persisted, selected)


async def test_oversized_plan_is_refused_before_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    from openknowledge_core.embedding_conversion import service as conversion_service

    service, plan, store = await memory_plan()
    monkeypatch.setattr(conversion_service, "PLAN_LIMIT", len(plan.to_json()) - 1)
    oversized = plan
    before = set(store.objects)
    with pytest.raises(ArchiveConflict, match="plan exceeds"):
        await service.publish_plan(oversized)
    assert set(store.objects) == before
