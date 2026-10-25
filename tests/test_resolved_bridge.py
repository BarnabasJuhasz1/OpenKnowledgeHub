"""Tiny completed archives exercise the actual conversion/resolution boundary.

Requires the PR66 packaged conversion dependency; no production data is selected.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Literal

import pytest

from openknowledge_adapters.parquet import StoredParquetArtifacts
from openknowledge_adapters.resolved.parquet import (
    ConversionSelection,
    conversion_completeness_evidence,
    resolve_parquet_vectors,
)
from openknowledge_adapters.resolved.store import (
    iter_resolved_vectors,
    reuse_resolved_artifact,
)
from openknowledge_contracts.embedding_artifacts import (
    ConversionManifest,
    ConvertedShard,
    EmbeddingArtifactRow,
)
from openknowledge_contracts.embeddings import EmbeddingSnapshot
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.embedding_conversion.models import ConvertEmbeddings
from openknowledge_core.embedding_conversion.service import EmbeddingConversionService
from openknowledge_core.embeddings.resolution import (
    ResolutionPlan,
    ResolutionRelease,
    ResolveEmbeddings,
    build_resolution_plan,
    resolution_identity,
)
from openknowledge_core.ingestion.archive_progress import (
    ArchiveCompletion,
    ArchiveProgress,
    ArchiveReader,
    HashedRef,
)
from openknowledge_core.ingestion.commands import DownloadDiff
from openknowledge_core.ingestion.errors import ArchiveConflict, ObjectChanged
from openknowledge_core.ingestion.service import DownloadDatasetService
from tests.fakes.ports import PRODUCER, FakeSource, FakeStore, clock, gz


def assert_no_staging(directory: Path) -> None:
    assert not any(path.name.startswith(".resolution-") for path in directory.iterdir())


def raw_rows(*ids: int, value: float = 0.1) -> bytes:
    return b"".join(
        json.dumps(
            {"corpusid": id_, "model": "specter_v2", "vector": json.dumps([value] * 768)}
        ).encode()
        + b"\n"
        for id_ in ids
    )


async def fixture_archive(
    directory: Path,
    *,
    conflicting: bool = False,
    empty_diff: bool = False,
    empty_updates: bool = False,
    scope: Literal["corpus", "bounded-sample"] = "bounded-sample",
) -> tuple[FakeStore, ResolutionPlan, tuple[ConversionSelection, ...], StoredParquetArtifacts]:
    store = FakeStore()
    source = FakeSource()
    command = ConvertEmbeddings(release="2026-10-13", bucket="raw", prefix="synthetic")
    maximum = 2**64 - 1
    source.add(command.dataset, "base.gz", gz(raw_rows(maximum, 0, 7, 7)))
    downloads = DownloadDatasetService(
        catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
    )
    progress = ArchiveProgress(store, downloads, PRODUCER, clock)
    pinned = await downloads.pin_dataset(command)
    for shard in pinned.shards:
        await downloads.archive_shard(pinned, shard)
    anchor = await progress.finalize(command)
    converter = EmbeddingConversionService(store, StoredParquetArtifacts(store))

    async def convert(selected: ConvertEmbeddings) -> HashedRef:
        conversion = await converter.prepare(selected)
        for index in range(len(conversion.shards)):
            await converter.convert_shard(conversion, index)
        completed = await converter.complete(conversion)
        return completed

    base_conversion = await convert(command)
    selections = [ConversionSelection(completion=anchor, conversions=(base_conversion,))]
    update = raw_rows(maximum, 8, value=0.2)
    if conflicting:
        update += raw_rows(8, value=0.3)
    source.add_diff(
        command.dataset,
        command.release,
        "2026-10-20",
        {} if empty_diff or empty_updates else {"update.gz": gz(update)},
        {} if empty_diff else {"delete.gz": gz(b'{"corpusid":7}\n{"corpusid":7}\n')},
    )
    diff = DownloadDiff(
        release="2026-10-20",
        from_release=command.release,
        dataset=command.dataset,
        bucket=command.bucket,
        prefix=command.prefix,
    )
    transition = await downloads.pin_diff(diff)
    for part in (transition.updates, transition.deletes):
        for shard in part.shards:
            await downloads.archive_shard(part, shard)
    completed = await progress.finalize(diff, anchor)
    parts: list[HashedRef] = []
    if not empty_diff:
        for kind in ("updates", "deletes"):
            # Explicit literals preserve the conversion command's public type.
            part_name: Literal["updates", "deletes"] = "updates" if kind == "updates" else "deletes"
            parts.append(
                await convert(
                    ConvertEmbeddings(**diff.model_dump(), part=part_name, completion=completed)
                )
            )
    selections.append(ConversionSelection(completion=completed, conversions=tuple(parts)))
    selected = tuple(selections)
    releases = (
        ResolutionRelease(release=command.release, sequence=0, identity=selected[0].identity),
        ResolutionRelease(
            release=diff.release,
            from_release=command.release,
            sequence=1,
            identity=selected[1].identity,
        ),
    )
    source_identity = EmbeddingSnapshot(
        snapshot="sha256:" + "0" * 64,
        release=diff.release,
        model_revision="synthetic-fixture-encoder-v1",
    )
    evidence = conversion_completeness_evidence(selected, scope)
    identity = resolution_identity(
        source_identity, releases, evidence, "synthetic encoder fixture", scope
    )
    plan = ResolutionPlan(
        source=source_identity.model_copy(update={"snapshot": identity}),
        releases=releases,
        completeness_evidence=evidence,
        space_evidence="synthetic encoder fixture",
        scope=scope,
    )
    return store, plan, selected, StoredParquetArtifacts(store)


@pytest.mark.parametrize("empty_diff", [False, True])
@pytest.mark.parametrize("scope", ["corpus", "bounded-sample"])
async def test_actual_archive_conversion_resolution(
    tmp_path: Path, empty_diff: bool, scope: Literal["corpus", "bounded-sample"]
) -> None:
    store, plan, selected, parquet = await fixture_archive(
        tmp_path, empty_diff=empty_diff, scope=scope
    )
    destination = tmp_path / "resolved"
    artifact = await resolve_parquet_vectors(plan, selected, store, parquet, destination)
    assert artifact.input_verification == "receipt-backed"
    assert reuse_resolved_artifact(destination, plan) == artifact
    rows = list(iter_resolved_vectors(destination, artifact))
    assert [r.corpusid for r in rows] == ([0, 7, 2**64 - 1] if empty_diff else [0, 7, 8, 2**64 - 1])
    assert rows[1].is_deleted is not empty_diff
    assert rows[-1].sequence == (0 if empty_diff else 1)
    replay = await resolve_parquet_vectors(plan, selected, store, parquet, tmp_path / "replay")
    assert artifact == replay


async def test_converted_conflict_never_finalizes(tmp_path: Path) -> None:
    store, plan, selected, parquet = await fixture_archive(tmp_path, conflicting=True)
    with pytest.raises(ValueError, match="conflicting equal-sequence"):
        await resolve_parquet_vectors(plan, selected, store, parquet, tmp_path / "resolved")
    assert not (tmp_path / "resolved").exists()
    assert_no_staging(tmp_path)


@pytest.mark.parametrize("failure", ["parquet", "source-receipt", "completion", "missing-part"])
async def test_pinned_input_corruption_fails_closed(tmp_path: Path, failure: str) -> None:
    store, plan, selected, parquet = await fixture_archive(tmp_path)
    if failure == "parquet":
        reader = EmbeddingConversionService(store, parquet).reader
        reference = await reader.read(selected[-1].conversions[-1], HashedRef)
        manifest = await reader.read(reference, ConversionManifest)
        store.put(manifest.entries[0].output.path, b"corrupt")
    elif failure == "source-receipt":
        reader = EmbeddingConversionService(store, StoredParquetArtifacts(store)).reader
        reference = await reader.read(selected[0].conversions[0], HashedRef)
        manifest = await reader.read(reference, ConversionManifest)
        store.put(manifest.entries[0].source_receipt.path, b"{}")
    elif failure == "completion":
        store.put(selected[-1].completion.ref.path, b"{}")
    else:
        selected = (*selected[:-1], selected[-1].model_copy(update={"conversions": ()}))
    with pytest.raises((ValueError, ArchiveConflict, ObjectChanged)):
        await resolve_parquet_vectors(plan, selected, store, parquet, tmp_path / "resolved")
    assert not (tmp_path / "resolved").exists()
    assert_no_staging(tmp_path)


async def test_explicit_empty_part_conversion_is_supported(tmp_path: Path) -> None:
    store, plan, selected, parquet = await fixture_archive(tmp_path, empty_updates=True)
    artifact = await resolve_parquet_vectors(plan, selected, store, parquet, tmp_path / "resolved")
    rows = list(iter_resolved_vectors(tmp_path / "resolved", artifact))
    assert [r.corpusid for r in rows] == [0, 7, 2**64 - 1]
    assert rows[1].is_deleted and rows[1].sequence == 1
    assert rows[-1].sequence == 0


async def test_verified_reader_failure_before_exhaustion_never_finalizes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, plan, selected, parquet = await fixture_archive(tmp_path)
    original = parquet.read

    async def failing(entry: ConvertedShard) -> AsyncGenerator[EmbeddingArtifactRow]:
        async for record in original(entry):
            yield record
        raise OSError("fixture verification failed at end of stream")

    monkeypatch.setattr(parquet, "read", failing)
    with pytest.raises(OSError, match="verification failed"):
        await resolve_parquet_vectors(plan, selected, store, parquet, tmp_path / "resolved")
    assert not (tmp_path / "resolved").exists()
    assert_no_staging(tmp_path)


async def test_completion_at_wrong_path_is_rejected(tmp_path: Path) -> None:
    store, plan, selected, parquet = await fixture_archive(tmp_path)
    reader = ArchiveReader(store)
    completion = await reader.read(selected[0].completion, ArchiveCompletion)
    wrong = ObjectPath(bucket="raw", name="synthetic/relocated-completion.json")
    store.put(wrong, completion.to_json())
    relocated = await reader.reference(wrong)
    selected = (selected[0].model_copy(update={"completion": relocated}), *selected[1:])
    releases = tuple(
        release.model_copy(update={"identity": selection.identity})
        for release, selection in zip(plan.releases, selected, strict=True)
    )
    evidence = conversion_completeness_evidence(selected, plan.scope)
    identity = resolution_identity(plan.source, releases, evidence, plan.space_evidence, plan.scope)
    rebound = ResolutionPlan(
        source=plan.source.model_copy(update={"snapshot": identity}),
        releases=releases,
        completeness_evidence=evidence,
        space_evidence=plan.space_evidence,
        scope=plan.scope,
    )
    with pytest.raises(ArchiveConflict, match="archive completion identity mismatch"):
        await resolve_parquet_vectors(rebound, selected, store, parquet, tmp_path / "resolved")
    assert not (tmp_path / "resolved").exists()


@pytest.mark.parametrize("scope", ["corpus", "bounded-sample"])
async def test_request_derives_plan_from_verified_completions(
    tmp_path: Path, scope: Literal["corpus", "bounded-sample"]
) -> None:
    store, expected, selected, _ = await fixture_archive(tmp_path, scope=scope)
    request = ResolveEmbeddings(
        model_revision=expected.source.model_revision,
        space_evidence=expected.space_evidence,
        scope=scope,
        selections=selected,
    )
    assert await build_resolution_plan(request, store) == expected
    reversed_request = ResolveEmbeddings.model_validate(
        {**request.model_dump(), "selections": tuple(reversed(selected))}
    )
    with pytest.raises(ValueError, match="contiguous embedding archive chain"):
        await build_resolution_plan(reversed_request, store)
