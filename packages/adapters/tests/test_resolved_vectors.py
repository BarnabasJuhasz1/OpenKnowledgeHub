import hashlib
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

import openknowledge_adapters.resolved.store as resolved_store
from openknowledge_adapters.resolved.store import (
    iter_resolved_vectors,
    read_resolved_artifact,
    resolve_vectors,
    reuse_resolved_artifact,
)
from openknowledge_contracts.embeddings import EmbeddingMutation, EmbeddingSnapshot
from openknowledge_core.embeddings.resolution import (
    ResolutionPlan,
    ResolutionRelease,
    resolution_identity,
)
from openknowledge_core.embeddings.serialization import vector_record_bytes

SOURCE = EmbeddingSnapshot(
    snapshot="sha256:" + "a" * 64, release="2026-10-06", model_revision="fixture-revision"
)


def plan() -> ResolutionPlan:
    releases = (
        ResolutionRelease(release="2025-01-01", sequence=0, identity="sha256:" + "a" * 64),
        ResolutionRelease(
            release=SOURCE.release,
            from_release="2025-01-01",
            sequence=1,
            identity="sha256:" + "b" * 64,
        ),
    )
    evidence = "synthetic fixture only: all explicitly declared sample inputs"
    identity = resolution_identity(SOURCE, releases, evidence, "fixture encoder", "bounded-sample")
    return ResolutionPlan(
        source=SOURCE.model_copy(update={"snapshot": identity}),
        releases=releases,
        completeness_evidence=evidence,
        space_evidence="fixture encoder",
        scope="bounded-sample",
    )


def row(
    p: ResolutionPlan, id_: int, sequence: int = 0, *, deleted: bool = False, value: float = 1
) -> EmbeddingMutation:
    return EmbeddingMutation(
        source=p.source,
        corpusid=id_,
        sequence=sequence,
        provenance="fixture shard",
        is_deleted=deleted,
        vector=() if deleted else (value,) * 768,
    )


def test_uint64_order_updates_tombstones_and_stable_duplicates(tmp_path: Path) -> None:
    p = plan()
    maximum = 2**64 - 1
    rows = [
        row(p, maximum),
        row(p, 0),
        row(p, 2**63),
        row(p, 0, 1, deleted=True),
        row(p, maximum, 1, value=2),
        row(p, maximum, 1, value=2).model_copy(update={"provenance": "other shard"}),
    ]
    first = resolve_vectors(p, iter(rows), tmp_path / "first")
    second = resolve_vectors(p, reversed(rows), tmp_path / "second")
    assert first == second == read_resolved_artifact(tmp_path / "first")
    result = list(iter_resolved_vectors(tmp_path / "first", first))
    assert [r.corpusid for r in result] == [0, 2**63, maximum]
    assert result[0].is_deleted
    assert result[-1].vector[0] == 2
    assert first.resolved.records == 3 and first.resolved.live_records == 2
    assert (
        first.resolved.sha256
        == "sha256:" + hashlib.sha256(b"".join(vector_record_bytes(r) for r in result)).hexdigest()
    )
    assert sorted(f.name for f in (tmp_path / "first").iterdir()) == [
        "manifest.json",
        "part-00000000.parquet",
    ]
    with pytest.raises(FileExistsError):
        resolve_vectors(p, [], tmp_path / "first")


@pytest.mark.parametrize("deletion", [False, True])
def test_conflicting_superseded_replay_fails_and_cleans(tmp_path: Path, deletion: bool) -> None:
    p = plan()
    rows = [row(p, 1), row(p, 1, 1), row(p, 1, deleted=deletion, value=2)]
    with pytest.raises(ValueError, match="conflicting equal-sequence"):
        resolve_vectors(p, rows, tmp_path / "result")
    assert list(tmp_path.iterdir()) == []


def test_source_failure_and_invalid_sequence_cleanup(tmp_path: Path) -> None:
    p = plan()

    def failing() -> Iterator[EmbeddingMutation]:
        yield row(p, 1)
        raise OSError("fixture read failed")

    with pytest.raises(OSError, match="fixture read failed"):
        resolve_vectors(p, failing(), tmp_path / "result")
    with pytest.raises(ValueError, match="outside the declared plan"):
        resolve_vectors(p, [row(p, 1, 2)], tmp_path / "result")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("corruption", ["truncated", "changed", "manifest"])
def test_corruption_is_rejected(tmp_path: Path, corruption: str) -> None:
    p = plan()
    artifact = resolve_vectors(p, [row(p, 1)], tmp_path / "result")
    records = tmp_path / "result" / "part-00000000.parquet"
    records.chmod(0o644)
    if corruption == "truncated":
        records.write_bytes(b"")
    elif corruption == "changed":
        records.write_bytes(records.read_bytes()[:-1] + b"x")
    else:
        manifest = tmp_path / "result" / "manifest.json"
        manifest.chmod(0o644)
        manifest.write_text("{}")
    with pytest.raises(ValueError):
        list(iter_resolved_vectors(tmp_path / "result", artifact))


def test_lineage_and_identity_cannot_be_inferred(tmp_path: Path) -> None:
    p = plan()
    for change in (
        {"completeness_evidence": ""},
        {"source": SOURCE},
        {"releases": p.releases[1:]},
        {"releases": (p.releases[0], p.releases[1].model_copy(update={"sequence": 0}))},
        {"releases": (p.releases[0], p.releases[1].model_copy(update={"from_release": None}))},
    ):
        with pytest.raises(ValidationError):
            ResolutionPlan.model_validate(p.model_dump() | change)


def test_verified_reuse_and_empty_artifact(tmp_path: Path) -> None:
    p = plan()
    artifact = resolve_vectors(p, [], tmp_path / "empty")
    assert list(iter_resolved_vectors(tmp_path / "empty", artifact)) == []
    with pytest.raises(ValueError, match="verification"):
        reuse_resolved_artifact(tmp_path / "empty", p)
    assert reuse_resolved_artifact(tmp_path / "empty", p, require_receipt_backed=False) == artifact
    data = tmp_path / "empty" / "part-00000000.parquet"
    data.chmod(0o644)
    data.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="bytes"):
        reuse_resolved_artifact(tmp_path / "empty", p, require_receipt_backed=False)


@pytest.mark.parametrize("change", ["digest", "order", "tombstone"])
def test_canonical_validation_survives_rewritten_physical_manifest(
    tmp_path: Path, change: str
) -> None:
    p = plan()
    artifact = resolve_vectors(p, [row(p, 1), row(p, 2)], tmp_path / "result")
    records = tmp_path / "result" / "part-00000000.parquet"
    table = pq.ParquetFile(records).read()  # pyright: ignore[reportUnknownMemberType]
    rows = table.to_pylist()
    if change == "digest":
        rows[0]["vector"][0] = 0.5
    elif change == "order":
        rows.reverse()
    else:
        rows[0]["is_deleted"] = True
    records.chmod(0o644)
    # Arrow stubs leave unused write_table parameters untyped.
    pq.write_table(  # pyright: ignore[reportUnknownMemberType]
        pa.Table.from_pylist(rows, schema=table.schema), records
    )
    changed = artifact.model_copy(
        update={
            "parquet_bytes": records.stat().st_size,
            "parts": (
                artifact.parts[0].model_copy(
                    update={
                        "parquet_bytes": records.stat().st_size,
                        "sha256": "sha256:" + hashlib.sha256(records.read_bytes()).hexdigest(),
                    }
                ),
            ),
        }
    )
    manifest = tmp_path / "result" / "manifest.json"
    manifest.chmod(0o644)
    manifest.write_text(changed.model_dump_json())
    with pytest.raises(ValueError):
        list(iter_resolved_vectors(tmp_path / "result", changed))


@pytest.mark.parametrize("corrupt_later", [False, True])
def test_part_rotation_and_late_integrity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corrupt_later: bool
) -> None:
    monkeypatch.setattr(resolved_store, "_PART_ROWS", 2)
    p = plan()
    artifact = resolve_vectors(
        p, [row(p, i, deleted=i == 3) for i in reversed(range(5))], tmp_path / "result"
    )
    assert [part.records for part in artifact.parts] == [2, 2, 1]
    assert [part.live_records for part in artifact.parts] == [2, 1, 1]
    assert artifact.parquet_bytes == sum(part.parquet_bytes for part in artifact.parts)
    rows = list(iter_resolved_vectors(tmp_path / "result", artifact))
    assert [r.corpusid for r in rows] == list(range(5))
    assert (
        artifact.resolved.sha256
        == "sha256:" + hashlib.sha256(b"".join(vector_record_bytes(r) for r in rows)).hexdigest()
    )
    if corrupt_later:
        later = tmp_path / "result" / "part-00000002.parquet"
        later.chmod(0o644)
        later.write_bytes(b"corrupt")
        with pytest.raises(ValueError, match="bytes"):
            reuse_resolved_artifact(tmp_path / "result", p, require_receipt_backed=False)


def test_failed_part_write_cleans_all_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(resolved_store, "_PART_ROWS", 2)
    original = resolved_store._file_digest  # pyright: ignore[reportPrivateUsage]

    def failing(path: Path) -> tuple[int, str]:
        if path.name == "part-00000001.parquet":
            raise OSError("fixture disk read failed")
        return original(path)

    monkeypatch.setattr(resolved_store, "_file_digest", failing)
    p = plan()
    with pytest.raises(OSError, match="fixture disk read failed"):
        resolve_vectors(p, [row(p, i) for i in range(3)], tmp_path / "result")
    assert list(tmp_path.iterdir()) == []
