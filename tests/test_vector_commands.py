"""Command scope, immutable descriptor files and exact-source seed lookup."""

import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from openknowledge_adapters import opensearch
from openknowledge_adapters.resolved.store import write_resolved_artifact
from openknowledge_contracts.embeddings import EmbeddingLookupResult, LookupEmbeddings
from openknowledge_core.search.vector_models import (
    SearchVectorNeighbors,
    VectorNeighbors,
    VectorPublication,
)
from openknowledge_pipelines import admission, bootstrap, cli
from openknowledge_pipelines.settings import LoadSettings, OpenSearchSettings, SearchSettings
from packages.adapters.tests.test_resolved_vectors import plan, row
from packages.core.tests.test_vector_publication import MemoryVectors
from tests.test_embedding_commands import LOAD_ENV

OS_ENV = {"OKP_OPENSEARCH_URL": "http://127.0.0.1:19203", "OPENSEARCH_PASSWORD": ""}


@pytest.fixture
def vector_artifact(tmp_path: Path) -> Path:
    directory = tmp_path / "artifact"
    source_plan = plan()
    write_resolved_artifact(source_plan, [row(source_plan, 7)], directory, "receipt-backed")
    return directory


def descriptor(directory: Path) -> VectorPublication:
    return VectorPublication(
        index="paper-vectors-" + "c" * 32,
        resolved=bootstrap.embedding_artifact(directory).resolved,
    )


def test_settings_scopes_preserve_search_requirements(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*LOAD_ENV, "S2_API_KEY", "OKP_BATCH_PROJECT"):
        monkeypatch.delenv(name, raising=False)
    for key, value in OS_ENV.items():
        monkeypatch.setenv(key, value)
    settings = OpenSearchSettings.model_validate({})
    assert settings.opensearch_user == "hub-indexer"
    assert "clickhouse_url" not in type(settings).model_fields
    with pytest.raises(ValidationError):
        SearchSettings.model_validate({})
    for key, value in LOAD_ENV.items():
        monkeypatch.setenv(key, value)
    combined = SearchSettings.model_validate({})
    assert combined.index_shards == 5 and combined.clickhouse_user == "default"
    for kind in (OpenSearchSettings, SearchSettings):
        with pytest.raises(ValidationError, match="must not carry"):
            kind.model_validate({"opensearch_url": "https://name:secret@example.com"})


@pytest.mark.parametrize("index", [None, "paper-vectors-" + "d" * 32])
def test_publish_uses_verified_stream_os_only_and_admission(
    vector_artifact: Path, monkeypatch: pytest.MonkeyPatch, index: str | None
) -> None:
    for name in (*LOAD_ENV, "S2_API_KEY", "OKP_BATCH_PROJECT"):
        monkeypatch.delenv(name, raising=False)
    events: list[str] = []
    store = MemoryVectors()

    @asynccontextmanager
    async def admitted() -> AsyncGenerator[None]:
        events.append("admission")
        yield

    @asynccontextmanager
    async def opened(config: opensearch.OpenSearchConfig) -> AsyncGenerator[MemoryVectors]:
        events.append("opensearch")
        assert config.url.host == "127.0.0.1"
        yield store

    monkeypatch.setattr(admission, "execution", admitted)
    monkeypatch.setattr(opensearch, "connect_vectors", opened)
    args = ["embeddings", "publish", "--artifact", str(vector_artifact)]
    if index is not None:
        args += ["--index", index]
    result = CliRunner().invoke(cli.main, args, env=OS_ENV)
    assert result.exit_code == 0, result.output
    publication = VectorPublication.model_validate_json(result.stdout)
    assert publication.resolved == bootstrap.embedding_artifact(vector_artifact).resolved
    if index is not None:
        assert publication.index == index
    assert store.complete_called and len(store.rows) == 1
    assert events == ["admission", "opensearch"]


def test_publish_closed_admission_never_opens_client(
    vector_artifact: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    @asynccontextmanager
    async def closed() -> AsyncGenerator[None]:
        raise admission.AdmissionError("fixture admission closed")
        yield

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("closed admission must precede client construction")

    monkeypatch.setattr(admission, "execution", closed)
    monkeypatch.setattr(opensearch, "connect_vectors", forbidden)
    result = CliRunner().invoke(
        cli.main, ["embeddings", "publish", "--artifact", str(vector_artifact)], env=OS_ENV
    )
    assert result.exit_code == 1 and "admission closed" in result.output


@pytest.mark.parametrize("arguments", [["--index", "latest"], ["--index", "paper-vectors-ABC"]])
def test_invalid_publication_index_precedes_settings(
    vector_artifact: Path, monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("invalid arguments must precede settings")

    monkeypatch.setattr(cli, "_settings", forbidden)
    result = CliRunner().invoke(
        cli.main, ["embeddings", "publish", "--artifact", str(vector_artifact), *arguments]
    )
    assert result.exit_code == 2 and "--index must be paper-vectors-" in result.output


@pytest.mark.parametrize("kind", ["invalid", "symlink", "fifo", "oversized"])
def test_publication_file_gate_precedes_settings(
    tmp_path: Path, vector_artifact: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    path = tmp_path / "publication.json"
    if kind == "symlink":
        target = tmp_path / "target.json"
        target.write_text(descriptor(vector_artifact).model_dump_json())
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "oversized":
        with path.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024 + 1)
    else:
        path.write_text("{}")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("descriptor gate must precede settings, admission and clients")

    monkeypatch.setattr(cli, "_settings", forbidden)
    monkeypatch.setattr(admission, "execution", forbidden)
    result = CliRunner().invoke(
        cli.main, ["embeddings", "neighbors", "--publication", str(path), "--id", "7"]
    )
    assert result.exit_code == 2 and "regular nonsymlink VectorPublication" in result.output


@pytest.mark.parametrize(
    "arguments", [["--id", str(2**64)], ["--id", "7", "--k", "0"], ["--id", "7", "--k", "101"]]
)
def test_invalid_neighbor_arguments_precede_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    path = tmp_path / "publication.json"
    path.write_text("{}")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("argument validation must precede settings")

    monkeypatch.setattr(cli, "_settings", forbidden)
    result = CliRunner().invoke(
        cli.main, ["embeddings", "neighbors", "--publication", str(path), *arguments]
    )
    assert result.exit_code == 2 and "Invalid value" in result.output


@pytest.mark.parametrize("missing", [False, True])
def test_neighbors_exact_lookup_no_parquet_or_admission(
    vector_artifact: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: bool
) -> None:
    publication = descriptor(vector_artifact)
    path = tmp_path / "publication.json"
    path.write_text(publication.model_dump_json())
    (vector_artifact / "part-00000000.parquet").unlink()
    expected = row(plan(), 7)
    events: list[str] = []

    async def lookup(settings: LoadSettings, command: LookupEmbeddings) -> EmbeddingLookupResult:
        events.append("lookup")
        assert command.source == publication.resolved.source and command.corpusids == (7,)
        return EmbeddingLookupResult(
            source=command.source,
            found=() if missing else (expected,),
            missing=(7,) if missing else (),
        )

    class Vectors(MemoryVectors):
        async def neighbors(self, command: SearchVectorNeighbors) -> VectorNeighbors:
            events.append("neighbors")
            assert command.seed == expected and command.k == 2
            assert command.expected_source == publication.resolved.source
            return VectorNeighbors(publication=command.publication, hits=())

    @asynccontextmanager
    async def opened(config: opensearch.OpenSearchConfig) -> AsyncGenerator[Vectors]:
        events.append("opensearch")
        yield Vectors()

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("neighbor reads must not take mutation admission")

    monkeypatch.setattr(admission, "execution", forbidden)
    monkeypatch.setattr(bootstrap, "lookup_embeddings", lookup)
    monkeypatch.setattr(opensearch, "connect_vectors", opened)
    result = CliRunner().invoke(
        cli.main,
        ["embeddings", "neighbors", "--publication", str(path), "--id", "7", "--k", "2"],
        env={**LOAD_ENV, **OS_ENV},
    )
    if missing:
        assert result.exit_code == 1 and "Paper 7 has no live embedding" in result.output
        assert events == ["lookup"]
    else:
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["publication"] == publication.model_dump(mode="json")
        assert events == ["lookup", "opensearch", "neighbors"]


def test_publish_rejects_unverified_artifact_before_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "unverified"
    write_resolved_artifact(plan(), (), directory, "unverified-iterable")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("unverified artifact must fail before settings or admission")

    monkeypatch.setattr(cli, "_settings", forbidden)
    monkeypatch.setattr(admission, "execution", forbidden)
    result = CliRunner().invoke(cli.main, ["embeddings", "publish", "--artifact", str(directory)])
    assert result.exit_code == 2 and "receipt-backed manifest" in result.output
