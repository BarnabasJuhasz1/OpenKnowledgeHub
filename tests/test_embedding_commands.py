"""Command validation and admission boundaries without external services."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from hashlib import sha256
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from openknowledge_adapters import clickhouse, gcs
from openknowledge_adapters.resolved.store import write_resolved_artifact
from openknowledge_contracts.embeddings import EmbeddingLookupResult, LookupEmbeddings
from openknowledge_contracts.manifests import ObjectPath, ObjectRef
from openknowledge_core.embeddings.resolution import ConversionSelection, ResolveEmbeddings
from openknowledge_core.ingestion.archive_progress import HashedRef
from openknowledge_pipelines import admission, bootstrap, cli
from openknowledge_pipelines.settings import LoadSettings
from packages.adapters.tests.test_resolved_vectors import plan

LOAD_ENV = {
    "OKP_CLICKHOUSE_URL": "http://127.0.0.1:18124",
    "OKP_CLICKHOUSE_USER": "default",
    "CLICKHOUSE_PASSWORD": "",
    "OKP_CLICKHOUSE_RAW_BUCKET": "fixture-raw",
}


def request() -> ResolveEmbeddings:
    return ResolveEmbeddings(
        model_revision="fixture:encoder",
        space_evidence="fixture:space",
        scope="corpus",
        selections=(
            ConversionSelection(
                completion=HashedRef(
                    ref=ObjectRef(
                        path=ObjectPath(bucket="raw", name="complete.json"), generation=1
                    ),
                    sha256=sha256(b"fixture").hexdigest(),
                ),
                conversions=(),
            ),
        ),
    )


@pytest.fixture
def artifact(tmp_path: Path) -> Path:
    directory = tmp_path / "resolved"
    # Only metadata is relevant to these validation tests; integration uses real receipts.
    write_resolved_artifact(plan(), (), directory, "receipt-backed")
    return directory


@pytest.mark.parametrize("command", ["resolve", "load", "lookup"])
def test_invalid_input_precedes_client_and_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("invalid command must fail before settings, admission or clients")

    monkeypatch.setattr(cli, "_settings", forbidden)
    monkeypatch.setattr(admission, "execution", forbidden)
    bad = tmp_path / "bad.json"
    bad.write_text('{"model_revision":"unknown"}')
    args = (
        ["--request", str(bad), "--destination", str(tmp_path / "out")]
        if command == "resolve"
        else ["--artifact", str(tmp_path)]
    )
    if command == "lookup":
        args += ["--id", str(2**64)]
    result = CliRunner().invoke(cli.main, ["embeddings", command, *args])
    assert result.exit_code == 2, result.output


def test_resolve_requires_admission_before_archive_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "request.json"
    path.write_text(request().model_dump_json())

    @asynccontextmanager
    async def closed() -> AsyncGenerator[None]:
        raise admission.AdmissionError("fixture admission closed")
        yield

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("closed admission must not construct archive clients")

    monkeypatch.setattr(admission, "execution", closed)
    monkeypatch.setattr(gcs, "connect", forbidden)
    result = CliRunner().invoke(
        cli.main,
        [
            "embeddings",
            "resolve",
            "--request",
            str(path),
            "--destination",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code == 1 and "fixture admission closed" in result.output
    assert not (tmp_path / "out").exists()


def test_load_requires_managed_gate_before_clickhouse(
    artifact: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refused(limits: tuple[str, ...]) -> None:
        assert limits == ("clickhouse",)
        raise click.ClickException("fixture managed gate closed")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("managed refusal must not open ClickHouse")

    monkeypatch.setattr(cli, "_require_managed_server", refused)
    monkeypatch.setattr(clickhouse, "connect", forbidden)
    result = CliRunner().invoke(
        cli.main, ["embeddings", "load", "--artifact", str(artifact)], env=LOAD_ENV
    )
    assert result.exit_code == 1 and "fixture managed gate closed" in result.output


def test_lookup_uses_manifest_only_and_no_admission(
    artifact: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (artifact / "part-00000000.parquet").unlink()

    async def lookup(settings: LoadSettings, command: LookupEmbeddings) -> EmbeddingLookupResult:
        assert command.corpusids == (2**64 - 1, 7, 7)
        return EmbeddingLookupResult(source=command.source, found=(), missing=(2**64 - 1, 7))

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("read-only lookup must not acquire admission")

    monkeypatch.setattr(admission, "execution", forbidden)
    monkeypatch.setattr(bootstrap, "lookup_embeddings", lookup)
    result = CliRunner().invoke(
        cli.main,
        [
            "embeddings",
            "lookup",
            "--artifact",
            str(artifact),
            "--id",
            str(2**64 - 1),
            "--id",
            "7",
            "--id",
            "7",
        ],
        env=LOAD_ENV,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["missing"] == [2**64 - 1, 7]
