"""Real archive conversion -> installed command wiring -> isolated ClickHouse.

Uses only a fake ArtifactStore for cloud objects and fake control-plane gates.
Conversion, SQLite/Arrow resolution, verification, publication and lookup are real.
Opt in with CLICKHOUSE_EMBEDDINGS_TEST_URL=http://127.0.0.1:18124.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from click.testing import CliRunner

from openknowledge_adapters import clickhouse, gcs
from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_contracts.embeddings import EmbeddingLookupResult
from openknowledge_core.embeddings.models import EmbeddingPublication
from openknowledge_core.embeddings.resolution import ResolvedArtifact, ResolveEmbeddings
from openknowledge_pipelines import admission, cli, deployments
from packages.adapters.tests.clickhouse.test_embeddings_server import (
    server as server,
)
from tests.fakes.ports import FakeStore
from tests.test_embedding_commands import LOAD_ENV
from tests.test_resolved_bridge import fixture_archive


async def test_resolve_load_lookup_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    server: tuple[clickhouse.ClickHouseEmbeddings, ClickHouseClient],
) -> None:
    _, client = server
    store, _, selections, _ = await fixture_archive(tmp_path, scope="corpus")
    request = ResolveEmbeddings(
        model_revision="synthetic-fixture-encoder-v1",
        space_evidence="synthetic encoder fixture",
        scope="corpus",
        selections=selections,
    )
    request_file = tmp_path / "request.json"
    request_file.write_text(request.model_dump_json())
    destination = tmp_path / "resolved"
    database = str((await client.rows("SELECT currentDatabase() AS name"))[0]["name"])
    events: list[str] = []
    real_connect = clickhouse.connect

    @asynccontextmanager
    async def archives(config: gcs.GcsConfig) -> AsyncGenerator[FakeStore]:
        events.append("archive open")
        yield store
        events.append("archive close")

    @asynccontextmanager
    async def database_connection(
        config: clickhouse.ClickHouseConfig,
    ) -> AsyncGenerator[clickhouse.ClickHouse]:
        # Override only the fixture DB; use the actual per-run constructor and HTTP client.
        assert config.database == "openknowledge"
        events.append("database open")
        async with real_connect(config.model_copy(update={"database": database})) as ch:
            yield ch
        events.append("database close")

    @asynccontextmanager
    async def admitted() -> AsyncGenerator[None]:
        events.append("admission enter")
        yield
        events.append("admission exit")

    @asynccontextmanager
    async def loader(*, wait: bool) -> AsyncGenerator[None]:
        assert wait is False
        events.append("loader enter")
        yield
        events.append("loader exit")

    def managed(limits: tuple[str, ...]) -> None:
        assert limits == (deployments.LOAD_LIMIT,)
        events.append("managed")

    monkeypatch.setattr(gcs, "connect", archives)
    monkeypatch.setattr(clickhouse, "connect", database_connection)
    monkeypatch.setattr(admission, "execution", admitted)
    monkeypatch.setattr(deployments, "one_clickhouse_loader", loader)
    monkeypatch.setattr(cli, "_require_managed_server", managed)
    resolved = await asyncio.to_thread(
        CliRunner().invoke,
        cli.main,
        [
            "embeddings",
            "resolve",
            "--request",
            str(request_file),
            "--destination",
            str(destination),
        ],
    )
    assert resolved.exit_code == 0, resolved.output
    manifest = ResolvedArtifact.model_validate_json(resolved.output)
    assert manifest.input_verification == "receipt-backed"
    assert manifest.plan.scope == "corpus" and manifest.resolved.records == 4
    assert events == ["admission enter", "archive open", "archive close", "admission exit"]
    events.clear()
    loaded = await asyncio.to_thread(
        CliRunner().invoke,
        cli.main,
        ["embeddings", "load", "--artifact", str(destination)],
        env=LOAD_ENV,
    )
    assert loaded.exit_code == 0, loaded.output
    assert EmbeddingPublication.model_validate_json(loaded.output).descriptor == manifest.resolved
    assert events == [
        "managed",
        "loader enter",
        "admission enter",
        "database open",
        "database close",
        "admission exit",
        "loader exit",
    ]
    for part in destination.glob("*.parquet"):
        part.unlink()
    events.clear()
    looked_up = await asyncio.to_thread(
        CliRunner().invoke,
        cli.main,
        [
            "embeddings",
            "lookup",
            "--artifact",
            str(destination),
            "--id",
            str(2**64 - 1),
            "--id",
            "7",
            "--id",
            "999",
            "--id",
            "0",
            "--id",
            "8",
        ],
        env=LOAD_ENV,
    )
    assert looked_up.exit_code == 0, looked_up.output
    result = EmbeddingLookupResult.model_validate_json(looked_up.output)
    assert [row.corpusid for row in result.found] == [2**64 - 1, 0, 8]
    assert result.missing == (7, 999)
    assert events == ["database open", "database close"]
