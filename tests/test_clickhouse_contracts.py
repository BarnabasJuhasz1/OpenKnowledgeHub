"""ClickHouse migrations against the dataset contracts.

Apart from the file format, every check applies the migrations with ``clickhouse local``. They
are skipped where no ``clickhouse`` binary is on the PATH, but fail under CI, which installs one.
"""

from __future__ import annotations

import itertools
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.clickhouse import table_columns
from openknowledge_contracts.tables.s2 import CONTRACTS

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations" / "clickhouse"
CLICKHOUSE = shutil.which("clickhouse")

needs_clickhouse = pytest.mark.skipif(
    CLICKHOUSE is None and not os.environ.get("CI"), reason="no clickhouse binary on PATH"
)

type Catalog = dict[str, list[dict[str, Any]]]


def migrations() -> list[Path]:
    return sorted(MIGRATIONS.glob("*.sql"))


def section(path: Path, name: str) -> str:
    """The statements under ``-- +goose <name>``, up to the next annotation or the end."""
    _, _, rest = path.read_text().partition(f"-- +goose {name}\n")
    return rest.partition("-- +goose ")[0]


def up() -> str:
    return "\n".join(section(path, "Up") for path in migrations())


def down() -> str:
    return "\n".join(section(path, "Down") for path in reversed(migrations()))


def clickhouse_local(sql: str, workdir: Path) -> list[dict[str, Any]]:
    assert CLICKHOUSE is not None, "CI runs these checks, so it must put clickhouse on the PATH"
    # Whether JSON output quotes 64-bit integers is a default that changed between releases, so
    # it is pinned: a UInt64 comes back as a JSON number.
    args = ["local", "--path", str(workdir), "--output_format_json_quote_64bit_integers=0"]
    result = subprocess.run(
        [CLICKHOUSE, *args, "--multiquery", "--query", sql],
        stdin=subprocess.DEVNULL,  # a piped stdin would be read as data for the INSERTs
        capture_output=True,
        text=True,
        check=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines()]


def test_every_migration_is_a_numbered_goose_file_with_up_and_down() -> None:
    assert migrations()
    for path in migrations():
        assert re.fullmatch(r"\d{5}_\w+\.sql", path.name), path.name
        lines = path.read_text().splitlines()
        assert lines[:2] == ["-- +goose NO TRANSACTION", "-- +goose Up"], path.name
        assert "-- +goose Down" in lines, path.name


@pytest.fixture(scope="module")
def catalog(tmp_path_factory: pytest.TempPathFactory) -> Catalog:
    """``system.columns`` and ``system.tables`` after applying every migration."""
    sql = f"""
        {up()}
        SELECT table, name, type, default_kind, default_expression FROM system.columns
        WHERE database = currentDatabase() ORDER BY table, position FORMAT JSONEachRow;
        SELECT name AS table, engine_full, sorting_key FROM system.tables
        WHERE database = currentDatabase() FORMAT JSONEachRow;
    """
    rows = clickhouse_local(sql, tmp_path_factory.mktemp("clickhouse"))
    return {
        "columns": [row for row in rows if "type" in row],
        "tables": [row for row in rows if "engine_full" in row],
    }


@needs_clickhouse
def test_the_migrations_create_one_table_per_contract(catalog: Catalog) -> None:
    created = sorted(table["table"] for table in catalog["tables"])
    assert created == sorted(contract.table for contract in CONTRACTS.values())


@needs_clickhouse
@pytest.mark.parametrize("contract", CONTRACTS.values(), ids=list(CONTRACTS))
def test_columns_match_the_contract(catalog: Catalog, contract: DatasetContract) -> None:
    actual = [
        (c["name"], c["type"], c["default_kind"], c["default_expression"])
        for c in catalog["columns"]
        if c["table"] == contract.table
    ]
    expected = [
        (c.name, c.type, "MATERIALIZED" if c.materialized else "", c.materialized)
        for c in table_columns(contract)
    ]
    assert actual == expected


@needs_clickhouse
@pytest.mark.parametrize("contract", CONTRACTS.values(), ids=list(CONTRACTS))
def test_tables_keep_the_highest_version_per_key(
    catalog: Catalog, contract: DatasetContract
) -> None:
    (table,) = [t for t in catalog["tables"] if t["table"] == contract.table]
    assert table["engine_full"].startswith("ReplacingMergeTree(version, is_deleted) ")
    assert table["sorting_key"] == ", ".join(contract.key)


@needs_clickhouse
def test_down_drops_every_table_up_created(tmp_path: Path) -> None:
    sql = f"""
        {up()}
        {down()}
        SELECT name FROM system.tables WHERE database = currentDatabase() FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == []


@needs_clickhouse
@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_replays_converge_whatever_order_batches_land_in(
    tmp_path: Path, order: tuple[int, ...]
) -> None:
    """Corpus 1 is deleted by a later release, 2 restored by one, 3 deleted within one."""
    first, second = "2026-09-29", "2026-10-06"

    def row(corpusid: int, release: str, is_deleted: int) -> str:
        return f"({corpusid}, 'm', 'r{release}', '{release}', 'raw', {is_deleted})"

    batches = [
        [row(1, second, 1), row(2, second, 0)],
        [row(1, first, 0), row(2, first, 1)],
        [row(3, first, 1), row(3, first, 0)],
    ]
    landed = [batches[index] for index in order]
    inserts = "\n".join(
        "INSERT INTO tldrs (corpusid, model, text, source_release, source_object, is_deleted)"
        f" VALUES {', '.join(batch)};"
        for batch in [*landed, *landed]
    )
    sql = f"""
        {section(MIGRATIONS / "00004_create_tldrs.sql", "Up")}
        {inserts}
        SELECT corpusid, text FROM tldrs FINAL ORDER BY corpusid FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == [{"corpusid": 2, "text": f"r{second}"}]
