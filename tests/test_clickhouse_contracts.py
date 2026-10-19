"""ClickHouse migrations against the dataset contracts.

Apart from the file format, every check applies the migrations with ``clickhouse local``.
"""

from __future__ import annotations

import itertools
import re
from pathlib import Path
from typing import Any

import pytest

from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.clickhouse import Column, table_columns
from openknowledge_contracts.tables.s2 import CONTRACTS
from tests.clickhouse_local import (
    MIGRATIONS,
    clickhouse_local,
    down,
    migrations,
    needs_clickhouse,
    run,
    section,
    up,
)

type Catalog = dict[str, list[dict[str, Any]]]

# Tables that hold no dataset: which objects were loaded into which table, and which release each
# table holds.
BOOKKEEPING = {"loads", "applied"}


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
def test_the_migrations_create_one_table_per_contract_and_the_bookkeeping(
    catalog: Catalog,
) -> None:
    created = sorted(table["table"] for table in catalog["tables"])
    assert created == sorted({contract.table for contract in CONTRACTS.values()} | BOOKKEEPING)


def expression(column: Column) -> tuple[str, str]:
    if column.materialized:
        return "MATERIALIZED", column.materialized
    return ("DEFAULT", column.default) if column.default else ("", "")


@needs_clickhouse
@pytest.mark.parametrize("contract", CONTRACTS.values(), ids=list(CONTRACTS))
def test_columns_match_the_contract(catalog: Catalog, contract: DatasetContract) -> None:
    actual = [
        (c["name"], c["type"], c["default_kind"], c["default_expression"])
        for c in catalog["columns"]
        if c["table"] == contract.table
    ]
    expected = [(c.name, c.type, *expression(c)) for c in table_columns(contract)]
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
def test_up_and_down_rerun_and_down_drops_every_table_up_created(tmp_path: Path) -> None:
    """Goose records a version only after a file ran, so a crashed migration runs again."""
    sql = f"""
        {up(2)}
        {down(2)}
        SELECT name FROM system.tables WHERE database = currentDatabase() FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == []


@needs_clickhouse
def test_authors_papercount_upgrade_preserves_counts_and_refuses_lossy_rollback(
    tmp_path: Path,
) -> None:
    migration = MIGRATIONS / "00011_make_authors_papercount_nullable.sql"
    upgrade, downgrade = section(migration, "Up"), section(migration, "Down")
    existing = [
        {"authorid": "1", "papercount": 0},
        {"authorid": "2", "papercount": 4294967295},
    ]
    select = "SELECT authorid, papercount FROM authors ORDER BY authorid FORMAT JSONEachRow;"
    assert (
        clickhouse_local(
            f"""
        {section(MIGRATIONS / "00005_create_authors.sql", "Up")}
        INSERT INTO authors (authorid, papercount) VALUES ('1', 0), ('2', 4294967295);
        {upgrade}
        {upgrade}
        {downgrade}
        {upgrade}
        {select}
        """,
            tmp_path,
        )
        == existing
    )
    clickhouse_local("INSERT INTO authors (authorid, papercount) VALUES ('3', NULL);", tmp_path)
    assert run(downgrade, tmp_path).returncode != 0
    assert clickhouse_local(select, tmp_path) == [*existing, {"authorid": "3", "papercount": None}]
    assert clickhouse_local(
        "SELECT type FROM system.columns WHERE database = currentDatabase() "
        "AND table = 'authors' AND name = 'papercount' FORMAT JSONEachRow;",
        tmp_path,
    ) == [{"type": "Nullable(UInt32)"}]


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
