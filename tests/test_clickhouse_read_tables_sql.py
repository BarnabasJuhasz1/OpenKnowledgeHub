"""The read-table rebuild SQL, run with ``clickhouse local`` against the migrations."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from openknowledge_adapters.clickhouse.read_tables import (
    COLUMNS,
    DELTA,
    IN,
    KEY,
    OUT,
    PAPERS,
    SOURCES,
    apply_sql,
    delta_sql,
    in_only_sql,
)
from openknowledge_core.ingestion.read_tables import READ_TABLES
from tests.clickhouse_local import clickhouse_local, needs_clickhouse, up


@needs_clickhouse
def test_edge_rebuild_preserves_live_citation_ids(tmp_path: Path) -> None:
    sql = f"""
        {up()}
        SYSTEM STOP MERGES citations;
        INSERT INTO citations
            (citationid, citingcorpusid, citedcorpusid, source_release, is_deleted)
        VALUES (11, 1, 2, '2026-10-01', 0), (12, 1, 2, '2026-10-01', 0),
               (13, 1, 3, '2026-10-01', 0), (14, 1, NULL, '2026-10-01', 0);
        INSERT INTO citations
            (citationid, citingcorpusid, citedcorpusid, source_release, is_deleted)
        VALUES (11, 1, 9, '2026-09-01', 0);
        INSERT INTO citations (citationid, source_release, is_deleted)
        VALUES (13, '2026-10-02', 1);
        CREATE TABLE citation_edges_out_new AS citation_edges_out;
        INSERT INTO citation_edges_out_new ({COLUMNS[OUT]}) {SOURCES[OUT]};
        CREATE TABLE citation_edges_in_new AS citation_edges_in;
        INSERT INTO citation_edges_in_new ({COLUMNS[IN]}) {SOURCES[IN]};
        SELECT citing, cited, citationid FROM citation_edges_out_new
        ORDER BY citationid FORMAT JSONEachRow;
        SELECT citing, cited, citationid FROM citation_edges_in_new
        ORDER BY citationid FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == [
        {"citing": 1, "cited": 2, "citationid": 11},
        {"citing": 1, "cited": 2, "citationid": 12},
        {"citing": 1, "cited": 2, "citationid": 11},
        {"citing": 1, "cited": 2, "citationid": 12},
    ]


def _full_build(suffix: str) -> str:
    return "".join(
        f"CREATE TABLE {table}{suffix} AS {table};\n"
        f"INSERT INTO {table}{suffix} ({COLUMNS[table]}) {SOURCES[table]};\n"
        for table in READ_TABLES
    )


@needs_clickhouse
def test_incremental_update_equals_full_rebuild(tmp_path: Path) -> None:
    run = [
        *delta_sql("citations", "2026-10-01", "2026-10-03"),
        *apply_sql("citations"),
        *delta_sql("papers", "2026-10-01", "2026-10-03"),
        *apply_sql("papers"),
    ]
    # A run that stopped before its last insert, then two complete reruns.
    statements = ";\n".join([*run[:-1], *run, *run])
    publish = "".join(
        f"EXCHANGE TABLES {table}_new AND {table}; DROP TABLE {table}_new;\n"
        for table in READ_TABLES
    )
    compare = "".join(
        f"SELECT '{name}' AS copy, * FROM {name} ORDER BY all FORMAT JSONEachRow;\n"
        for table in READ_TABLES
        for name in (table, f"{table}_new")
    )
    sql = f"""
        {up()}
        SYSTEM STOP MERGES citations;
        SYSTEM STOP MERGES papers;
        INSERT INTO citations (citationid, citingcorpusid, citedcorpusid, isinfluential, intents,
            source_release, is_deleted)
        VALUES (11, 1, 2, 1, [['background']], '2026-10-01', 0),
               (12, 1, 2, 0, [], '2026-10-01', 0), (13, 1, 3, 0, [], '2026-10-01', 0),
               (14, 1, NULL, 0, [], '2026-10-01', 0), (15, 2, 3, 0, [], '2026-10-01', 0),
               (17, 2, 4, 0, [], '2026-10-01', 0);
        INSERT INTO papers (corpusid, title, year, citationcount, source_release, is_deleted)
        VALUES (1, 'a', 2020, 1, '2026-10-01', 0), (2, 'b', 2021, 2, '2026-10-01', 0),
               (3, 'c', NULL, 3, '2026-10-01', 0), (4, 'd', 2023, 4, '2026-10-01', 0);
        {_full_build("_new")}
        {publish}
        INSERT INTO citations (citationid, citingcorpusid, citedcorpusid, isinfluential, intents,
            source_release, is_deleted)
        VALUES (11, 1, 9, 1, [['background']], '2026-10-02', 0),
               (14, 1, 3, 0, [], '2026-10-02', 0), (17, 2, NULL, 0, [], '2026-10-02', 0),
               (16, 3, 1, 1, [['methodology'], ['result']], '2026-10-02', 0),
               (15, 2, 3, 1, [['result']], '2026-10-03', 0);
        INSERT INTO citations (citationid, source_release, is_deleted)
        VALUES (13, '2026-10-02', 1), (15, '2026-10-02', 1);
        INSERT INTO papers (corpusid, title, year, citationcount, source_release, is_deleted)
        VALUES (2, 'b2', 2021, 5, '2026-10-02', 0), (5, 'e', 2026, 0, '2026-10-02', 0),
               (4, 'd2', 2023, 6, '2026-10-03', 0);
        INSERT INTO papers (corpusid, source_release, is_deleted)
        VALUES (3, '2026-10-02', 1), (4, '2026-10-02', 1);
        {statements};
        {_full_build("_new")}
        {compare}
    """
    copies: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in clickhouse_local(sql, tmp_path):
        copies[row.pop("copy")].append(row)
    for table in READ_TABLES:
        assert copies[table] == copies[f"{table}_new"], table
    assert sorted(row["citationid"] for row in copies[IN]) == [11, 12, 14, 15, 16]
    assert [row["corpusid"] for row in copies[PAPERS]] == [1, 2, 4, 5]


@needs_clickhouse
def test_in_only_rebuild_fills_ids_from_out(tmp_path: Path) -> None:
    rebuild = ";\n".join(in_only_sql())
    sql = f"""
        {up()}
        INSERT INTO citation_edges_out (citing, cited, isinfluential, intents, citationid)
        VALUES (2, 1, 0, 1, 21), (1, 2, 1, 4, 22);
        INSERT INTO citation_edges_in (citing, cited, isinfluential, intents)
        VALUES (2, 1, 0, 1), (1, 2, 1, 4);
        {rebuild};
        SELECT * FROM citation_edges_in_new ORDER BY cited, citing FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == [
        {"citing": 2, "cited": 1, "isinfluential": False, "intents": 1, "citationid": 21},
        {"citing": 1, "cited": 2, "isinfluential": True, "intents": 4, "citationid": 22},
    ]


@needs_clickhouse
@pytest.mark.parametrize("source", ["citations", "papers"])
def test_delta_takes_keys_after_start_up_to_end(source: str, tmp_path: Path) -> None:
    delta = ";\n".join(delta_sql(source, "2026-10-01", "2026-10-03"))
    sql = f"""
        {up()}
        INSERT INTO {source} ({KEY[source]}, source_release, is_deleted)
        VALUES (1, '2026-10-01', 0), (2, '2026-10-02', 0), (3, '2026-10-03', 0),
               (4, '2026-10-04', 0), (5, '2026-10-02', 1);
        {delta};
        SELECT key FROM {DELTA[source]} ORDER BY key FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == [{"key": 2}, {"key": 3}, {"key": 5}]
