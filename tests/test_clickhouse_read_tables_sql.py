"""The read-table rebuild SQL, run with ``clickhouse local`` against the migrations."""

from __future__ import annotations

from pathlib import Path

from openknowledge_adapters.clickhouse.read_tables import COLUMNS, IN, OUT, SOURCES
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
