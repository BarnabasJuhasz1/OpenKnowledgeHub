"""Tiny offline Parquet/engine checks; no shared service or corpus data."""

import json
import struct
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.clickhouse_local import CLICKHOUSE, needs_clickhouse

SOURCE = "sha256:" + "a" * 64
OTHER = "sha256:" + "b" * 64
SCHEMA = Path(__file__).resolve().parents[1] / "docs/embeddings/clickhouse-proposal.sql"


def engine(sql: str, scratch: Path) -> subprocess.CompletedProcess[str]:
    assert CLICKHOUSE is not None
    return subprocess.run(
        [
            CLICKHOUSE,
            "local",
            "--path",
            str(scratch),
            "--max_threads",
            "1",
            "--background_schedule_pool_size",
            "1",
            "--multiquery",
            "--query",
            sql,
            "--output_format_json_quote_64bit_integers=0",
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def ddl() -> str:
    return SCHEMA.read_text().split("-- Validation templates:")[0]


def insert(snapshot: str, sequence: int, deleted: bool = False, revision: str = "unknown") -> str:
    vector = "[]" if deleted else "arrayWithConstant(768,toFloat32(0.1))"
    return (
        "INSERT INTO paper_embeddings_staging VALUES "
        f"('{snapshot}','embeddings-specter_v2','2026-10-06','specter_v2','{revision}',"
        f"768,'float32',18446744073709551615,{sequence},'fixture:1',{int(deleted)},{vector});\n"
    )


@needs_clickhouse
def test_parquet_uint64_float32_lookup_and_tombstones(tmp_path: Path) -> None:
    version = engine("SELECT version()", tmp_path / "version")
    assert version.returncode == 0, version.stderr
    assert version.stdout.strip() == "26.8.22.13", "revalidate schema on a changed engine"
    parquet = tmp_path / "tiny.parquet"
    table = pa.Table.from_pylist(
        [{"corpusid": 2**64 - 1, "vector": [0.1] * 768}],
        schema=pa.schema([("corpusid", pa.uint64()), ("vector", pa.list_(pa.float32()))]),
    )
    # Arrow 26 is pinned; the older stubs leave unrelated writer options unknown.
    pq.write_table(table, parquet)  # pyright: ignore[reportUnknownMemberType]
    query = SCHEMA.read_text().split("-- Query template:")[1].split("SELECT corpusid", 1)[1]
    query = "SELECT corpusid" + query
    query = (
        query.replace("{snapshot:String}", f"'{SOURCE}'")
        .replace("{ids:Array(UInt64)}", "[toUInt64(18446744073709551615)]")
        .rstrip()
        .removesuffix(";")
        + " FORMAT JSONEachRow;"
    )
    sql = ddl() + insert(SOURCE, 2, True) + insert(SOURCE, 0) * 2 + insert(OTHER, 3)
    sql += query  # deleted even though the stale live replay arrived last
    sql += (
        "INSERT INTO paper_embeddings_staging SELECT "
        f"'{SOURCE}','embeddings-specter_v2',toDate('2026-10-06'),'specter_v2','unknown',"
        "768,'float32',corpusid,toUInt64(3),'parquet:1',toUInt8(0),vector "
        f"FROM file('{parquet}', Parquet);"
    )
    sql += query  # a genuinely newer update restores the embedding
    result = engine(sql, tmp_path / "engine")
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(rows) == 1
    assert rows[0]["corpusid"] == 2**64 - 1
    assert rows[0]["sequence"] == 3
    assert rows[0]["provenance"] == "parquet:1"
    assert struct.pack("!768f", *rows[0]["vector"]) == struct.pack(
        "!768f", *table.column("vector")[0].as_py()
    )


@needs_clickhouse
def test_publication_gates_detect_conflicts_and_metadata_reuse(tmp_path: Path) -> None:
    validations = (
        SCHEMA.read_text()
        .split("-- Validation templates:")[1]
        .split("\n", 1)[1]
        .split("-- Query template:")[0]
    )
    sql = ddl() + insert(SOURCE, 1) + insert(SOURCE, 1, True, revision="different")
    result = engine(sql + validations, tmp_path)
    assert result.returncode == 0, result.stderr
    assert len(result.stdout.splitlines()) == 2  # metadata conflict and sequence conflict


@needs_clickhouse
@pytest.mark.parametrize(
    "vector", ["[]", "arrayWithConstant(768,toFloat32('nan'))", "arrayWithConstant(768,0)"]
)
def test_engine_rejects_invalid_live_vectors(tmp_path: Path, vector: str) -> None:
    sql = insert(SOURCE, 1).replace("arrayWithConstant(768,toFloat32(0.1))", vector)
    result = engine(ddl() + sql, tmp_path)
    assert result.returncode != 0
    assert "valid_vector" in result.stderr
