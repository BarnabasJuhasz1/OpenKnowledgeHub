"""Tiny checks against the deployed embedding template, without a server."""

import json
import struct
import subprocess
from pathlib import Path

import pytest

from tests.clickhouse_local import CLICKHOUSE, MIGRATIONS, needs_clickhouse, section

MIGRATION = MIGRATIONS / "00016_create_embedding_template.sql"


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


@needs_clickhouse
def test_template_preserves_uint64_float32_and_tombstones(tmp_path: Path) -> None:
    sql = section(MIGRATION, "Up") * 2
    sql += (
        "INSERT INTO paper_embeddings_template VALUES "
        "(18446744073709551615,18446744073709551615,'fixture:1',false,"
        "arrayWithConstant(768,toFloat32(0.1))), (0,1,'fixture:2',true,[]);"
        "SELECT corpusid, sequence, is_deleted, "
        "arrayMap(x -> reinterpretAsUInt32(x), vector) AS bits "
        "FROM paper_embeddings_template ORDER BY corpusid FORMAT JSONEachRow;"
    )
    result = engine(sql, tmp_path)
    assert result.returncode == 0, result.stderr
    deleted, live = [json.loads(line) for line in result.stdout.splitlines()]
    assert deleted == {"corpusid": 0, "sequence": 1, "is_deleted": True, "bits": []}
    assert live["corpusid"] == live["sequence"] == 2**64 - 1
    assert live["is_deleted"] is False
    assert live["bits"] == [struct.unpack("<I", struct.pack("<f", 0.1))[0]] * 768


@needs_clickhouse
@pytest.mark.parametrize(
    ("deleted", "vector"),
    [
        (False, "[]"),
        (False, "arrayWithConstant(768,toFloat32('nan'))"),
        (False, "arrayWithConstant(768,toFloat32('inf'))"),
        (False, "arrayWithConstant(768,toFloat32(0))"),
        (True, "arrayWithConstant(768,toFloat32(1))"),
    ],
)
def test_template_rejects_invalid_vectors(tmp_path: Path, deleted: bool, vector: str) -> None:
    sql = section(MIGRATION, "Up") + (
        "INSERT INTO paper_embeddings_template VALUES "
        f"(1,1,'fixture:1',{str(deleted).lower()},{vector});"
    )
    result = engine(sql, tmp_path)
    assert result.returncode != 0
    assert "valid_vector" in result.stderr
