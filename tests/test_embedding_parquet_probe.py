from __future__ import annotations

import base64
import gzip
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from examples.embedding_parquet import convert
from openknowledge_core.ingestion.errors import IngestionError


def sample(tmp_path: Path, values: list[float], *, records: int = 3) -> tuple[Path, Path]:
    data = b"".join(
        json.dumps(
            {"corpusid": 2**64 - 1 - i, "model": "specter_v2", "vector": json.dumps(values)}
        ).encode()
        + b"\n"
        for i in range(3)
    )
    source = tmp_path / "source.gz"
    source.write_bytes(gzip.compress(data))
    compressed = source.read_bytes()
    receipt = tmp_path / "source.receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "semantic-scholar",
                "release_id": "2026-10-06",
                "dataset": "embeddings-specter_v2",
                "shard": {"source_path": "/source.gz", "file_name": "source.gz"},
                "object_ref": {
                    "path": {"bucket": "raw", "name": "base/source.gz"},
                    "generation": 1,
                },
                "size": len(compressed),
                "sha256": hashlib.sha256(compressed).hexdigest(),
                "md5": base64.b64encode(hashlib.md5(compressed).digest()).decode(),
                "crc32c": "AAAAAA==",
                "schema_ref": "fixture-only",
                "gzip_members": 1,
                "records": records,
                "uncompressed_size": len(data),
                "producer": {
                    "transform": "verbatim",
                    "transform_version": 1,
                    "software_version": "test",
                    "image_digest": None,
                },
                "completed_at": "2026-10-25T00:00:00Z",
            }
        )
    )
    return source, receipt


def test_roundtrip_batches_reuse_and_corruption(tmp_path: Path) -> None:
    source, receipt = sample(tmp_path, [0.1] * 768)
    destination = tmp_path / "v1"
    result = convert(source, receipt, destination, batch_rows=2)
    # Arrow stubs leave the optional columns parameter untyped.
    table = pq.ParquetFile(destination / "vectors.parquet").read()  # pyright: ignore[reportUnknownMemberType]
    assert table.column("corpusid").to_pylist() == [2**64 - 1, 2**64 - 2, 2**64 - 3]
    assert table.column("source_line").to_pylist() == [1, 2, 3]
    assert pq.ParquetFile(destination / "vectors.parquet").num_row_groups == 2
    assert result.rows == 3
    assert 0 < result.max_absolute_error < 1e-8
    assert convert(source, receipt, destination, batch_rows=2) == result
    (destination / "vectors.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="conflicts"):
        convert(source, receipt, destination, batch_rows=2)


@pytest.mark.parametrize(
    "values,records,limit",
    [
        ([0.1] * 768, 4, 100000),
        ([1e100] * 768, 3, 100000),
        ([0.1] * 767, 3, 100000),
        ([0.1] * 768, 3, 10),
    ],
)
def test_failed_validation_never_publishes(
    tmp_path: Path, values: list[float], records: int, limit: int
) -> None:
    source, receipt = sample(tmp_path, values, records=records)
    destination = tmp_path / "v1"
    with pytest.raises((ValueError, IngestionError)):
        convert(source, receipt, destination, max_record_bytes=limit)
    assert not destination.exists()
    assert not list(tmp_path.glob(".embedding-probe-*"))


def test_changed_source_rejected(tmp_path: Path) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    source.write_bytes(source.read_bytes()[:-1])
    with pytest.raises(ValueError, match="SHA256"):
        convert(source, receipt, tmp_path / "v1")


def replace_source(source: Path, receipt: Path, raw: bytes, *, members: int = 1) -> None:
    compressed = gzip.compress(raw)
    source.write_bytes(compressed)
    value = json.loads(receipt.read_text())
    value.update(
        size=len(compressed),
        sha256=hashlib.sha256(compressed).hexdigest(),
        md5=base64.b64encode(hashlib.md5(compressed).digest()).decode(),
        uncompressed_size=len(raw),
        records=raw.count(b"\n") + int(bool(raw) and not raw.endswith(b"\n")),
        gzip_members=members,
    )
    receipt.write_text(json.dumps(value))


@pytest.mark.parametrize(
    "field,value",
    [("sha256", "0" * 64), ("md5", "A" * 22 + "=="), ("gzip_members", 2), ("uncompressed_size", 1)],
)
def test_receipt_integrity_fields(tmp_path: Path, field: str, value: str | int) -> None:
    source, receipt = sample(tmp_path, [0.1] * 768)
    data = json.loads(receipt.read_text())
    data[field] = value
    receipt.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        convert(source, receipt, tmp_path / "out")
    assert not (tmp_path / "out").exists()
    assert not list(tmp_path.glob(".embedding-probe-*"))


@pytest.mark.parametrize("row", [b"\n", b"{}\n", b"[]\n", b'{"corpusid":true}\n', b"\xff\n"])
def test_malformed_rows(tmp_path: Path, row: bytes) -> None:
    source, receipt = sample(tmp_path, [0.1] * 768)
    replace_source(source, receipt, row)
    with pytest.raises(IngestionError):
        convert(source, receipt, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_float32_bits_duplicates_and_final_unterminated_line(tmp_path: Path) -> None:
    import struct

    values = ([0.0, -0.0, 0.1, -0.1, 2**-149, 2**-150, 3.4028234663852886e38] * 110)[:768]
    source, receipt = sample(tmp_path, values)
    row = gzip.decompress(source.read_bytes()).splitlines()[0]
    replace_source(source, receipt, row + b"\n" + row)
    result = convert(source, receipt, tmp_path / "out", batch_rows=1)
    table = pq.ParquetFile(tmp_path / "out/vectors.parquet").read()  # pyright: ignore[reportUnknownMemberType]
    assert result.rows == 2
    assert table.column("corpusid").to_pylist() == [2**64 - 1] * 2
    actual = table.column("vector").to_pylist()
    expected = b"".join(struct.pack("<f", value) for value in values)
    for vector in actual:
        assert vector is not None
        assert b"".join(struct.pack("<f", value) for value in vector) == expected


@pytest.mark.parametrize("field", ["rows", "parquet_bytes"])
def test_replay_rejects_tampered_output_receipt(tmp_path: Path, field: str) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    out = tmp_path / "out"
    convert(source, receipt, out)
    data = json.loads((out / "receipt.json").read_text())
    data[field] += 1
    (out / "receipt.json").write_text(json.dumps(data))
    with pytest.raises(ValueError, match="conflicts"):
        convert(source, receipt, out)


def test_publish_failure_cleanup_and_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    out = tmp_path / "out"

    def fail_rename(source: Path, destination: Path) -> None:
        raise OSError("fixture publication failure")

    with monkeypatch.context() as patch:
        patch.setattr("examples.embedding_parquet.os.rename", fail_rename)
        with pytest.raises(OSError, match="fixture publication failure"):
            convert(source, receipt, out)
    assert not out.exists()
    assert not list(tmp_path.glob(".embedding-probe-*"))
    assert convert(source, receipt, out).rows == 3


def test_diff_and_symlink_destinations_rejected(tmp_path: Path) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    out = tmp_path / "out"
    out.symlink_to(tmp_path / "absent", target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        convert(source, receipt, out)
    out.unlink()
    data = json.loads(receipt.read_text())
    data["object_ref"]["path"]["name"] = "diffs/source.gz"
    receipt.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="base shards"):
        convert(source, receipt, out)


def test_real_cli(tmp_path: Path) -> None:
    import subprocess
    import sys

    source, receipt = sample(tmp_path, [0.0] * 768)
    command = [
        sys.executable,
        "-m",
        "examples.embedding_parquet",
        str(source),
        str(receipt),
        str(tmp_path / "out"),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
    assert json.loads(result.stdout)["rows"] == 3
    failure = subprocess.run(
        command + ["--batch-rows", "0"], capture_output=True, text=True, timeout=30
    )
    assert failure.returncode == 2
    assert "batch_rows must be" in failure.stderr
    assert "Traceback" not in failure.stderr


@pytest.mark.parametrize("kind", ["truncated", "crc", "trailing-junk"])
def test_gzip_failure_even_with_matching_compressed_hash(tmp_path: Path, kind: str) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    data = bytearray(source.read_bytes())
    if kind == "truncated":
        del data[-4:]
    elif kind == "crc":
        data[-8] ^= 255
    else:
        data.extend(b"junk")
    source.write_bytes(data)
    value = json.loads(receipt.read_text())
    value.update(
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
        md5=base64.b64encode(hashlib.md5(data).digest()).decode(),
    )
    receipt.write_text(json.dumps(value))
    with pytest.raises(IngestionError):
        convert(source, receipt, tmp_path / "out")
    assert not (tmp_path / "out").exists()
    assert not list(tmp_path.glob(".embedding-probe-*"))


def test_empty_and_concatenated_gzip(tmp_path: Path) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    raw = gzip.decompress(source.read_bytes())
    replace_source(source, receipt, b"")
    assert convert(source, receipt, tmp_path / "empty").rows == 0
    replace_source(source, receipt, raw, members=2)
    data = gzip.compress(raw[:100]) + gzip.compress(raw[100:])
    source.write_bytes(data)
    value = json.loads(receipt.read_text())
    value.update(
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
        md5=base64.b64encode(hashlib.md5(data).digest()).decode(),
    )
    receipt.write_text(json.dumps(value))
    assert convert(source, receipt, tmp_path / "multi").rows == 3


@pytest.mark.parametrize("corpusid", [-1, True, 2**64])
def test_invalid_ids(tmp_path: Path, corpusid: int | bool) -> None:
    source, receipt = sample(tmp_path, [0.0] * 768)
    raw = json.dumps(
        {"corpusid": corpusid, "model": "specter_v2", "vector": json.dumps([0.0] * 768)}
    ).encode()
    replace_source(source, receipt, raw)
    with pytest.raises((ValueError, IngestionError)):
        convert(source, receipt, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_replay_checks_actual_parquet_schema(tmp_path: Path) -> None:
    import pyarrow as pa

    source, receipt = sample(tmp_path, [0.0] * 768)
    out = tmp_path / "out"
    convert(source, receipt, out)
    output = out / "vectors.parquet"
    table = pa.Table.from_pylist([{"wrong_column": value} for value in [1, 2, 3]])
    with pq.ParquetWriter(output, table.schema) as writer:
        writer.write_table(table)
    data = json.loads((out / "receipt.json").read_text())
    data.update(
        parquet_bytes=output.stat().st_size,
        parquet_sha256=hashlib.sha256(output.read_bytes()).hexdigest(),
    )
    (out / "receipt.json").write_text(json.dumps(data))
    with pytest.raises(ValueError, match="schema or row count"):
        convert(source, receipt, out)


def test_nonregular_source_rejected_without_opening(tmp_path: Path) -> None:
    import os

    source, receipt = sample(tmp_path, [0.0] * 768)
    source.unlink()
    os.mkfifo(source)
    with pytest.raises(ValueError, match="regular files"):
        convert(source, receipt, tmp_path / "out")


@pytest.mark.parametrize("name", ["receipt.json", "vectors.parquet"])
@pytest.mark.parametrize("kind", ["fifo", "symlink"])
def test_replay_rejects_nonregular_artifacts_without_hanging(
    tmp_path: Path, name: str, kind: str
) -> None:
    import os
    import subprocess
    import sys

    source, receipt = sample(tmp_path, [0.0] * 768)
    out = tmp_path / "out"
    convert(source, receipt, out)
    artifact = out / name
    artifact.unlink()
    if kind == "fifo":
        os.mkfifo(artifact)
    else:
        artifact.symlink_to(source)
    result = subprocess.run(
        [sys.executable, "-m", "examples.embedding_parquet", str(source), str(receipt), str(out)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 2
    expected = "must be a regular file" if kind == "fifo" else "must not be a symlink"
    assert f"existing destination {name} {expected}" in result.stderr
    assert "Traceback" not in result.stderr
    assert result.stdout == ""
