import gzip
import json
from pathlib import Path

import pytest

from examples.measure_fulltext import measure


def test_real_file_boundary_gzip_unicode_and_prefix(tmp_path: Path) -> None:
    line = (json.dumps({"corpusid": 1, "body": {"text": "α"}}) + "\n").encode()
    path = tmp_path / "sample.bin"
    path.write_bytes(gzip.compress(line * 2))
    result = measure(path, max_records=1)
    assert result.records == 1 and result.text_utf8_bytes == 2
    assert result.stop_reason == "record-limit"
    assert measure(path).records == 2
    assert measure(path).stop_reason == "eof"
    with pytest.raises(ValueError, match="limit exceeded"):
        measure(path, max_record_bytes=4)
    path.write_bytes(path.read_bytes()[:-5])
    with pytest.raises(EOFError):
        measure(path)


def test_lookup_absence_limits_and_exact_bound(tmp_path: Path) -> None:
    line = (json.dumps({"corpusid": 1, "body": {"text": "private body"}}) + "\n").encode()
    path = tmp_path / "local.gz"
    path.write_bytes(gzip.compress(line))
    found = measure(path, corpusid=1)
    assert found.lookup_status == "found" and found.match is not None
    assert found.match.line == 1
    assert "private body" not in found.model_dump_json()
    assert "unverified" in found.provenance
    assert measure(path, corpusid=2).lookup_status == "absent-in-file"
    # No lookahead beyond a record/total budget to claim EOF, even at exact EOF.
    assert measure(path, corpusid=2, max_records=1).lookup_status == "incomplete-scan"
    exact = measure(path, corpusid=2, max_decoded_bytes=len(line))
    assert exact.stop_reason == "decoded-byte-limit"
    assert exact.lookup_status == "incomplete-scan"
    assert measure(path, max_record_bytes=len(line)).records == 1
    with pytest.raises(ValueError, match="limit exceeded"):
        measure(path, max_record_bytes=len(line) - 1)
    with pytest.raises(ValueError, match="limit exceeded"):
        measure(path, max_decoded_bytes=len(line) - 1)
    path.write_bytes(gzip.compress(line + b"not-json\n"))
    assert measure(path, corpusid=1).stop_reason == "found-prefix"
    from openknowledge_core.ingestion.errors import IngestionError

    with pytest.raises(IngestionError):
        measure(path, corpusid=2)


def test_cli_local_entrypoint_and_redacted_failure(tmp_path: Path) -> None:
    import subprocess
    import sys

    path = tmp_path / "sample.gz"
    path.write_bytes(gzip.compress(b'{"corpusid":1,"body":{"text":"private body"}}\n'))
    result = subprocess.run(
        [sys.executable, "-m", "examples.measure_fulltext", str(path), "--corpusid", "1"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    assert json.loads(result.stdout)["lookup_status"] == "found"
    assert "private body" not in result.stdout
    path.write_bytes(gzip.compress(b'{"private body":\n'))
    failed = subprocess.run(
        [sys.executable, "-m", "examples.measure_fulltext", str(path)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert failed.returncode == 2
    assert "private body" not in failed.stderr
    assert "Traceback" not in failed.stderr
