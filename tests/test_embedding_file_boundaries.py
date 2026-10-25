"""Regular-file gates must reject hostile local inputs promptly, before opening them."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from openknowledge_core.embeddings.resolution import ResolveEmbeddings
from tests.test_embedding_commands import artifact as artifact
from tests.test_embedding_commands import request


@pytest.mark.parametrize("kind", ["fifo", "symlink"])
@pytest.mark.parametrize("target", ["manifest", "part"])
def test_resolved_inputs_reject_nonregular_files_promptly(
    artifact: Path,
    tmp_path: Path,
    kind: str,
    target: str,
) -> None:
    path = artifact / ("manifest.json" if target == "manifest" else "part-00000000.parquet")
    outside = tmp_path / "outside"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    if kind == "fifo":
        os.mkfifo(path)
    else:
        path.symlink_to(outside)
    if target == "manifest":
        args = [
            str(Path(sys.executable).parent / "okp"),
            "embeddings",
            "lookup",
            "--artifact",
            str(artifact),
            "--id",
            "1",
        ]
    else:
        script = (
            "from pathlib import Path; import sys; "
            "from openknowledge_adapters.resolved.store import "
            "read_resolved_artifact, iter_resolved_vectors; "
            "p=Path(sys.argv[1]); list(iter_resolved_vectors(p, read_resolved_artifact(p)))"
        )
        args = [sys.executable, "-c", script, str(artifact)]
    result = subprocess.run(
        args, cwd=tmp_path, capture_output=True, text=True, check=False, timeout=5
    )
    assert result.returncode != 0
    assert "regular" in result.stderr and "symlink" in result.stderr


@pytest.mark.parametrize("kind", ["fifo", "symlink", "oversized"])
def test_request_file_refused_before_configuration(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "request.json"
    if kind == "fifo":
        os.mkfifo(path)
    elif kind == "symlink":
        target = tmp_path / "target.json"
        target.write_text(request().model_dump_json())
        path.symlink_to(target)
    else:
        with path.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024 + 1)
    result = subprocess.run(
        [
            str(Path(sys.executable).parent / "okp"),
            "embeddings",
            "resolve",
            "--request",
            str(path),
            "--destination",
            str(tmp_path / "output"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 2
    assert "--request must be a regular nonsymlink JSON file (at most 16 MiB)" in result.stderr
    assert not (tmp_path / "output").exists()


def test_oversized_manifest_refused_before_configuration(artifact: Path, tmp_path: Path) -> None:
    path = artifact / "manifest.json"
    path.chmod(0o644)
    with path.open("wb") as stream:
        stream.truncate(16 * 1024 * 1024 + 1)
    result = subprocess.run(
        [
            str(Path(sys.executable).parent / "okp"),
            "embeddings",
            "lookup",
            "--artifact",
            str(artifact),
            "--id",
            "1",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 2 and "at most 16 MiB" in result.stderr


@pytest.mark.parametrize("revision", [" ", "\t\n", "unknown", "Unverified"])
def test_request_requires_evidenced_revision(revision: str) -> None:
    with pytest.raises(ValidationError, match="evidenced model revision"):
        ResolveEmbeddings.model_validate({**request().model_dump(), "model_revision": revision})
