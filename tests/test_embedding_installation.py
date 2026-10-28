"""Exercise wheel-installed entrypoints away from repository import paths."""

import os
import subprocess
import sys
from pathlib import Path

from tests.test_embedding_commands import artifact as artifact

ROOT = Path(__file__).resolve().parents[1]


def test_installed_commands_and_resolved_extra(tmp_path: Path, artifact: Path) -> None:
    wheels, site = tmp_path / "wheels", tmp_path / "site"
    env = {**os.environ, "UV_CACHE_DIR": "/tmp/embedding-uv-cache"}
    build = subprocess.run(
        ["uv", "build", "--all-packages", "--wheel", "--offline", "--out-dir", str(wheels)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert build.returncode == 0, build.stderr
    installed = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--offline",
            "--no-deps",
            "--python",
            sys.executable,
            "--target",
            str(site),
            *(str(path) for path in sorted(wheels.glob("*.whl"))),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert installed.returncode == 0, installed.stderr
    env["PYTHONPATH"] = str(site)
    # Import the resolved extra with unrelated backends blocked. Every project must
    # come from the installed wheels; this process cannot silently use editable code.
    script = """
import importlib, importlib.abc, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
class OnlyResolved(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'httpx', 'gcloud', 'aiohttp', 'opensearchpy', 'prefect'}:
            raise ImportError('unrelated backend: ' + fullname)
        if fullname == 'openknowledge_adapters.parquet':
            raise ImportError('resolved must not depend on another adapter extra')
sys.meta_path.insert(0, OnlyResolved())
for name in ('openknowledge_contracts', 'openknowledge_pipelines',
             'openknowledge_core.embeddings.resolution',
             'openknowledge_adapters.resolved.store', 'openknowledge_adapters.resolved.parquet'):
    module = importlib.import_module(name)
    assert pathlib.Path(module.__file__).resolve().is_relative_to(root), module.__file__
"""
    imports = subprocess.run(
        [sys.executable, "-c", script, str(site)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert imports.returncode == 0, imports.stderr
    executable = site / "bin" / "okp"
    for command in ("resolve", "load", "lookup"):
        help_result = subprocess.run(
            [str(executable), "embeddings", command, "--help"],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert help_result.returncode == 0, help_result.stderr
    # An installed command reads a real finalized manifest, then refuses missing
    # configuration before any client construction, even without its Parquet files.
    (artifact / "part-00000000.parquet").unlink()
    for key in ("OKP_CLICKHOUSE_URL", "CLICKHOUSE_PASSWORD", "OKP_CLICKHOUSE_RAW_BUCKET"):
        env.pop(key, None)
    lookup = subprocess.run(
        [
            str(executable),
            "embeddings",
            "lookup",
            "--artifact",
            str(artifact),
            "--id",
            str(2**64 - 1),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert lookup.returncode == 1 and "OKP_CLICKHOUSE_URL is not set" in lookup.stderr
