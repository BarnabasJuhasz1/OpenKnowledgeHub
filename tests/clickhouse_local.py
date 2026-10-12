"""SQL through ``clickhouse local``, for checks that need ClickHouse but no server.

They are skipped where no ``clickhouse`` binary is on the PATH, but fail under CI, which installs
one.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations" / "clickhouse"
CLICKHOUSE = shutil.which("clickhouse")

needs_clickhouse = pytest.mark.skipif(
    CLICKHOUSE is None and not os.environ.get("CI"), reason="no clickhouse binary on PATH"
)


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


def run(sql: str, workdir: Path, **params: str) -> subprocess.CompletedProcess[str]:
    """Run ``sql`` with query parameters, returning the process even when it fails."""
    assert CLICKHOUSE is not None, "CI runs these checks, so it must put clickhouse on the PATH"
    # Whether JSON output quotes 64-bit integers is a default that changed between releases, so
    # it is pinned: a UInt64 comes back as a JSON number.
    args = ["local", "--path", str(workdir), "--output_format_json_quote_64bit_integers=0"]
    args += [f"--param_{name}={value}" for name, value in params.items()]
    return subprocess.run(
        [CLICKHOUSE, *args, "--multiquery", "--query", sql],
        stdin=subprocess.DEVNULL,  # a piped stdin would be read as data for the INSERTs
        capture_output=True,
        text=True,
        check=False,
    )


def clickhouse_local(sql: str, workdir: Path, **params: str) -> list[dict[str, Any]]:
    """The rows ``sql`` prints as JSONEachRow, which must succeed."""
    result = run(sql, workdir, **params)
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]
