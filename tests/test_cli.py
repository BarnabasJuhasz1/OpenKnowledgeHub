"""The okp command line."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from click.testing import CliRunner

from openknowledge_pipelines import cli
from openknowledge_pipelines.flows.s2_download import Summary


def test_download_passes_options_to_the_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def fake_flow(**kwargs: str) -> Summary:
        calls.append(kwargs)
        return Summary("2026-10-06", "papers", files=3, completed=1, skipped=2)

    monkeypatch.setattr(cli, "s2_client", lambda: None)
    monkeypatch.setattr(cli, "s2_download", fake_flow)
    result = CliRunner().invoke(
        cli.main, ["s2", "download", "--dataset", "papers", "--bucket", "raw"]
    )
    assert result.exit_code == 0, result.output
    assert calls == [
        {"release": "latest", "dataset": "papers", "bucket": "raw", "prefix": "semantic-scholar"}
    ]
    assert json.loads(result.output) == {
        "release": "2026-10-06",
        "dataset": "papers",
        "files": 3,
        "completed": 1,
        "skipped": 2,
    }


def test_dataset_and_bucket_are_required() -> None:
    result = CliRunner().invoke(cli.main, ["s2", "download", "--dataset", "papers"])
    assert result.exit_code == 2
    assert "--bucket" in result.output


def test_importing_the_package_turns_prefect_telemetry_off() -> None:
    env = {
        key: value for key, value in os.environ.items() if key != "PREFECT_SERVER_ANALYTICS_ENABLED"
    }
    code = (
        "import os, openknowledge_pipelines; print(os.environ['PREFECT_SERVER_ANALYTICS_ENABLED'])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "false"


def test_a_missing_key_fails_before_the_flow_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S2_API_KEY", raising=False)
    cli.s2_client.cache_clear()
    monkeypatch.setattr(cli, "s2_download", lambda **_: pytest.fail("the flow must not start"))
    result = CliRunner().invoke(
        cli.main, ["s2", "download", "--dataset", "papers", "--bucket", "raw"]
    )
    assert result.exit_code == 1
    assert "S2_API_KEY is not set" in result.output
