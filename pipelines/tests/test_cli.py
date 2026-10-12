import json
import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from click.testing import CliRunner
from prefect.deployments.runner import RunnerDeployment
from prefect.flows import load_flow_from_entrypoint

from openknowledge_core.ingestion import DownloadDatasetService, LoadDatasetService
from openknowledge_pipelines import bootstrap, cli, deployments
from openknowledge_pipelines.settings import LoadSettings, PipelineSettings
from tests.fakes.ports import PAPERS, PRODUCER, FakeSource, FakeStore, FakeTables, clock

ROOT = Path(__file__).resolve().parents[2]
TARGET = ["--bucket", "raw", "--prefix", "s2"]
LOAD_ENV = {
    "OKP_CLICKHOUSE_URL": "http://clickhouse:8123",
    "CLICKHOUSE_PASSWORD": "dev",
    "OKP_CLICKHOUSE_RAW_BUCKET": "raw",
}


def okp(*args: str) -> tuple[int, str]:
    result = CliRunner().invoke(cli.main, ["s2", *args])
    return result.exit_code, result.output


@pytest.mark.usefixtures("wired")
def test_pin_prints_the_explicit_release() -> None:
    code, output = okp("pin", *TARGET)
    assert code == 0, output
    assert json.loads(output) == {
        "release_id": "2026-10-06",
        "record": {"bucket": "raw", "name": "s2/2026-10-06/release.json"},
    }


@pytest.mark.usefixtures("wired", "prefect_server")
def test_download_runs_the_flow_and_prints_the_result() -> None:
    code, output = okp("download", "--release", "2026-10-06", "--dataset", "papers", *TARGET)
    assert code == 0, output
    result = json.loads(output.splitlines()[-1])
    assert list(result) == [
        "release_id",
        "dataset",
        "shards",
        "created",
        "recovered",
        "verified",
        "inventory",
        "manifest",
    ]
    assert (result["shards"], result["created"]) == (2, 2)


@pytest.mark.usefixtures("wired")
def test_download_shard_archives_one_shard() -> None:
    code, output = okp(
        "download-shard", "--release", "2026-10-06", "--dataset", "papers", "--shard", "1", *TARGET
    )
    assert code == 0, output
    result = json.loads(output)
    assert (result["index"], result["outcome"]) == (1, "created")
    assert result["receipt"]["name"] == "s2/2026-10-06/papers/part-1.gz.receipt.json"


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["download", "--dataset", "papers", *TARGET], "Missing option '--release'"),
        (
            ["download", "--release", "latest", "--dataset", "papers", *TARGET],
            "--release must look like 2026-10-06",
        ),
        (
            ["download", "--release", "2026-10-06", "--dataset", "Papers", *TARGET],
            "--dataset must look like publication-venues",
        ),
        (
            ["download-shard", "--release", "2026-10-06", "--dataset", "papers", *TARGET],
            "Missing option '--shard'",
        ),
        (["pin", "--bucket", "Raw"], "--bucket must be a bucket name"),
        (["pin", "--bucket", "raw", "--prefix", "a//b"], "--prefix must look like"),
        (
            ["download-shard", "--release", "2026-10-06", "--dataset", "papers", "--shard", "-1"]
            + TARGET,
            "--shard must be a whole number like 0",
        ),
        (
            ["download-shard", "--release", "2026-10-06", "--dataset", "papers", "--shard", "x"]
            + TARGET,
            "--shard must be a whole number like 0",
        ),
    ],
)
@pytest.mark.usefixtures("wired")
def test_bad_options_exit_2(args: list[str], message: str) -> None:
    code, output = okp(*args)
    assert code == 2
    assert message in output


@pytest.mark.usefixtures("prefect_server")
def test_load_runs_the_flow_without_an_s2_key(
    monkeypatch: pytest.MonkeyPatch, wired: DownloadDatasetService, store: FakeStore
) -> None:
    assert okp("download", "--release", "2026-10-06", "--dataset", "papers", *TARGET)[0] == 0

    @asynccontextmanager
    async def open_load_service(settings: LoadSettings) -> AsyncGenerator[LoadDatasetService]:
        yield LoadDatasetService(
            store=store, tables=FakeTables(store), producer=PRODUCER, clock=clock
        )

    monkeypatch.setattr(bootstrap, "open_load_service", open_load_service)
    monkeypatch.delenv("S2_API_KEY")
    for name, value in LOAD_ENV.items():
        monkeypatch.setenv(name, value)
    code, output = okp("load", "--release", "2026-10-06", "--dataset", "papers", *TARGET)
    assert code == 0, output
    assert json.loads(output.splitlines()[-1])["loaded"] == 2


def test_serve_offers_the_load_flow_by_module_path(monkeypatch: pytest.MonkeyPatch) -> None:
    served: list[RunnerDeployment] = []

    def fake_serve(*found: RunnerDeployment, limit: int) -> None:
        served.extend(found)

    monkeypatch.setattr(deployments, "prefect_serve", fake_serve)
    for name, value in LOAD_ENV.items():
        monkeypatch.setenv(name, value)
    result = CliRunner().invoke(cli.main, ["serve"])
    assert result.exit_code == 0, result.output
    [deployment] = served
    assert deployment.entrypoint is not None
    assert load_flow_from_entrypoint(deployment.entrypoint) is deployments.s2_load


def test_a_missing_key_fails_before_anything_opens(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def must_not_open(settings: PipelineSettings) -> AsyncGenerator[DownloadDatasetService]:
        pytest.fail("no service may open without a key")
        yield  # pragma: no cover

    monkeypatch.delenv("S2_API_KEY", raising=False)
    monkeypatch.setattr(bootstrap, "open_service", must_not_open)
    code, output = okp("pin", *TARGET)
    assert code == 1
    assert "S2_API_KEY is not set" in output


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("OKP_S2_ATTEMPTS", "many-topsecret"),
        ("OKP_IMAGE_DIGEST", "sha256:topsecret"),
        ("S2_API_KEY", "topsecret\u00a0"),
    ],
)
@pytest.mark.usefixtures("wired")
def test_a_bad_setting_is_named_without_its_value(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    code, output = okp("pin", *TARGET)
    assert code == 1
    assert name in output and "topsecret" not in output and "Value error" not in output


@pytest.mark.usefixtures("wired")
def test_an_option_error_names_the_option_once_with_a_valid_value() -> None:
    code, output = okp("pin", "--release", "2026-1-6", *TARGET)
    assert code == 2
    assert output.count("--release") == 1
    assert "must be latest or look like 2026-10-06" in output and "pattern" not in output


def test_the_group_help_shows_every_summary_whole() -> None:
    result = CliRunner().invoke(cli.main, ["s2", "--help"], terminal_width=60)
    code, output = result.exit_code, result.output
    assert code == 0
    listed = " ".join(output.split("Commands:")[1].split())
    assert all(
        text in listed for text in (cli.PIN_HELP, cli.DOWNLOAD_HELP, cli.DOWNLOAD_SHARD_HELP)
    )


def test_an_ingestion_error_exits_1_with_its_message(
    wired: DownloadDatasetService, source: FakeSource
) -> None:
    source.files["papers"]["part-0.gz"] = PAPERS["part-0.gz"][:-4]
    code, output = okp(
        "download-shard", "--release", "2026-10-06", "--dataset", "papers", "--shard", "0", *TARGET
    )
    assert code == 1
    assert "Error: part-0.gz: the gzip stream" in output and "rerun" in output


def test_importing_the_package_turns_prefect_telemetry_off() -> None:
    env = {"PATH": os.environ["PATH"]}
    code = (
        "import os, openknowledge_pipelines; print(os.environ['PREFECT_SERVER_ANALYTICS_ENABLED'])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "false"


NO_PREFECT = """
import sys
from contextlib import asynccontextmanager
from click.testing import CliRunner
from openknowledge_core.ingestion import DownloadDatasetService
from openknowledge_pipelines import bootstrap, cli
from tests.fakes.ports import PAPERS, PRODUCER, FakeSource, FakeStore, clock

source = FakeSource()
for name, data in PAPERS.items():
    source.add("papers", name, data)

@asynccontextmanager
async def open_service(settings):
    yield DownloadDatasetService(
        catalog=source, downloads=source, store=FakeStore(), producer=PRODUCER, clock=clock
    )

bootstrap.open_service = open_service
args = ["s2", "download-shard", "--release", "2026-10-06", "--dataset", "papers", "--shard", "0",
        "--bucket", "raw"]
result = CliRunner().invoke(cli.main, args, env={"S2_API_KEY": "k"})
print(result.exit_code, "prefect" in sys.modules)
"""


def test_download_shard_never_loads_prefect() -> None:
    env = {"PATH": os.environ["PATH"], "PYTHONPATH": str(ROOT)}
    result = subprocess.run(
        [sys.executable, "-c", NO_PREFECT], cwd=ROOT, env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["0", "False"]
