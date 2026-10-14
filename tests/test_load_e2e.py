"""``okp s2 load`` against ClickHouse and a fake GCS in Docker, which CI must have."""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import shutil
import socket
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from click.testing import CliRunner
from prefect.testing.utilities import prefect_test_harness

from openknowledge_adapters import gcs
from openknowledge_core.ingestion import DownloadDataset, DownloadDatasetService
from openknowledge_pipelines import cli
from tests.clickhouse_local import up
from tests.fakes.ports import PRODUCER, FakeSource, clock

E2E = Path(__file__).parent / "e2e"
FIXTURE = (
    Path(__file__).parents[1] / "packages/contracts/tests/fixtures/s2/publication-venues.jsonl"
)
BUCKET = "openknowledge-508216-raw"
DATASET = "publication-venues"
DOCKER = shutil.which("docker")

pytestmark = pytest.mark.skipif(
    DOCKER is None and not os.environ.get("CI"), reason="no docker on PATH"
)


def _free_port() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return str(probe.getsockname()[1])


@pytest.fixture(scope="module")
def ports() -> Iterator[dict[str, str]]:
    ports = {"CLICKHOUSE_PORT": _free_port(), "GCS_PORT": _free_port()}
    env = {**os.environ, **ports}
    compose = [
        DOCKER or "docker",
        "compose",
        "-p",
        f"okp-e2e-{os.getpid()}",
        "-f",
        str(E2E / "compose.yaml"),
    ]
    subprocess.run([*compose, "up", "-d", "--wait"], env=env, check=True)
    try:
        subprocess.run(
            [
                *compose,
                "exec",
                "-T",
                "clickhouse",
                "clickhouse-client",
                "--multiquery",
            ],
            input=f"CREATE DATABASE openknowledge;\nUSE openknowledge;\n{up()}",
            text=True,
            env=env,
            check=True,
        )
        gcs_root = f"http://127.0.0.1:{ports['GCS_PORT']}"
        httpx.post(
            f"{gcs_root}/storage/v1/b", params={"project": "e2e"}, json={"name": BUCKET}
        ).raise_for_status()
        with prefect_test_harness():
            yield ports
    finally:
        subprocess.run([*compose, "down", "--volumes"], env=env, check=True)


def archive(ports: dict[str, str], release: str, lines: list[str]) -> None:
    """Archive one shard per line through the download service, as okp s2 download would."""
    source = FakeSource()
    source.releases = [release]
    for index, line in enumerate(lines):
        source.add(DATASET, f"part-{index}.gz", gzip.compress(line.encode() + b"\n", mtime=0))
    config = gcs.GcsConfig(
        request_timeout_seconds=60,
        min_transfer_rate=1024,
        api_root=f"http://127.0.0.1:{ports['GCS_PORT']}",
    )

    async def run() -> None:
        async with gcs.connect(config) as store:
            service = DownloadDatasetService(
                catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
            )
            pinned = await service.pin_dataset(
                DownloadDataset(release=release, dataset=DATASET, bucket=BUCKET)
            )
            for shard in pinned.shards:
                await service.archive_shard(pinned, shard)
            await service.write_manifest(pinned)

    asyncio.run(run())


def okp_load(ports: dict[str, str], release: str) -> tuple[int, str]:
    env = {
        "OKP_CLICKHOUSE_URL": f"http://127.0.0.1:{ports['CLICKHOUSE_PORT']}",
        "CLICKHOUSE_PASSWORD": "openknowledge",
        "OKP_CLICKHOUSE_RAW_BUCKET": BUCKET,
        "OKP_GCS_API_ROOT": f"http://127.0.0.1:{ports['GCS_PORT']}",
        "OKP_TASK_RETRY_DELAY_SECONDS": "0",
    }
    args = ["s2", "load", "--release", release, "--dataset", DATASET, "--bucket", BUCKET]
    result = CliRunner().invoke(cli.main, args, env=env)
    return result.exit_code, result.output


def loaded(ports: dict[str, str], release: str) -> dict[str, Any]:
    code, output = okp_load(ports, release)
    assert code == 0, output
    return json.loads(output.splitlines()[-1])


def query(ports: dict[str, str], sql: str) -> None:
    httpx.post(
        f"http://127.0.0.1:{ports['CLICKHOUSE_PORT']}",
        params={"database": "openknowledge"},
        headers={"X-ClickHouse-User": "openknowledge", "X-ClickHouse-Key": "openknowledge"},
        content=sql,
    ).raise_for_status()


def test_a_load_is_repeatable_and_rebuilt_from_raw(ports: dict[str, str]) -> None:
    archive(ports, "2026-10-06", FIXTURE.read_text().splitlines())
    first = loaded(ports, "2026-10-06")
    assert (first["loaded"], first["live_rows"]) == (2, 2)
    assert loaded(ports, "2026-10-06")["already_loaded"] == 2
    query(ports, "TRUNCATE TABLE loads")
    again = loaded(ports, "2026-10-06")
    assert (again["loaded"], again["live_rows"]) == (2, 2)


def test_a_record_that_breaks_the_contract_fails_the_load(ports: dict[str, str]) -> None:
    record = json.loads(FIXTURE.read_text().splitlines()[0])
    archive(ports, "2026-10-13", [json.dumps({**record, "surprise": 1})])
    code, output = okp_load(ports, "2026-10-13")
    assert code == 1
    assert "a record has a key the contract does not declare; the object stays unloaded" in output


def test_a_key_repeated_identically_is_counted(ports: dict[str, str]) -> None:
    record = json.loads(FIXTURE.read_text().splitlines()[0])
    archive(ports, "2026-10-20", [json.dumps(record), json.dumps(dict(reversed(record.items())))])
    result = loaded(ports, "2026-10-20")
    assert (result["live_rows"], result["identical_duplicates"]) == (1, 1)
