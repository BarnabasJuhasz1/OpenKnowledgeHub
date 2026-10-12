"""The ``okp`` command line, where only ``download``, ``load`` and ``serve`` load Prefect."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping
from typing import Any

import click
from pydantic import BaseModel, ValidationError

from openknowledge_core.ingestion import (
    DownloadDataset,
    DownloadShard,
    IngestionError,
    LoadDataset,
    PinRelease,
)
from openknowledge_pipelines import bootstrap
from openknowledge_pipelines.settings import CommonSettings, LoadSettings, PipelineSettings

PIN_HELP = "Archive a release's metadata and print it as JSON, with the explicit release_id."
DOWNLOAD_HELP = "Archive a whole dataset through the Prefect flow."
DOWNLOAD_SHARD_HELP = "Archive one shard of a dataset without Prefect."
LOAD_HELP = "Load an archived dataset into its ClickHouse table through the Prefect flow."
SERVE_HELP = "Serve the flows the control VM runs, until stopped."
RELEASE_HELP = "The release_id that okp s2 pin printed, such as 2026-10-06."
DATASET_HELP = "Dataset name, such as publication-venues."
PREFIX_HELP = "Folder in the bucket that every object goes under."
OPTION_RULES = {
    "release": "--release must look like 2026-10-06, the release_id that okp s2 pin printed",
    "dataset": (
        "--dataset must look like publication-venues: lowercase letters, digits, hyphens "
        "and underscores"
    ),
    "shard": "--shard must be a whole number like 0, counting the shards in dataset.json from 0",
    "bucket": (
        "--bucket must be a bucket name of 3 to 63 lowercase letters, digits, dots, hyphens "
        "and underscores, without gs://"
    ),
    "prefix": "--prefix must look like semantic-scholar, with no empty, '.' or '..' part",
}
PIN_RULES = {**OPTION_RULES, "release": "--release must be latest or look like 2026-10-06"}
SETTING_RULES = {
    "OKP_IMAGE_DIGEST": "OKP_IMAGE_DIGEST must be sha256: followed by 64 lowercase hex digits",
    "OKP_S2_API_URL": "OKP_S2_API_URL must be an http or https URL",
    "OKP_CLICKHOUSE_URL": "OKP_CLICKHOUSE_URL must be an http or https URL",
}
MISSING_KEY = (
    "S2_API_KEY is not set; keep it in ~/.config/openknowledge/.env and run okp through "
    "uv run --env-file ~/.config/openknowledge/.env"
)


def _default(kind: type[BaseModel], field: str) -> object:
    return kind.model_fields[field].default


@click.group()
def main() -> None:
    """OpenKnowledge pipelines."""


@main.group()
def s2() -> None:
    """Semantic Scholar datasets."""


@s2.command(help=PIN_HELP, short_help=PIN_HELP)
@click.option(
    "--release",
    default=_default(PinRelease, "release"),
    show_default=True,
    help="Release id, or latest.",
)
@click.option("--bucket", required=True, help="Target bucket name, without gs://.")
@click.option(
    "--prefix", default=_default(PinRelease, "prefix"), show_default=True, help=PREFIX_HELP
)
def pin(release: str, bucket: str, prefix: str) -> None:
    command = _command(PinRelease, PIN_RULES, release=release, bucket=bucket, prefix=prefix)
    settings = _settings(PipelineSettings)

    async def run() -> BaseModel:
        async with bootstrap.open_service(settings) as service:
            return await service.pin_release(command)

    _run(run)


@s2.command(help=DOWNLOAD_HELP, short_help=DOWNLOAD_HELP)
@click.option("--release", required=True, help=RELEASE_HELP)
@click.option("--dataset", required=True, help=DATASET_HELP)
@click.option("--bucket", required=True, help="Target bucket name, without gs://.")
@click.option(
    "--prefix", default=_default(DownloadDataset, "prefix"), show_default=True, help=PREFIX_HELP
)
def download(release: str, dataset: str, bucket: str, prefix: str) -> None:
    command = _command(
        DownloadDataset,
        OPTION_RULES,
        release=release,
        dataset=dataset,
        bucket=bucket,
        prefix=prefix,
    )
    settings = _settings(PipelineSettings)

    async def run() -> BaseModel:
        async with bootstrap.open_service(settings) as service:
            return await bootstrap.download_flow(service, settings)(command)

    _run(run)


@s2.command("download-shard", help=DOWNLOAD_SHARD_HELP, short_help=DOWNLOAD_SHARD_HELP)
@click.option("--release", required=True, help=RELEASE_HELP)
@click.option("--dataset", required=True, help=DATASET_HELP)
@click.option(
    "--shard",
    required=True,
    metavar="N",
    help="Index into the dataset's archived inventory, dataset.json.",
)
@click.option("--bucket", required=True, help="Target bucket name, without gs://.")
@click.option(
    "--prefix", default=_default(DownloadShard, "prefix"), show_default=True, help=PREFIX_HELP
)
def download_shard(release: str, dataset: str, shard: str, bucket: str, prefix: str) -> None:
    command = _command(
        DownloadShard,
        OPTION_RULES,
        release=release,
        dataset=dataset,
        shard=shard,
        bucket=bucket,
        prefix=prefix,
    )
    settings = _settings(PipelineSettings)

    async def run() -> BaseModel:
        async with bootstrap.open_service(settings) as service:
            return await service.archive_shard_at(command)

    _run(run)


@s2.command(help=LOAD_HELP, short_help=LOAD_HELP)
@click.option("--release", required=True, help=RELEASE_HELP)
@click.option("--dataset", required=True, help=DATASET_HELP)
@click.option("--bucket", required=True, help="Bucket holding the archive, without gs://.")
@click.option(
    "--prefix", default=_default(LoadDataset, "prefix"), show_default=True, help=PREFIX_HELP
)
def load(release: str, dataset: str, bucket: str, prefix: str) -> None:
    command = _command(
        LoadDataset,
        OPTION_RULES,
        release=release,
        dataset=dataset,
        bucket=bucket,
        prefix=prefix,
    )
    _settings(LoadSettings)
    from openknowledge_pipelines.deployments import s2_load

    _run(lambda: s2_load(command))


@main.command(help=SERVE_HELP, short_help=SERVE_HELP)
def serve() -> None:
    _settings(LoadSettings)
    from openknowledge_pipelines import deployments

    deployments.serve()


def _command[C: BaseModel](kind: type[C], rules: Mapping[str, str], **values: object) -> C:
    try:
        return kind.model_validate(values)
    except ValidationError as exc:
        errors = exc.errors(include_input=False, include_url=False)
        options = dict.fromkeys(str(error["loc"][0]) for error in errors if error["loc"])
    messages = [rules.get(option, f"--{option} is not valid") for option in options]
    raise click.UsageError("; ".join(messages) or "these options do not fit together")


def _settings[S: CommonSettings](kind: type[S]) -> S:
    try:
        return kind()  # pyright: ignore[reportCallIssue]  # fields come from the environment
    except ValidationError as exc:
        problems = [_setting_problem(error) for error in exc.errors(include_input=False)]
    raise click.ClickException("; ".join(problems))


def _setting_problem(error: Any) -> str:  # noqa: ANN401 - pydantic's ErrorDetails
    field = ".".join(str(part) for part in error["loc"])
    prefix = PipelineSettings.model_config.get("env_prefix", "")
    name = field if field.isupper() else f"{prefix}{field}".upper()
    if error["type"] == "missing":
        return MISSING_KEY if name == "S2_API_KEY" else f"{name} is not set"
    return SETTING_RULES.get(name, f"{name}: {str(error['msg']).removeprefix('Value error, ')}")


def _run(run: Callable[[], Coroutine[Any, Any, BaseModel]]) -> None:
    try:
        result = asyncio.run(run())
    except IngestionError as exc:
        message = str(exc)
    else:
        click.echo(result.model_dump_json())
        return
    raise click.ClickException(message)
