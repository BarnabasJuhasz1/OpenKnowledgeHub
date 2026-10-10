"""The ``okp`` command line."""

from __future__ import annotations

import json
from dataclasses import asdict

import click

from openknowledge_adapters.semantic_scholar import S2Error
from openknowledge_pipelines.flows.s2_download import DEFAULT_PREFIX, LATEST, s2_client, s2_download


@click.group()
def main() -> None:
    """OpenKnowledge pipelines."""


@main.group()
def s2() -> None:
    """Semantic Scholar datasets."""


@s2.command()
@click.option(
    "--release",
    default=LATEST,
    show_default=True,
    help="Release id, or latest to pin the newest release when the run starts.",
)
@click.option("--dataset", required=True, help="Dataset name, such as papers or abstracts.")
@click.option("--bucket", required=True, help="Target bucket name, without gs://.")
@click.option("--prefix", default=DEFAULT_PREFIX, show_default=True, help="Object prefix.")
def download(release: str, dataset: str, bucket: str, prefix: str) -> None:
    """Download one dataset of one release into the bucket, skipping files with a receipt."""
    try:
        s2_client()  # a missing key fails here, before any flow run exists
    except S2Error as exc:
        raise click.ClickException(str(exc)) from exc
    summary = s2_download(release=release, dataset=dataset, bucket=bucket, prefix=prefix)
    click.echo(json.dumps(asdict(summary)))
