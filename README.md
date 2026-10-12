# OpenKnowledgeHub

## Semantic Scholar download, locally

Needs `uv` and `gcloud auth application-default login` with an account that can create and read objects in the bucket.
Keep the API key in `~/.config/openknowledge/.env` as `S2_API_KEY=...` and let uv load it.
Run only `publication-venues` locally. Every other dataset waits for the Batch job.

```sh
uv sync
uv run --env-file ~/.config/openknowledge/.env okp s2 pin --bucket openknowledge-508216-raw --prefix test
uv run --env-file ~/.config/openknowledge/.env okp s2 download --release <release_id> --dataset publication-venues --bucket openknowledge-508216-raw --prefix test
```

`okp s2 pin` resolves `latest` and archives the release metadata.
It prints the explicit `release_id`, which every download of one import passes as `--release`.
That way datasets never mix releases.

`okp s2 download-shard --shard N` archives shard N of the pinned inventory without Prefect.
It lists the dataset once per process to find the shard's link, and the one request per second limit holds only within that process.

`--prefix` defaults to `semantic-scholar`. Local runs use `test`, so the folder can be deleted afterwards.

Every object is created once and never overwritten. Under `<prefix>/<release>/`:

- `release.json`: the release metadata with its README.
- `<dataset>/dataset.json`: the dataset metadata and the pinned inventory of its shards.
- `<dataset>/<file>`: the shard as served.
- `<dataset>/<file>.receipt.json`: its checksums and line count, written after the gzip check passed.
- `<dataset>/manifest.json`: every object that has a receipt, written by `okp s2 download` after the last receipt.

A rerun skips a shard whose receipt matches the stored object.
It rehashes an object that has no receipt.
It refreshes a signed link that has expired.

Settings come from the environment.
The key is `S2_API_KEY`, and every other setting is `OKP_` plus a field name from `pipelines/src/openknowledge_pipelines/settings.py`.

With Prefect's default profile and no `PREFECT_API_URL`, Prefect starts a temporary local server for the run.
`okp` turns that server's telemetry off.

## Loading into ClickHouse

```sh
uv run okp s2 load --release <release_id> --dataset publication-venues --bucket openknowledge-508216-raw
```

It loads every object in the dataset's `manifest.json` into the dataset's table.
ClickHouse reads each object itself through its `raw` named collection and checks every record against the dataset's contract.
It needs `OKP_CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD` and `OKP_CLICKHOUSE_RAW_BUCKET`, the bucket that collection reads, but no S2 key.

Each loaded object leaves a row in `loads`.
A rerun skips an object whose row matches its generation, its checksum, the table and the load SQL, so a table created again or emptied loads everything again.
`tests/test_load_e2e.py` runs it against ClickHouse and a fake GCS in Docker.

Before a pull request:

```sh
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
uv run pyright
uv run lint-imports
```
