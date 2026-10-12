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

Before a pull request:

```sh
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
uv run pyright
uv run lint-imports
```

## Pipelines image

`pipelines/Dockerfile` builds one image for Batch download tasks, loads and the Prefect worker.
Its entry point is `okp`; the worker runs `prefect` from the same image.
Build it from the repository root:

```sh
docker build -f pipelines/Dockerfile -t pipelines .
docker run --rm pipelines s2 download-shard --help
```

Pull requests build and smoke-test it.
Every push to `main` publishes it as `europe-west4-docker.pkg.dev/openknowledge-508216/openknowledge/pipelines:<commit>` and writes the digest to the job summary.
Deployments pin that digest, never a tag, and pass it as `OKP_IMAGE_DIGEST`: receipts record it, and an image cannot know its own digest.
