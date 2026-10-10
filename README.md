# OpenKnowledgeHub

## Semantic Scholar download, locally

Needs `uv` and `gcloud auth application-default login` with an account that can create and read objects in the bucket.
Keep the API key in `~/.config/openknowledge/.env` as `S2_API_KEY=...` and let uv load it.

```sh
uv sync
uv run --env-file ~/.config/openknowledge/.env okp s2 download --dataset publication-venues --prefix test --bucket openknowledge-508216-raw
```

`--release` defaults to `latest` and is pinned to one release id when the run starts. `--prefix` defaults to `semantic-scholar`, local runs use `test` so the folder can be deleted afterwards.
Files land at `<prefix>/<release>/<dataset>/<file>`, each with a `<file>.receipt.json` written after the upload passed the gzip check.
A rerun skips every file that has a receipt and fetches fresh signed links when one has expired.
With Prefect's default profile and no `PREFECT_API_URL`, Prefect starts a temporary local server for the run. `okp` keeps its telemetry off.
Before a pull request run `uv run pytest`, then `uv run ruff check . && uv run ruff format --check .`.
