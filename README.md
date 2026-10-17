# OpenKnowledgeHub

## Semantic Scholar download, locally

Needs `uv` and `gcloud auth application-default login` with an account that can create and read objects in the bucket.
Keep the API key in `~/.config/openknowledge/.env` as `S2_API_KEY=...` and let uv load it.
Use `publication-venues` for local downloads; use managed Batch entry points for large archives.
The direct `download` and `download-shard` primitives retain their local behavior and settings.
Manual invocations are operator-managed: do not run them alongside managed jobs or other users
of the same S2 key. They do not acquire the shared Prefect budget or inspect orphaned Batch jobs.
Batch invokes `download-shard` internally for its scheduled tasks; those tasks belong to the
managed job's budget.

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

A dataset's diff from one release to the next goes under `<prefix>/diffs/<from>/<release>/<dataset>/`.
There `diff.json` pins its update and delete files, and `updates/` and `deletes/` each hold their files, receipts and `manifest.json`.

A rerun skips a shard whose receipt matches the stored object.
It rehashes an object that has no receipt.
It refreshes a signed link that has expired.

Settings come from the environment.
The key is `S2_API_KEY`, and every other setting is `OKP_` plus a field name from `pipelines/src/openknowledge_pipelines/settings.py`.

With Prefect's default profile and no `PREFECT_API_URL`, Prefect starts a temporary local server for the run.
`okp` turns that server's telemetry off.

## Downloading through Batch

On the control VM, `okp serve` also serves the deployment `s2-batch-download/control-vm`.
It pins the dataset, runs each shard without a receipt as a task of one Cloud Batch job, and writes the manifest.
Each task runs `okp s2 download-shard` from the same image with its shard in `OKP_SHARD`.
Up to `OKP_BATCH_PARALLELISM` tasks run at once, 1 by default.
Each task makes one listing call with the key, and S2 retries wait a random part of their backoff, so tasks that start together spread out.
A run makes up to `OKP_TASK_RETRIES` more rounds for shards still without a receipt, and a new run resumes the same way.

`s2-batch-download-diff/control-vm` does the same for one dataset's diff to the next release, with a job per part that has files.
Its tasks add `--from-release` and `--part` to `okp s2 download-shard`.

Batch VMs use Spot by default; `OKP_BATCH_PROVISIONING_MODEL=STANDARD` selects standard VMs.
Each submission keeps the same operation ID and job name across task retries, so a lost creation reply resumes that job rather than creating another one.
`OKP_BATCH_WAIT_TIMEOUT_SECONDS` bounds each job's whole wait, including queued time and API outages, to 24 hours by default, separately from each task's `OKP_BATCH_MAX_RUN_SECONDS`.
If that wait expires, the run fails and names the job.

All six managed download deployments take the same active Prefect global concurrency limit
`semantic-scholar` with limit 1 and no slot decay. `okp serve` creates it if missing; verify an
existing limit has these settings. Separate family entry points do not run dataset jobs simultaneously:
with `OKP_BATCH_PARALLELISM=8`, one managed job runs at most eight tasks in total, not eight per family.
Each process still has its own S2 rate limiter; jittered retries handle task startup bursts.

A busy request makes no acquisition retries and times out after one second at most in Prefect's
acquisition service. It may fail promptly rather than wait in a global queue. Start one managed
archive at a time; rerun a refused request after the active run ends. Each deployment queues its
own additional runs. The runner cap stays at three processes; briefly competing download runs can
still delay loads/watchdog startup, but do not hold runner slots indefinitely waiting for this limit.

Before pinning and before each round, the controller scans active download jobs across all listing
pages. It deletes queued jobs of its own family and waits for its own running jobs to end. Base and
diff jobs share family cleanup. An active foreign-family or unlabelled legacy download job causes
refusal without cancelling or waiting for it. Let that job end, or inspect and drain it manually,
then retry. A timed-out/cancelled controller may leave Batch work running. Lease renewal failure
stops the controller; the next run must inspect its orphaned jobs before submitting new work.
This is conservative recovery under the shared Prefect lease, not a distributed Batch reservation:
external manual workers, delayed requests and uncoordinated old controllers are outside its budget.

## Full-text and SPECTER2 raw archives

These archives write only to the existing `openknowledge-508216-raw` GCS bucket. They do not load
ClickHouse, create derived stores, or advance an applied checkpoint. The seven core datasets retain
`s2-batch-download/control-vm` and `s2-batch-download-diff/control-vm`; those entry points now reject
non-core datasets before opening clients. Fulltext accepts only `s2orc_v2`; embeddings accepts only
`embeddings-specter_v2`. Managed routes reject unknown and legacy dataset names.

Before upgrading, stop old controllers and drain **all** their queued/running download jobs to a
terminal state. Do not overlap old and new controllers: old code can cancel jobs across families.
Publish/pin an image containing this code for both the control runner and Batch tasks, then restart
`okp serve` to register the new deployments. No infrastructure changes are required by this code.
Use the existing Batch permissions, GCS object access and secret configuration. Managed CLI runs
need `PREFECT_API_URL` pointing at the same server as `okp serve`, ADC, `S2_API_KEY`,
`OKP_IMAGE_DIGEST`, and `OKP_BATCH_PROJECT`, `OKP_BATCH_REGION`, `OKP_BATCH_SERVICE_ACCOUNT`,
`OKP_BATCH_NETWORK`, `OKP_BATCH_SUBNETWORK`, `OKP_BATCH_IMAGE_REPOSITORY`,
`OKP_BATCH_S2_KEY_SECRET`. Use the same compute and key configuration across families.
`okp serve` also needs the existing ClickHouse/load/watchdog settings. No new schedules are added.

Initial explicit base commands (run one, wait for completion, then run the other):

```sh
uv run --env-file ~/.config/openknowledge/.env okp s2 archive-fulltext --release 2026-10-13 --bucket openknowledge-508216-raw
uv run --env-file ~/.config/openknowledge/.env okp s2 archive-specter2 --release 2026-10-06 --bucket openknowledge-508216-raw
```

Or submit to the served deployments, with the complete `command` parameter:

```sh
uv run prefect deployment run s2-batch-download-fulltext/control-vm --params '{"command":{"release":"2026-10-13","dataset":"s2orc_v2","bucket":"openknowledge-508216-raw","prefix":"semantic-scholar"}}'
uv run prefect deployment run s2-batch-download-embeddings/control-vm --params '{"command":{"release":"2026-10-06","dataset":"embeddings-specter_v2","bucket":"openknowledge-508216-raw","prefix":"semantic-scholar"}}'
```

The CLI runs the guarded flow directly and prints its Batch result; Prefect CLI submits a deployment
run. Both preserve explicit, source-specific releases. With the default prefix, base objects go to
`gs://openknowledge-508216-raw/semantic-scholar/2026-10-13/s2orc_v2/` and
`gs://openknowledge-508216-raw/semantic-scholar/2026-10-06/embeddings-specter_v2/`.
Receipts, immutable inventories and complete final manifests use the same layout as core archives.
Rerun the same parameters to recover/verify existing objects and download only unfinished shards.

For an explicit adjacent diff, add `--from-release` to the same CLI command:

```sh
uv run --env-file ~/.config/openknowledge/.env okp s2 archive-fulltext --from-release "$FROM_RELEASE" --release "$TO_RELEASE" --bucket openknowledge-508216-raw
uv run --env-file ~/.config/openknowledge/.env okp s2 archive-specter2 --from-release "$FROM_RELEASE" --release "$TO_RELEASE" --bucket openknowledge-508216-raw
uv run prefect deployment run s2-batch-download-fulltext-diff/control-vm --params "{\"command\":{\"from_release\":\"$FROM_RELEASE\",\"release\":\"$TO_RELEASE\",\"dataset\":\"s2orc_v2\",\"bucket\":\"openknowledge-508216-raw\",\"prefix\":\"semantic-scholar\"}}"
uv run prefect deployment run s2-batch-download-embeddings-diff/control-vm --params "{\"command\":{\"from_release\":\"$FROM_RELEASE\",\"release\":\"$TO_RELEASE\",\"dataset\":\"embeddings-specter_v2\",\"bucket\":\"openknowledge-508216-raw\",\"prefix\":\"semantic-scholar\"}}"
```

Set `FROM_RELEASE` and `TO_RELEASE` to a manually verified adjacent pair **for that dataset**;
these commands do not establish that either archive has diff support for any particular pair.
Missing diffs fail without producing a diff inventory/final manifest or advancing a checkpoint
(the destination release metadata may already be pinned). Diff objects retain explicit updates and
deletes under `semantic-scholar/diffs/<from>/<to>/<dataset>/`; each nonempty part gets its own manifest.
Full-text inventory, schema, size and access still require manual verification with the production
key before starting the initial download. No such facts are assumed here. Latest-release discovery,
automatic schedules, legacy archives, vector indexing and derived-store application remain outside
this workflow.

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
On the control VM, `okp serve` serves the same flow as the deployment `s2-load/control-vm`, so a run can start from the Prefect UI.

`okp s2 load-diff --from-release <release_id> --release <release_id> --dataset <name> --bucket <bucket>` loads one archived diff, its update files as upserts and then its delete files as tombstones, and `okp serve` serves it as `s2-load-diff/control-vm`.
Both load deployments take the Prefect global concurrency limit `clickhouse`, which `okp serve` creates with a limit of 1, so no two served loads overlap, and each load deployment runs one run at a time, so a second run of the same load waits on the server and holds no slot.
The `applied` table records each base load and diff that passed its checks, and a diff loads only into a table that holds the release it starts from or the one it brings, which is a rerun.
When lineage is recorded, a base loads only into an empty table or one holding the same release, and `publication-venues` also into one holding an older release.
A load of `publication-venues`, which has no diffs, flags the venues its release no longer lists in `removed`.

`okp serve` also serves `watchdog/control-vm`, which runs every 6 hours.
Each run writes `watchdog heartbeat`, then checks every table that follows diffs.
It writes one `watchdog stale` line per table whose last load was applied more than `OKP_WATCHDOG_MAX_AGE_DAYS` days ago, 14 by default.
A table never loaded does not count.
Stale data never fails a run, but a ClickHouse error does.
Both texts are fixed so that a log alert can match them.

Load recovery still needs persisted ownership of a begun load and shared coordination per table.
Until those are implemented, do not load a base over a populated table with missing lineage or overlap base and diff loads for the same dataset.

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
