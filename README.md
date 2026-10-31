# OpenKnowledge

## Semantic Scholar download, locally

Needs `uv`, the shared `PREFECT_API_URL`, and `gcloud auth application-default login` with an account that can create and read objects in the bucket.
Keep the API key in `~/.config/openknowledge/.env` as `S2_API_KEY=...` and let uv load it.
Run only `publication-venues` locally. Every other dataset downloads through Batch.
Direct local downloads do not participate in the shared managed-download budget.

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

Mutating commands, including `okp s2 pin` and `okp s2 download`, require the shared
`PREFECT_API_URL` and an open deployment admission gate; a temporary local server is insufficient.
`okp` turns Prefect server telemetry off.

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

All six managed download deployments share the `semantic-scholar` concurrency limit of 1.
One dataset job runs at a time, with up to `OKP_BATCH_PARALLELISM` parallel shards (8 when configured).
Busy requests fail promptly. Recovery handles queued/running jobs of the same dataset family;
active jobs from another family or legacy jobs without a known core dataset label block new work.
Legacy jobs labelled with one of the seven core datasets remain recoverable by core controllers.

## Full-text and SPECTER2 archives

Dedicated managed base/diff flows archive `s2orc_v2` and `embeddings-specter_v2` in the existing
raw GCS bucket. They reuse pinned inventories, receipts, manifests and resumable shard downloads.
Each dataset uses its own explicit release; these archives do not load into ClickHouse.
Managed archive commands require the shared Prefect server on the control VM, configured via
`PREFECT_API_URL` or the active Prefect profile, with the `semantic-scholar` limit created by
`okp serve`; they never fall back to a temporary local server. Both accept `--from-release`
for an explicit diff.

| Command | Deployments (base; diff) |
| --- | --- |
| `okp s2 archive-fulltext` | `s2-batch-download-fulltext/control-vm`; `s2-batch-download-fulltext-diff/control-vm` |
| `okp s2 archive-specter2` | `s2-batch-download-embeddings/control-vm`; `s2-batch-download-embeddings-diff/control-vm` |

`okp s2 read-base` emits a bounded JSONL prefix of a completed base for either archive
family, with generation-pinned source lineage. It needs storage read access only.
`ArchiveConsumer.stream` supports full traversal with `ConsumeBase(max_records=None)`;
close the async generator when stopping early. Prefix records are explicitly partial:
whole-shard checksums are verified on exhaustion. Archived diffs are not materialized.

## Coordinated sync

The Prefect deployment `s2-sync/control-vm` (or `okp s2 sync` on the shared server) updates
seven core datasets in ClickHouse, then archives full text and SPECTER2 in GCS.
It selects per-dataset releases and resumes contiguous diffs from verified progress.
Core failures, including a missing base, block both archives; initial bases require manual setup.
When citations holds a release the citation edge tables were not built from, the sync rebuilds
them before the archives; a failed rebuild blocks the archives like a core failure.
Core diffs above 30% of the configured full-base size are not loaded. This reference stays fixed
as diffs advance; refresh `core_baselines` after installing a replacement base.

Sync stages log start/end and one elapsed-time heartbeat every five minutes while waiting;
heartbeats indicate the runner is alive, not verified server progress. Citation rebuild logs
separate recovery, outgoing/incoming builds, count validation, and publication stages, including
standalone `okp s2 build-read-tables` runs. No additional ClickHouse grants are needed.

Each executed sync run publishes an `s2-sync-results` Markdown artifact in Prefect's run
Artifacts tab, with dataset releases/outcomes, edge counts, stage durations and image/version
metadata. Cancellation and SIGTERM are both reported as `interrupted`, since async flows receive
SIGTERM as a cancellation; Prefect owns whether the final run is cancelled, rescheduled or crashed.
Failed or gracefully interrupted runs retain observed partial results;
abrupt process termination cannot guarantee a final artifact. A failed or timed-out artifact upload warns without
changing the sync outcome. Artifacts exclude settings, source records and exception messages.

`OKP_SYNC` is JSON with `bucket`, optional `prefix` (default `semantic-scholar`), and three
initially empty fields: `core_baselines`, `archives`, and `enabled_archives`.
`core_baselines` maps datasets to manifest references returned by `okp s2 verify-base-manifest`.
`archives` maps `s2orc_v2` or `embeddings-specter_v2` to `{base, completion}`: `base` contains
`dataset`, `release`, `bucket`, and optional `prefix`; `completion` is the verified reference
returned by `okp s2 adopt-archive-base`. Both helpers validate existing downloaded bases.
`enabled_archives` lists which archive stages run; a baseline alone does not enable a stage.
Manual archive commands with `--from-release` also require the matching `archives` entry.

The daily 01:30 Europe/Zurich schedule defaults disabled; enable it with
`OKP_SYNC_SCHEDULE_ENABLED=true` in the serving process environment.
`CANCEL_NEW` cancels overlapping deployed runs before execution, without a backlog;
these cancellations do not trigger the `run_failed` log alert.

## Loading into ClickHouse

```sh
uv run okp s2 load --release <release_id> --dataset publication-venues --bucket openknowledge-508216-raw
```

It loads every object in the dataset's `manifest.json` into the dataset's table.
ClickHouse reads each object itself through its `raw` named collection and checks every record against the dataset's contract.
It needs `OKP_CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD` and `OKP_CLICKHOUSE_RAW_BUCKET`, the bucket that collection reads, but no S2 key.

`okp s2 load --measurement-report` optionally writes a new JSON report for that single
load. It distinguishes acknowledged inserts from skipped checkpoints and unknown partial
writes; memory values are snapshot gauges, not peaks. Use
`--measurement-kind synthetic-validation` for disposable development loads. Measurement errors are reported
separately and preserve the load outcome.

Each loaded object leaves a row in `loads`.
A rerun skips an object whose row matches its generation, its checksum, the table and the load SQL, so a table created again or emptied loads everything again.
Apply migrations 00011 to 00014 before starting loads with the new image. Inserts reject NULL into non-nullable columns. The shared insert safety setting changes upsert and delete SQL hashes for all seven supported datasets. Eligible base files for all seven and diff update/delete files for the six datasets with diffs replay when next loaded, including files already loaded with the previous image. Allow an idle window for the 00012 journal rewrite and these replays.
Migration 00014 orders citation versions by release, then deletion, then whether `contexts` is present, so a record with contexts beats the same record without.
A citations load refuses until 00014 is applied, and 00014 refuses while `citations` holds rows or checkpoints from the old order, whose merges may have dropped contexts.
To get past it, stop citation loads, run `RENAME TABLE citations TO citations_legacy`, run the Up statement of 00007 to create an empty table with a new UUID, apply the migrations and load the base release again from all its raw objects.
The old checkpoints no longer count and stay in place. Drop `citations_legacy` once that load passes its checks.
Migration 00015 adds `citation_edges_out` and `citation_edges_in`, slim tables ordered for
outgoing and incoming neighbourhood lookups. Migration 00017 adds papers_slim, narrow paper
metadata ordered by corpusid. The sync brings them current when papers or citations changes;
do it manually with `okp s2 build-read-tables` or the unscheduled Prefect
deployment `s2-build-read-tables/control-vm`. All three take the shared `clickhouse` limit.
When a source moved only by diffs since the release its read tables hold, they are updated in place:
the keys the diffs touched are deleted from them and inserted again from the source. Anything else
(a first build, a new base release, rows from before `citationid`) rebuilds all three, and so does
`--full`. A diff touching more than `OKP_READ_TABLES_MAX_DELTA_SHARE` of a read table's rows (0.1, the
most a full rebuild may drop) or more than `OKP_READ_TABLES_MAX_DELTA_KEYS` keys (100000000, which an
update holds in memory) rebuilds in full.
An in-place update needs free disk for its rows plus 5 GiB. If it fails after its deletes, the tables
stay half-updated until a rerun finishes the update; their applied markers move only at the end.
The edge tables retain duplicate citing/cited pairs, so readers use `DISTINCT`, and they are plain
MergeTree, so a query with `FINAL` on them fails. `intents` is a mask of background (1),
methodology (2) and result (4); a null and an empty list both give 0, and other values are dropped.
Migrations 00018 and 00020 add `citationid` to `citation_edges_out` and `citation_edges_in` so readers can fetch citation contexts by primary key; existing rows read 0 until rebuilt after the migration.
When `citation_edges_out` has IDs but `citation_edges_in` does not, only `citation_edges_in` is rebuilt,
from `citation_edges_out`: after migration 00020, one plain build fills `citation_edges_in.citationid`.
A rebuild requires completed papers and citations releases and records each table’s source
release and live UUID in applied. A full rebuild needs about 28 bytes of free disk per citations row plus
a provisional 100 bytes per papers row. All three counts are validated before any swap; each
must retain at least 90% of its live table (an empty live table passes). A refusal leaves the
live tables unchanged; each sync repeats the rebuild until an operator resolves the refusal. A cancelled rebuild's
insert keeps running in ClickHouse; the next rebuild stops it and removes stale staging tables
before checking disk capacity. Apply migrations 00017 to 00020 before the first rebuild. After deploying,
run s2-build-read-tables once, verify all three applied markers at their source releases, and
delete the old s2-build-citation-edges deployment from Prefect.

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

## Indexing papers into OpenSearch

```sh
uv run okp serve-index      # second serve process, on the OpenSearch VM
uv run okp search index     # queue the deployment and wait for its final state
```

Apply ClickHouse migration `00019` before starting the indexer. The `search-index/opensearch-vm`
deployment queues one run at a time, triggered by Completed or Failed runs of `s2-load`,
`s2-load-diff` and `s2-sync`, with a six-hour fallback schedule. It uses the shared Prefect
server (`PREFECT_API_URL`) and admission gate, and creates no S2 or ClickHouse concurrency
limits. Manual `okp search index` runs use the same deployment queue and need only the
shared Prefect connection locally; search credentials and settings belong to the OpenSearch VM.
On first installation the gate requires operator admission, as described above.

Readers query the `papers` alias. When `papers`, `abstracts` and `tldrs` are applied at one
release with matching live table UUIDs, the flow reads their latest rows into one document
per paper. If they are not aligned, it logs why and finishes without writes. Rows can be
newer than the pass's release: replacing merges discard older versions, so the indexer
always reads the latest state. The release bounds the key range and the diff's lower edge;
new IDs above the frozen range are picked up by the next diff. A contract
change or base table replacement builds a physical index named
`papers-v<contract>-<release>-<source fingerprint>`. After all parts succeed, the flow resets
refresh to its default, refreshes, and atomically switches the alias. Old indexes are retained.
Otherwise it updates the live index by ID for IDs touched after its recorded release,
covering missed releases in one pass, then refreshes and records the applied release.
A current run repairs the alias if needed and does no document writes.

`title`, `abstract` and `tldr` go through OpenSearch's `english` analyzer, which stems, so a match on `networks` finds `network`.
`publicationdate` is a `date` in `yyyy-MM-dd` that takes range queries and sorts, and a value in another shape leaves the field unindexed but keeps the document.
`title.exact` uses the `standard` analyzer without stemming.
The `lowercase` keyword normalizer makes filters on `venue`, `journal.name`, `s2fieldsofstudy.category`, `publicationtypes` and every mapped `externalids.*` ignore case; `_source` retains the original case.
`journal.volume` and `journal.pages` are keywords with their original case.
A keyword value longer than 8191 characters stays in `_source` but is not indexed, since OpenSearch refuses a term over 32766 bytes.
Changing the mapping means building a new index.
Parts are Prefect tasks with bounded concurrency. `OKP_INDEX_BASE_PARTS` defaults to 256,
`OKP_INDEX_DIFF_PARTS` to 18, and `OKP_INDEX_PARALLELISM` to 2. The roughly 255M papers
and 50M abstracts need about 1M papers per part: ClickHouse hashes a join side whole.
Diff joins read touched keys, so 18 parts target about 1M changed documents per part for a
weekly diff of 17–18M documents. Corpus ID distribution can be uneven; tune parts and
parallelism to the VM's memory. Each part streams into bulk requests of about 5 MB.
ClickHouse's `http_send_timeout` must allow for a stalled bulk request (30 seconds by default).

The `indexed` table stores frozen bounds, source UUIDs, contract version and completed parts.
A failed base leaves an unpublished partial index; a failed diff can leave some live documents
updated. Each part, the bounds/started receipt step and the closing step takes its own admission slot;
receipt and applied-state reads for planning take none.
Ordinary search exceptions release the slot, since requests are awaited and writes are idempotent.
Cancellation or uncertain admission still requires operator reconciliation as described below.
When a deploy closes admission, running parts drain and the next part is refused, failing the run.
Rerun after the deploy to skip successful parts. An unfinished base resumes at its own release
while contract, source UUIDs and part count match, even if applied has advanced. Once complete,
the next run diffs it to the applied release. No old index is deleted. Indexing holds no `clickhouse`
limit, so loads can continue. Rows changed during a read are sent again by the next pass.

Each document carries the largest `version` of its three rows, and the writes use `external_gte`, which accepts an equal version again.
Reading the latest rows keeps versions nondecreasing across resumed parts and later diffs.
The next diff re-reads keys touched after the receipt's release.
A bulk item that OpenSearch refuses with 429 is sent again up to 3 times, after 2, 4 and 8 seconds; an item still refused after that stops the pass with exit 1, and a rerun sends it again.
An index is created with `refresh_interval` at -1, so a base build does not write a small segment every second.
It needs `OKP_CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD`, `OKP_CLICKHOUSE_RAW_BUCKET`, `OKP_OPENSEARCH_URL` and `OPENSEARCH_PASSWORD`.
Unless set, `OKP_OPENSEARCH_USER` is `hub-indexer`, `OKP_OPENSEARCH_TIMEOUT_SECONDS` is 60 and `OKP_INDEX_SHARDS` is 5.
An unreachable OpenSearch or ClickHouse fails the flow; completed parts remain resumable.
`tests/test_search_e2e.py` runs it against ClickHouse and OpenSearch in Docker.

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

## Deployment admission

All mutating CLI commands and served flows hold a shared Prefect
`deployment-admission` slot for their lifetime, except search indexing, which admits each part
and its closing step separately. Read-only `read-base`,
`verify-base-manifest` and watchdog work while closed. Existing admitted Google Batch
shard workers finish without Prefect access; direct shard commands require admission.

Fresh `okp serve` creates the gate closed; restarts preserve it. Install this prerequisite
in an idle window with old clients disabled: existing binaries do not honor admission.
Never delete/recreate, deactivate or enable decay on the gate while clients can run.

- `okp admission status` prints `{"supported": true, "closed": bool, "active_slots": int}`.
- `okp admission close` sets capacity to zero, refusing new work while admitted work finishes.
- `okp admission open` reopens after deployment verification; it refuses while slots are held.
- `okp admission release --slots N` frees all held slots, only while closed and when `N`
  matches the held count. First reconcile every run and external job using admission owner
  logs, Prefect run states, Batch operations and ClickHouse activity (`system.processes`,
  checked as the ClickHouse container default user, not the `openknowledge` user).
  Never release to bypass drain.

Sync holds the `semantic-scholar` lease from before admission through the whole cycle,
including ClickHouse loads; managed downloads refuse while it runs. Batch recovery still
precedes each source operation.
Standalone loads, load-diff and citation edge rebuilds (CLI and served flows) acquire `clickhouse`
before admission and fail promptly when busy, without capacity retries. Sync waits for
ClickHouse while holding its source lease and admission; it does not refuse on load contention.
Source lease refusals or cancellation before admission take no admission slot.
Except for ordinary search exceptions, only successful work releases its slot.
Failed/interrupted runs and uncertain API responses
require operator reconciliation: external work may survive. Counter mutations are never
replayed on uncertain responses. This includes a load of a release that has not been archived,
even if validation failed before writes: close admission, verify no surviving work, and use
`admission release` before reopening. Loads require the archived manifest and objects first.
Closing alone does not establish idleness.
A scheduled sync or load starting while closed ends Failed and mails `run_failed`;
a gate left closed for days mails at every scheduled run.
