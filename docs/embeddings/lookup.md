# Embedding ingest and lookup boundary

The packaged ClickHouse adapter publishes complete resolved Float32 snapshots and looks
up explicit IDs. It creates no ANN index, quantization, cloud resources or deployment.
Raw Parquet conversion and raw-to-resolved reduction are separate upstream jobs.

## Contracts and entrypoints

Import `EmbeddingSnapshot`, `EmbeddingMutation`, `LookupEmbeddings`,
`EmbeddingLookupResult` and `ResolvedVectors` from `openknowledge_contracts.embeddings`.
`ResolvedVectors` pins source metadata, total/live counts, the canonical SHA256, and
resolution/completeness/space evidence. Its evidence references require caller review;
a valid descriptor alone does not prove that upstream data is complete.

A source is an immutable `sha256:<64 lowercase hex>` identity with dataset, release,
model, dimension, dtype and evidenced model revision. Never select `latest`. Publications
reject unknown/unverified model revisions. IDs and sequence support the full UInt64 range.
Live rows have 768 finite Float32 coordinates and nonzero norm; tombstones have no vector.
Provenance must be a stable nonsensitive locator, never credentials or signed URLs.
Construct public models through validation; unchecked Pydantic bypass APIs are outside
this boundary.

Use `publish_embeddings(publisher, descriptor, rows)` from
`openknowledge_core.embeddings.service`. The publisher is
`ClickHouseEmbeddings(client)` or `connection.embeddings` from the existing per-run
`clickhouse.connect(config)` context. `rows` is an async stream of resolved
`EmbeddingMutation` winners in strictly increasing corpusid order, including tombstones.
The upstream resolver must reject conflicting equal-sequence replays and select the
highest sequence before removing any deletion state. Duplicate IDs at this boundary are
rejected even when equal. Do not publish an incomplete diff as a full snapshot.

Canonical digest bytes come from
`openknowledge_core.embeddings.serialization.vector_record_bytes`, shared with search.
Publication returns `EmbeddingPublication(descriptor, table)`. Lookup uses
`await connection.embeddings.lookup(LookupEmbeddings(source=source, corpusids=ids))`.
It verifies every pinned source field, accepts at most 3000 IDs, deduplicates requests,
and returns live winners and absent/deleted IDs in first-requested order. Empty lookups
still check publication identity. There is no implicit latest or fallback snapshot.

## Publication and failure behavior

Apply migration `00016_create_embedding_template.sql` before running a publisher. Each
job clones a private `MergeTree` attempt table, inserts at most 1000 rows / 4 MiB per request (oversized records fail),
and validates source/order/count/digest while consuming the input. It then streams the
stored rows back in corpusid order and verifies the same canonical digest and counts.
Reads use Float32 IEEE bits to retain signed zero and subnormals without decimal JSON
rounding. All coordinates remain Float32; no norm conversion or quantization is applied.

After verification, the complete descriptor is written to the attempt's table comment.
One atomic `RENAME TABLE` publishes it as `paper_embeddings_<snapshot hex>`. A failed or
incomplete attempt has no published name. An existing destination is never replaced:
an exact descriptor replay succeeds, and any differing descriptor fails. Concurrent
attempts use distinct names; one publication wins and equal losers verify its descriptor.
Different source metadata under the same snapshot cannot be published or looked up.
Published tables must remain immutable; only this publisher should own their write/DDL
permissions. There is no multi-store transaction or registry service.

There are no automatic insert retries. An ambiguous insert may already have written rows;
retry the whole job with a fresh iterator and fresh attempt, using a finite caller-owned
retry budget and the configured HTTP timeout. If a successful rename response is lost,
a whole-job replay reloads and verifies the stream before accepting the equal published
descriptor; it does not skip ingestion. The command/bridge caller owns the source
iterator lifetime and must close it on early failure (for example using `aclosing`).
Ordinary failures attempt
one cleanup; process loss or failed cleanup may leave invisible attempt tables. Operators
must remove abandoned `paper_embeddings_attempt_*` tables only after confirming their
jobs are no longer running. Do not modify or remove published tables while readers use them.
Rollback of the template migration deliberately leaves published/attempt tables intact.

## Deployment limits

Grant the job CREATE/INSERT/SELECT/ALTER/RENAME/DROP on its embedding tables and SELECT
on `system.tables`; use an Atomic database. Serving accounts need SELECT on published
tables and `system.tables`. Publication performs a full stored-row verification scan;
Python buffering is bounded, but database work and temporary disk are corpus-dependent.
The job needs space for an entire attempt beside retained snapshots and concurrent jobs.
Disk preflight, retention, deletion freshness against current paper state, and deployment
of the packaged job remain operational responsibilities. No corpus size, throughput,
capacity or production recovery claim follows from tiny fixtures.

`packages/adapters/tests/clickhouse/test_embeddings_server.py` is the opt-in real HTTP
server boundary. Set `CLICKHOUSE_EMBEDDINGS_TEST_URL=http://127.0.0.1:18124` and optional
`CLICKHOUSE_EMBEDDINGS_TEST_USER` / `CLICKHOUSE_EMBEDDINGS_TEST_PASSWORD`. Tests create and
drop unique `embedding_lookup_test_*` databases, exercise migration idempotency and the
packaged connection, and never start a server. Local schema tests exercise the actual template migration, Float32 storage, UInt64 IDs
and vector constraints.

## Installed commands

`okp embeddings resolve --request request.json --destination resolved/` accepts a
`ResolveEmbeddings` JSON request with `model_revision`, `space_evidence`, `scope`
(`corpus` or `bounded-sample`) and `selections`. Each selection contains a pinned
archive `completion` HashedRef and `conversions`, the pinned conversion complete.json
HashedRefs for its parts. Supply the base followed by every contiguous completed diff.
The command derives release sequence, completeness evidence and snapshot identity;
operators do not compute another hash. Corpus scope covers that complete explicit
archive chain, rather than every paper worldwide. Resolution takes deployment admission.

`okp embeddings load --artifact resolved/` requires finalized receipt-backed metadata
and exhausts the verified source under the existing managed ClickHouse loader limit
and deployment admission. Retries are whole-job retries with a fresh iterator.
`okp embeddings lookup --artifact resolved/ --id 123 --id 456` reads source metadata
without scanning local Parquet, requires no deployment lease, and prints the typed
found/missing result. Both use existing `LoadSettings` ClickHouse configuration.

Request JSON and manifest.json must be nonsymlink regular files of at most 16 MiB;
reads check file size and enforce that bound during reading. Parquet parts must also
be nonsymlink regular files and match their finalized sizes before hashing/reading.
Parent directories and concurrent local file replacement remain trusted; these checks
are not a filesystem race isolation mechanism. No cloud or control-plane operation is
performed merely by installing these commands.
