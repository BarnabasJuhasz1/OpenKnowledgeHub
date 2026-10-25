# Embedding ingest and lookup boundary

This slice is offline and not deployed. It defines the shared contract, bounded pure
validation/resolution, async ports and a SQL proposal outside migrations. There is no
production adapter, loader, publication service, CLI or cloud resource. All available
paper embeddings are the target; the tiny fixtures are correctness checks only.

## Shared contract handoff

Import `EmbeddingSnapshot`, `EmbeddingMutation`, `LookupEmbeddings` and
`EmbeddingLookupResult` from `openknowledge_contracts.embeddings`. Search consumes this
contract; changes to its fields require coordination with the owning embedding lane.

`EmbeddingSnapshot` fields are `snapshot`, `dataset`, `release`, `model`, `dimension`,
`dtype`, `model_revision`. `snapshot` is `sha256:<64 lowercase hex>` identifying an
immutable completed manifest/content, never `latest` or a mutable object path. This
syntax does not establish that a manifest exists or is complete. The producer defines
and records the canonical bytes hashed. The other fields are fixed to
`embeddings-specter_v2`, an explicit release, `specter_v2`, 768, `float32`, and an
explicit revision string (`unknown` when unknown). No query adapter can be inferred
from the upstream model label. A single identity must always have the same metadata.

`EmbeddingMutation` fields are `source` (the snapshot), `corpusid`, `sequence`,
`provenance`, `is_deleted`, `vector`. IDs and sequence are UInt64, including values
above signed Int64. Sequence is stable source release/diff order, never arrival time.
Live vectors contain 768 finite Float32 values with at least one nonzero coordinate;
tombstones have an empty vector. Validation canonicalizes to Float32, rejects overflow
and vectors that become entirely zero after rounding. Float64 JSON renderings of the
canonical values do not change the declared wire dtype. ClickHouse emits short Float32
JSON numbers; consumers must validate them through this contract before comparison.
Provenance is a stable nonsensitive source locator/ordinal, never signed URLs or secrets.

The same `(snapshot, corpusid, sequence)` must replay with equal canonical contents,
including provenance and deletion state. `validate_batch(source, rows)` checks metadata,
conflicts and the 3000-mutation budget within one batch, retaining source order and
identical duplicates. It cannot check earlier batches or concurrent writers.
`resolve_lookup(command, rows)` validates bounded candidates, selects the maximum sequence
before filtering deletes, and returns found/missing in first-requested ID order. Inputs
are limited to 3000 IDs and candidates to 3000 mutations. The adapter must perform global
replay validation and latest-row reduction before returning such candidates, rather than
feeding arbitrarily many historic duplicates to this helper. No full snapshot is loaded
into Python. Public Pydantic models must be constructed via validation; bypass APIs such
as `model_construct` and unchecked `model_copy(update=...)` are outside this boundary.

## Publication and failure requirements

Raw base-shard Parquet is a receipt-backed source-order stream with unresolved duplicates.
It is not a serving-resolved snapshot. No metadata here links a raw probe to complete
publication. A complete replacement must materialize the base and every contiguous diff,
including deletes; do not serve an incomplete diff as a complete snapshot.

A future adapter must compare every source field to a durable snapshot registry on ingest
and lookup, reject unknown/unpublished sources, validate conflicts across all batches,
and seal the snapshot against writes before publication. Durable compare-and-set of a
validated manifest pointer is required. Independent writers and concurrent publication
need exclusion; this PR supplies neither locks nor a registry. Metadata validation alone
cannot prove completeness or digest correctness. Failed staging stays invisible.

An ambiguous transport failure can leave partial inserts; retry the identical batch.
Preserve tombstones so stale replays cannot resurrect deleted embeddings. Checkpoint only
after acknowledged verification, with bounded concurrency/retries and useful errors;
record digests, source checksums, sequence ranges and verified counts without logging
vectors or credentials. No cancellation redesign is included. Paper deletion propagation
or an authoritative serving-time check remains necessary: embedding release identity does
not establish current paper deletion status. Retention and recovery policies remain open.

## SQL and precision

`clickhouse-proposal.sql` uses `MergeTree`, preserving replay evidence until validation.
ReplacingMergeTree could discard conflicting equal-sequence rows during background merges.
The two validation templates detect metadata reuse and replay conflicts. A future publisher
must enforce them while staging is sealed and compare metadata to the pinned manifest.
The lookup template uses `argMax` of the complete tuple before filtering tombstones.
Identical duplicates resolve deterministically; conflicting ties are forbidden, not resolved
by choosing an arbitrary winner. Query parameters never become SQL identifiers.

Snapshot is in the sorting key to isolate releases. No partitioning or ANN index is added.
The candidate granularity 256 is unmeasured. At 768 Float32 coordinates, vector-only bytes
per 64/256/8192-row granule are 196608/786432/25165824 (arithmetic, not measured reads).
Smaller granules increase mark overhead; compression, adaptive byte granularity, caches,
duplicates and snapshot retention affect actual cost. No broad benchmark is included.

Float32 is the baseline. BFloat16 remains experimental, with no quality/capacity claim;
Float16 Parquet must never be relabeled BFloat16. Quantization requires a real quality gate.
Tiny local tests use ClickHouse 26.8.22.13, verifying unsigned Parquet IDs, Float32 recovery,
duplicate ordering, tombstones, snapshot isolation, conflict gates and vector constraints.
No real corpus sample is available. Throughput, memory at scale, compression, latency,
quality, capacity, complete publication and production recovery remain unverified.

## Cost and rollout

Fixed resources created now: 0. Not deployed. Future storage, compute and operational cost
are unmeasured. Publication gating scans and duplicate retention are costs to measure with
an authorized real corpus later; the bounded Python helper does not bound a database scan.
Before production: implement and verify registry/sealing, durable replay checks, manifest
completeness, bounded parameterized serving, failure recovery and deletion freshness; then
measure the Float32 baseline on authorized data. No existing job or migration changes here.
