# Separate vector publications and offline evaluation

Stacked on `feat/embedding-contracts` at
`229aa888ae7dd532984c2da4146e0e43ba357235`, which follows canonical main
`378c05c` (Hub #60 merged). The feature diff excludes the shared contract commit.
The accepted lexical search
projection stays unchanged: no vector fields in `PAPERS`, no extra lexical source
reads, and no changes to its writer, CLI, settings or deployment. This opt-in library
adds a separate immutable vector index/publication boundary. No route is enabled.
The product target remains **all available papers with embeddings**; fixture and
integration budgets do not define permanent corpus subsets. Full text is independent.

## Input contract and publication

Shared `openknowledge_contracts.embeddings` is owned by the ClickHouse lane. Search
uses its `EmbeddingSnapshot` and `EmbeddingMutation` without duplicating them.
Three stores keep different roles: batch Parquet for reproducible processing,
ClickHouse for ID lookup and OpenSearch for retrieval. All share stable UInt64
corpusid, source dataset/release/model/revision, dimension/dtype, provenance and
explicit deletion semantics.

**Raw receipt-backed Parquet base shards are unresolved and duplicate-preserving.**
They cannot be passed directly to this publisher. An upstream resolver must supply:

- One `ResolvedVectors` descriptor: exact shared source identity, total unique
  records (including retained tombstones), live count, canonical content digest,
  resolution evidence, completeness evidence and encoder-space evidence references.
- One finite async stream in strictly increasing corpusid order with exactly one
  current mutation per ID, including tombstones. Every record must have the same
  full source identity and nonblank provenance. Histories must already be resolved;
  repeated IDs, even identical duplicates, are rejected. The publisher does not
  infer latest rows from shard order or silently resolve equal-sequence conflicts.

Canonical `resolved-vectors-v1` content is the concatenation of
`vector_record_bytes(record)`: shared-contract validated Float32 values, JSON object
with sorted keys, comma/colon separators, UTF-8, one newline per mutation, including
all source metadata, sequence and provenance. `sha256` is `sha256:HEX` over those
bytes, not the raw shard/Parquet bytes. Source snapshot identity remains the upstream
immutable manifest identity; do not relabel a raw sample hash as a resolved snapshot.
The upstream resolver/reviewer must verify evidence refs against the actual source
chain. Nonempty strings are not proof that external manifests or licences were read.
Known placeholders `unknown`/`unverified` model revisions are refused for publication.
No actual revision or query-adapter compatibility is inferred from `specter_v2`.

`VectorPublicationService.publish(publication, records)`:

1. Exclusively creates a fresh `paper-vectors-<32 lowercase hex>` physical index with
   `building` metadata. Choose a fresh UUID hex per attempt; lexical index names and
   aliases cannot be used. Existing indices are not adopted or overwritten.
2. Validates source, provenance, order, counts and digest while streaming batches of
   at most 128, with a 64 KiB per-record serialized bound. Create-only bulk actions retain tombstones without a vector field.
   Any item refusal, missing response, conflict or source failure fails the attempt.
3. After local digest/count agreement, blocks all writes using the Blocks API,
   refreshes and checks complete-shard total/live counts. Only then persists
   `complete` metadata and verifies its identity and write block before returning.

The [Blocks API](https://docs.opensearch.org/latest/api-reference/index-apis/blocks/)
waits for in-flight writes to finish before acknowledging the write block.
Publication metadata is persisted through the
[`_meta` mapping parameter](https://docs.opensearch.org/latest/mappings/mapping-parameters/meta/).
This is not a transaction with Parquet or ClickHouse, a signed receipt, or an alias
CAS. Direct port calls are implementation details: use the service for digest/order
validation. A failed/ambiguous attempt is abandoned; use a new index name with the
same resolved descriptor for a full retry. There is no automatic adoption, incremental
update, cleanup or resume. Client transport retries may yield a create conflict after
an ambiguous success; this conservatively abandons the attempt instead of guessing.
The caller owns/explicitly closes the source iterator and per-run client lifecycle.

## Explicit read and update boundary

`SearchVectorNeighbors` requires a publication, an independently expected source
snapshot, and a live `EmbeddingMutation` seed. An older publication cannot satisfy a
newer expected source. The adapter checks the physical index's exact persisted
publication metadata, completion state, write block and complete-shard total/live
counts on every read, then fetches
the seed. Absent/tombstoned seed means no vector neighbors, without replaying the
supplied old live vector. A live seed must match stored vector, sequence, source and
provenance exactly before its vector is sent in a query. Malformed/mismatched reads
are errors, never evidence that a vector is absent.

The query excludes self and tombstones, requests at most 100 neighbors, disables
partial search results and rejects timeout, failed shards, malformed/duplicate/self
hits or another index's results. Returned scores are OpenSearch scores, not raw
cosine values. `hybrid_ids(lexical_ids, neighbors)` offers explicit rank fusion (RRF
constant 60); BM25-only IDs are retained. Caller still applies its canonical paper
visibility/deletion policy when hydrating results. This library does not grant
visibility to papers deleted after the selected snapshot, validate current lexical
release alignment, or implement query encoding/inference. No free-text query encoder
is assumed compatible because its dimension happens to match.

An update/delete is a **new fully resolved immutable snapshot publication**, followed
by explicit caller selection using the newly expected source. Old create replays
cannot overwrite a sealed index or mutate the new publication. Tombstones persist
as documents; this boundary does not rely on OpenSearch's expiring delete-version
cache. There is no mutable “latest” selector here. Persisting/promoting a selection
and enforcing a no-rollback policy across processes is the consuming application's
responsibility; intentionally selecting an old source is not prevented by an
imaginary durable fence. Readers/writers assume trusted index administration: an
administrator able to remove the block, rewrite metadata or delete/recreate the
physical index can break immutability. Use separate least-privilege reader access
when deployment is designed; no IAM or credentials work is included.

Library wiring is deliberately opt-in: the bootstrap/composition root constructs
one configured `AsyncOpenSearch` client per run, injects it into
`OpenSearchVectors(client)`, then injects that port into
`VectorPublicationService(store)`. Await `publish` and persist the exact returned
`VectorPublication` only on success. Await `neighbors` with the expected source to
read it. No new bootstrap/CLI/settings are wired in this slice; behavior tests execute
this entry point through the pinned client's real serialization and fake transport.

## Engine scope and bounded integration verification

The adapter pins an explicit float32, 768-d Lucene HNSW cosine mapping with one
primary and zero replicas for **opt-in validation**, not a capacity recommendation.
Official [engine documentation](https://docs.opensearch.org/latest/mappings/supported-field-types/knn-methods-engines/)
lists Lucene HNSW with `cosinesimil`; the
[k-NN query reference](https://docs.opensearch.org/latest/query-dsl/specialized/k-nn/index/)
supports filtering with that engine. No compression or rescore tuning is implemented.
The mapping keeps `_source` enabled and explicitly disables vector derived source
(`index.knn.derived_source.enabled`, present in the
[3.9 plugin settings source](https://github.com/opensearch-project/k-NN/blob/3.9/src/main/java/org/opensearch/knn/index/KNNSettings.java))
so exact seed/provenance comparison does not depend on vector reconstruction. This
retains another vector copy on disk; no compressed-footprint savings are assumed.
The test compose pins OpenSearch 3.9.0 by digest; uv.lock pins opensearch-py 3.2.0.
Installed client source confirms create/bulk/mget/search/add_block/get_mapping/settings
APIs and transport retries (3; connection failures and 502–504; timeout retries off).
Transport fixtures alone do not establish engine behavior or real-vector quality.

The manager provisioned the isolated cached OpenSearch 3.9.0 container
`embedding-search-check` at **http://127.0.0.1:19203**, matching the compose digest,
with 2 CPU / 2 GiB RAM / 512 MiB heap. Only this endpoint and fresh fixture indices
are authorized. No service lifecycle/Docker operation or download is part of the
integration tests. The manager owns container cleanup. These fixture resources are
not an all-vector capacity recommendation.

`packages/adapters/tests/opensearch/test_vectors_integration.py` contains **seven**
opt-in cases, each using <=4 synthetic vector records and its own physical index
(`paper-vectors-5ea7...`; one `text-vector-check-...` lexical fixture). The client
has a 3-second request timeout and no retries. Source-location assertions prevent
silently testing another editable worktree. Run from this worktree with installed
dependencies, with its source roots first:

```bash
PYTHONPATH="$PWD/packages/core/src:$PWD/packages/contracts/src:$PWD/packages/adapters/src:$PWD/pipelines/src:$PWD" \
OKP_VECTOR_INTEGRATION=1 python -m pytest \
  packages/adapters/tests/opensearch/test_vectors_integration.py -q -s --tb=short
```

Tests cover actual mapping/settings (including disabled vector derived source),
canonical Float32 and UInt64 extremes, completion metadata, write blocking, counts,
filtered neighbors/scores, missing/deleted seeds, wrong identity/provenance, late
replay, incomplete/failed publication and reader count checks. A lexical fixture uses
the unchanged lexical writer and verifies that text replacement cannot alter the
separate sealed vector index. Adversarial completion-marker tests use only their
own fixture indices. No existing lexical index is inspected or modified.

All seven cases passed against the exact pinned OpenSearch 3.9.0 engine through a
regular dev-box SSH session. The isolated fixture used a 256 MiB tmpfs data directory
within the same 2 GiB memory limit to avoid the dev boot disk's watermark. The run
verified the actual wire representation of boolean query parameters, mapping,
source preservation, sealing and reads. The 35 feature unit cases also passed,
including Float32 underflow rejection and rounding before paper-query equality.
These are synthetic behavior checks, not real-corpus quality or capacity measurements.

The test module never provisions a service and skips unless explicitly opted in.
A repeat run requires a separately reserved isolated endpoint; fixture-container
cleanup is separate from the tests. No real corpus intake or benchmark ran.

## Offline evaluator and real-data gate

`tools/benchmark_search.py` and `core/search/pilot.py` preserve a separate bounded
exact-cosine baseline (<=512 vectors, <=32 queries, <=16 MiB input). It compares
supplied candidate IDs within that exact sample, exposes ideal payload arithmetic
and optionally fuses supplied BM25 ranks. It starts no client, model or service:

```bash
uv run --offline --no-sync python tools/benchmark_search.py /path/to/pilot.json
```

Input is `PilotInput`; use `PilotInput.model_json_schema()` to emit its JSON schema.
The offline evaluation envelope is not the shared ingestion/publication contract.
Its provenance/model evidence declarations require review. Synthetic behavior tests,
perfect fixture recall or tiny execution times do not establish real-corpus recall,
semantic relevance, compression, latency, throughput, capacity or cost.

No real sample currently exists. Use **one** shared package specified by the
manager's `embedding-batch/operator-export.md` handoff artifact: reconciled completion/
manifest/receipt metadata, <=512 total original records and lineage from <=3 pinned
objects, and <=32 paper seeds from the same vectors. Preserve duplicate raw rows.
Do not make separate lane export requests or fabricate receipts for prefixes. Missing
from the bounded sample is neither corpus absence nor deletion. Adapter/checkpoint/
preprocessing remain unknown until evidenced; no placeholder revision to bypass the
publication gate. Search additionally needs the manager-approved resolved projection
of that same package and its descriptor (not new cloud data). The resolver must explain
sequence/tombstone decisions and completeness scope: a derived test sample may be
explicitly complete **as a sample**, never labelled a complete full-corpus release.

Future all-vector capacity needs verified live N and d. Raw float32 payload is
N*d*4 bytes; ideal one-bit codes would be N*d/8, not actual RAM/index size. Add graph,
source/text duplication, replicas, JVM/native/page cache and old+new rebuild/scratch.
No verified N, index disk, ANN RAM or build time is available here. The supplied
4 CPU / 32 GiB / 500 GiB ~$200 text-only Infra #29 proposal is not approved all-vector
capacity. The old synthesis's 50 GB CH disk was stale (supplied correction: 1000 GB);
its synthetic recall, unverified counts and cheap all-vector estimates remain rejected.
The later exclusive benchmark should compare a validated compressed on_disk candidate
with explicit rescoring against an exact real-sample baseline; this Lucene fixture
slice does not establish engine portability, compression quality or the production tier.

## Rollout and recovery order

1. Keep the PR stacked on `feat/embedding-contracts` (`229aa888`); run independent
   sequential review and checks against that exact local dependency. Keep lexical
   serving and existing ingestion unchanged.
2. Repeat the opt-in integration when the engine/client contract changes, using a
   separately reserved isolated endpoint. Obtain the shared package and explicit
   resolved descriptor; verify source-space evidence before any real-vector publication.
3. Separately schedule the larger real-data quality/capacity benchmark and choose an
   all-vector deployment/availability budget. No full-corpus execution is approved here.
4. Wire the library in a separate opt-in composition root after operator approval.
   Publish a new physical index against a pinned resolved snapshot; do not select on
   failure or partial receipts. Persist the returned identity, reconcile coverage,
   confirm paper deletion/visibility policy, then explicitly promote the expected source.
5. Keep the previous validated selection and lexical-only mode for rollback. A failed
   attempt stays unselected; preserve bounded diagnostics and abandon its physical name.
   Rerun into a new index from the same immutable input. Cleanup/retention must be
   operator-coordinated after checking no consumer selects that index. Choosing an
   older snapshot deliberately requires the application's rollback policy; no automatic
   selector or durable cross-store CAS exists in this change.
