# Resolved vectors bridge

`openknowledge_core.embeddings.resolution` defines the shared `ResolutionPlan`,
`ResolutionRelease`, `ResolvedArtifact`, and `resolution_identity` rules. The source
snapshot digest binds the plan, including the target release, model revision,
explicit scope, evidence, and ordered release selection digests. Base sequence is
zero; each subsequent release increments it by one and must name its predecessor.
There is no implicit latest selection.

`openknowledge_adapters.resolved.parquet.resolve_parquet_vectors` accepts a plan,
ordered `ConversionSelection` values, an `ArtifactStore` supplying pinned documents,
a `core.embedding_conversion.ports.ParquetArtifacts` implementation, and a destination
directory. Each selection pins archive completion and conversion complete.json
objects. The bridge verifies archive predecessor references, inventories, complete
manifest membership, conversion membership, source receipts, row counts and source
order. It consumes and exhausts `ParquetArtifacts.read(ConvertedShard)`, which owns
raw Parquet schema, byte digest and generation verification. Pipeline bootstrap injects
the packaged `StoredParquetArtifacts(store)` implementation. The resolver adapter
imports only the core port, never that sibling adapter. Empty parts may be evidenced
by inventory alone or explicit empty conversions; every nonempty part must appear
exactly once. No implicit latest selection or archive listing occurs.

The receipt bridge supports `corpus` and `bounded-sample` scope. Its
`completeness_evidence` must equal `conversion_completeness_evidence(selections, scope)`.
Corpus means the entire declared completed archive chain: every inventory shard and
nonempty update/delete part must be converted and verified. It does not assert that
the upstream archive contains every paper in the world. Fixtures and deliberately
partial upstream selections use bounded-sample scope. Encoder compatibility remains
an explicitly reviewed external requirement.

The storage implementation uses fixed 8-byte big-endian keys for unsigned UInt64
ordering. SQLite retains a state digest per historical `(corpusid, sequence)` and
only the latest full Float32 value per ID. The incremental
`resolution_writer` is shared by the sync fixture wrapper and the async port consumer;
it finalizes only after all input iterators end successfully. Equal-sequence disagreements fail even
when superseded by a newer release. Equal states deduplicate independently of input
provenance/shard order; final provenance derives from the bound plan and sequence.
Tombstones remain in the output.

Finalized output consists of read-only `part-00000000.parquet` and subsequent
ordered parts, plus `manifest.json`. Each part holds at most 262,144 rows; batches
hold at most 1,024 rows. The manifest pins every part size, SHA256 and row/live counts.
The manifest binds physical Parquet size/SHA256 and the existing canonical
`vector_record_bytes` digest over strictly ordered `EmbeddingMutation` records.
`iter_resolved_vectors` and `stream_resolved_vectors` check both identities while
streaming; consumers must exhaust the iterator before sealing a publication/load.
The async hook plugs directly into `VectorPublicationService.publish` and the
shared ClickHouse record boundary. `reuse_resolved_artifact` verifies the plan,
assurance label, and all bytes before returning an existing artifact. New resolution
never overwrites a destination; failed staging is removed and a partial destination
without its final manifest is never considered finalized.

The lower-level `resolve_vectors(plan, records, destination)` hook deliberately
labels artifacts `unverified-iterable`. It proves resolution of the supplied stream,
not source integrity or completeness.

Scratch currently lives in the destination's parent, so choosing that directory
chooses the scratch filesystem too. Corpus work must run on an appropriately sized
worker; control-VM disk capacity is not assumed. Record batches and SQLite cache
are bounded, and the manifest records input row and known Parquet byte counts.
Fixed part rotation bounds each Parquet writer's footer metadata. Only the small
manifest metadata grows with part count. Disk/throughput measurements on real inputs
remain a deployment gate.

The pipeline now includes the conversion and resolver dependencies and these installed
commands:

- `okp embeddings resolve --request request.json --destination artifact/` derives the
  plan identity from the selected completed archive chain and verifies its Parquet inputs.
- `okp embeddings load --artifact artifact/` publishes to ClickHouse through the existing
  deployment-admission and load-limit guards.
- `okp embeddings lookup --artifact artifact/ --id 123` selects the pinned publication
  from manifest metadata without scanning the local Parquet parts.

Dev checks cover receipt-backed archive → conversion → resolution → ClickHouse lookup,
including updates, deletes, replay, corruption and nonregular-file rejection. Wheel-installed
load and lookup also passed against local ClickHouse and Prefect with the normal guards;
verification used synthetic fixtures and downloaded no cloud corpus data. Cloud deployment,
real-input scratch/storage sizing, throughput and retrieval quality remain unverified.
