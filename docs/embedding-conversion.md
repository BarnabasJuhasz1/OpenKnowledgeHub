# Archived embeddings to Parquet

Use the installed pipeline commands to convert completed raw archives for downstream
embedding resolution and lookup. Base/update rows keep every source row, duplicate
and UInt64 ID in order; deletes contain IDs and source-line numbers without vectors.
These artifacts are not resolved snapshots.

```sh
okp embeddings convert-local /local/source.gz /local/source.receipt.json /local/output
okp embeddings convert --release 2026-10-06 --bucket openknowledge-508216-raw
```

The local command validates a complete receipted base shard; it does not certify an
archive completion. The managed command validates the archive completion and submits
one Cloud Batch task per missing shard. It uses the existing Batch project, region,
service account, network, image and resource settings, without an S2 API key. It reads
existing archive objects only and never calls the upstream download API.

For a completed diff, add `--from-release`, `--part updates` or `--part deletes`, and
`--completion "$PINNED_COMPLETION"`, containing the exact generation/SHA256 HashedRef
JSON. A verified empty diff part publishes an empty conversion manifest. The operator
must select both parts for the downstream resolver; conversion does not apply deletes.

`okp serve` registers the unscheduled `embedding-conversion/control-vm` flow. Cloud
Batch invokes `okp embeddings convert-shard --plan ...`, with `OKP_SHARD` selecting
the task's shard. Plans and converted outputs are content/generation pinned. A final
completion references a manifest only after every shard's bytes and receipt agree.
Retries reuse verified outputs; conflicting or corrupt output fails rather than being
silently replaced. Active foreign Batch jobs refuse admission, and queued/running
conversion jobs are waited on before new work starts. This enumeration is not a
cross-controller lock; deployment admission must also protect the managed entrypoint.

PyArrow lives in the packaged Parquet adapter, included in the pipeline image. The
example script is only a compatibility wrapper. Conversion buffers bounded records
and row groups but requires scratch disk for one compressed source shard plus its
Parquet output. The verified reader stages one Parquet shard locally. Footer metadata
grows with row-group count; no constant total-memory or disk guarantee is claimed.

Validation uses synthetic archives, real Parquet and local HTTP storage fixtures.
No GCP corpus transfer or production job has run. Real throughput, compression and
capacity remain unmeasured. Publication here ends at raw Parquet artifacts; resolution,
ClickHouse lookup and OpenSearch serving are separate consumers.
