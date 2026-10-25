# Local embeddings conversion example

The installed `okp embeddings` commands and managed Batch flow are documented in
[Archived embeddings to Parquet](../docs/embedding-conversion.md). This example is a
thin compatibility wrapper. From the repository root, using cached dependencies:

```sh
uv run --offline --frozen python -m examples.embedding_parquet \
  /local/shard.gz /local/shard.receipt.json /local/new-output-directory \
  --batch-rows 128 --max-record-bytes 1048576
```

This offline pilot is not deployed. It reads an existing gzip shard and its
`ShardReceipt`; it does not fetch sources, call services, or select a corpus.
It reuses `parse_paper`/`EmbeddingPaper` and the streaming integrity checker.
No new dependencies or shared contracts are introduced.

The caller must supply a base shard: `ShardReceipt` has no explicit base/diff
kind. Recognizable `diffs/` paths and delete schema references are rejected, but
that check cannot establish resolved snapshot provenance. Snapshot and mutation
contracts belong to the separate resolved-snapshot lane. This is not a whole
corpus loader and does not resolve deletes or duplicates.

Every source row is retained in order, including repeated corpus IDs. Output is
`vectors.parquet` (uint64 `corpusid`, one-based uint64 `source_line`, fixed list
of 768 float32 values) plus `receipt.json`. Schema metadata carries source lineage
and raw base semantics. Finite values are rounded to float32; overflow fails.
Signed zero and subnormal values are retained according to float32 rounding;
underflow can round to zero. The receipt reports maximum absolute rounding error.

The converter checks compressed size, SHA256, MD5, gzip framing/CRC, member count,
row count and uncompressed byte count against the receipt. Bucket CRC32C metadata
is not independently recomputed. Malformed rows, unsupported models, invalid IDs,
nonfinite vectors, oversized records and mismatches fail the entire attempt.
The record-byte limit includes the newline when present; a final line without a
newline is accepted. Source and receipt must be regular local files.

There is no total shard-size or row-count cap. Python row buffering is limited to
`batch_rows` (1–1024, default 128), plus a bounded record (default 1 MiB, maximum
8 MiB), compressed chunks and the existing decoder's maximum 8 MiB output step.
PyArrow also retains row-group footer metadata proportional to row-group count;
this is not a constant total-RSS guarantee. No production capacity or throughput
claim is made from fixtures.

Publication renames a completed sibling temporary directory into place on the
same filesystem. A failed attempt cleans up its temporary directory. Existing
outputs are never repaired in place: replay validates source integrity, options,
output size/hash, row count and schema/lineage before returning the old receipt.
A conflicting or incomplete destination fails; select a new path or inspect it
manually. Concurrent writers can race at rename; the loser fails and can retry
against the completed destination. A hard process exit can leave an ignored
`.embedding-probe-*` directory; remove it only after confirming no writer owns it.
There is no power-loss durability guarantee. Use trusted local input/output
parents; destination and output-file symlinks are rejected, but this is not a
security boundary against concurrent filesystem edits.
