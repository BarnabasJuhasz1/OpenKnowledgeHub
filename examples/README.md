Run the local restricted metadata probe from the repository root, after installing
the workspace dependencies:

```sh
uv run python -m examples.measure_fulltext /path/to/local.jsonl.gz --corpusid 123 --max-records 100 --max-record-bytes 8388608 --max-decoded-bytes 67108864
```

Omit `--corpusid` for prefix measurements. Only local regular JSONL or gzip files
are intended; no downloads, cloud clients, receipts, or indexes are created.
The file must already be available locally. Inputs must not be concurrently modified.
The schema is the existing unversioned `s2orc_v2` shape (`corpusid`, `body.text`).
Reported license strings are evidence only and grant no display/derivation rights.
No body text is returned. This is a sequential metadata lookup, not a text loader.

Ordinary gzip is decoded from the beginning; line numbers are decoded JSONL lines,
not seek offsets. Record bytes include the newline. A bounded read uses at most one
extra decoded byte to detect an oversized line; gzip buffering may decode ahead
internally. Limits bound accepted lines/prefix bytes, not compressed bytes, CPU,
or total process memory. The defaults cap 100 records, 8 MiB per line and 64 MiB
per accepted decoded prefix. Raising flags cannot exceed 10000 records or these
byte ceilings. Overlong, malformed and truncated records fail without body text
in CLI errors. A found prefix does not certify an unread tail or gzip trailer.
At an exact record/decoded limit the scan remains incomplete, even if the file
would end there. Only observed EOF establishes `absent-in-file`.
The SHA256 covers accepted decoded JSONL bytes only, not a whole source object.

Local files have no verified release, generation or completion provenance.
Core archive metadata separately preserves `SourceLineage` and explicitly reports
`completion-pinned-prefix`: whole-shard integrity requires full shard consumption.
Core `absent-in-stream` only describes the supplied stream, which may itself be
bounded by `ConsumeBase`; it never establishes absence from an archive or corpus.
First matching ID wins. Sources are historic bases only; later diffs and deletes
are unsupported, so results must not be presented as latest/current-state results.

Synthetic fixtures establish behavior only. Real corpus size, coverage,
performance, schema breadth and rights remain UNVERIFIED. This example introduces
no production full-text loader, publication engine, or shared archive/vector schema.
