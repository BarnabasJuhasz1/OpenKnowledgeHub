-- +goose NO TRANSACTION
-- +goose Up
-- One row per archived object loaded into a table; the latest load of an object wins. A record
-- counts only while target_uuid is the table's current UUID and the table has rows, so a
-- dropped, recreated or truncated table is loaded again.
CREATE TABLE loads
(
    target LowCardinality(String),
    target_uuid UUID,
    source_object String,
    generation UInt64,
    sha256 String,
    source_release Date,
    operation Enum8('upsert' = 0, 'delete' = 1),
    records UInt64,
    written_rows UInt64,
    load_sql_sha256 String,
    software_version String,
    image_digest Nullable(String),
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (target, source_object);

-- +goose Down
DROP TABLE loads;
