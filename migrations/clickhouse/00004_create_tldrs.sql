-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS tldrs
(
    corpusid UInt64,
    model String,
    text Nullable(String),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY corpusid;

-- +goose Down
DROP TABLE IF EXISTS tldrs;
