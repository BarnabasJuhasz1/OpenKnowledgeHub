-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS paper_ids
(
    sha String,
    corpusid UInt64,
    `primary` Bool,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY sha;

-- +goose Down
DROP TABLE IF EXISTS paper_ids;
