-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS publication_venues
(
    id String,
    name String,
    `type` Nullable(String),
    issn Nullable(String),
    alternate_issns Array(String),
    alternate_names Array(String),
    url Nullable(String),
    alternate_urls Array(String),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id;

-- +goose Down
DROP TABLE IF EXISTS publication_venues;
