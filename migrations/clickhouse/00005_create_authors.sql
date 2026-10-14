-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS authors
(
    authorid String,
    externalids Nullable(String),
    url String,
    name String,
    aliases Array(String),
    aliases_is_null Bool,
    affiliations Array(String),
    affiliations_is_null Bool,
    homepage Nullable(String),
    papercount UInt32,
    citationcount UInt32,
    hindex UInt32,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY authorid;

-- +goose Down
DROP TABLE IF EXISTS authors;
