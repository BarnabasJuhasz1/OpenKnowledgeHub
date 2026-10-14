-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS abstracts
(
    corpusid UInt64,
    openaccessinfo Tuple(
        disclaimer Nullable(String),
        externalids Map(String, Nullable(String)),
        license Nullable(String),
        url Nullable(String),
        status Nullable(String)
    ),
    abstract String,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY corpusid;

-- +goose Down
DROP TABLE IF EXISTS abstracts;
