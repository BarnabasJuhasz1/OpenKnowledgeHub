-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS citations
(
    citationid UInt64,
    citingcorpusid UInt64,
    citedcorpusid Nullable(UInt64),
    isinfluential Bool,
    contexts Array(String),
    contexts_is_null Bool,
    intents Array(Array(String)),
    intents_is_null Bool,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY citationid;

-- +goose Down
DROP TABLE IF EXISTS citations;
