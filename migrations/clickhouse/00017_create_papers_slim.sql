-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS papers_slim
(
    corpusid UInt64 CODEC(Delta, ZSTD(1)),
    title Nullable(String),
    year Nullable(Int16),
    venue Nullable(String),
    citationcount UInt32,
    influentialcitationcount UInt32,
    referencecount UInt32
)
ENGINE = MergeTree
ORDER BY corpusid
SETTINGS index_granularity = 1024;

-- +goose Down
DROP TABLE IF EXISTS papers_slim;
