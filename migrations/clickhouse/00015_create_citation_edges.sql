-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS citation_edges_out
(
    citing UInt64 CODEC(Delta, ZSTD(1)),
    cited UInt64 CODEC(ZSTD(1)),
    isinfluential Bool,
    intents UInt8
)
ENGINE = MergeTree
ORDER BY (citing, cited)
SETTINGS index_granularity = 1024;

CREATE TABLE IF NOT EXISTS citation_edges_in
(
    citing UInt64 CODEC(ZSTD(1)),
    cited UInt64 CODEC(Delta, ZSTD(1)),
    isinfluential Bool,
    intents UInt8
)
ENGINE = MergeTree
ORDER BY (cited, citing)
SETTINGS index_granularity = 1024;

-- +goose Down
DROP TABLE IF EXISTS citation_edges_in;
DROP TABLE IF EXISTS citation_edges_out;
