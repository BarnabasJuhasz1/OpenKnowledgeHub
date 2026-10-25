-- +goose NO TRANSACTION
-- +goose Up
-- Template only: jobs clone private attempts; readers use explicitly published tables.
CREATE TABLE IF NOT EXISTS paper_embeddings_template
(
    corpusid UInt64,
    sequence UInt64,
    provenance String,
    is_deleted Bool,
    vector Array(Float32) CODEC(ZSTD(1)),
    CONSTRAINT valid_vector CHECK
        (is_deleted AND empty(vector)) OR
        (NOT is_deleted AND length(vector) = 768
         AND arrayAll(x -> isFinite(x), vector)
         AND arrayExists(x -> x != 0, vector)),
    CONSTRAINT valid_provenance CHECK notEmpty(provenance)
)
ENGINE = MergeTree
ORDER BY corpusid
SETTINGS index_granularity = 256;

-- +goose Down
-- Published and abandoned attempt tables require explicit operator removal.
DROP TABLE IF EXISTS paper_embeddings_template;
