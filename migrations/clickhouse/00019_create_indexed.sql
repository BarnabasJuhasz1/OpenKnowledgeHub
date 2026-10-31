-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS indexed
(
    plan String,
    kind LowCardinality(String),
    part UInt32,
    recorded_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY recorded_at;

-- +goose Down
DROP TABLE IF EXISTS indexed;
