-- +goose NO TRANSACTION
-- +goose Up
-- One row per base release or diff that a table took in and that passed its checks. The
-- latest row for a table tells which release it holds, as long as its UUID is the table's.
CREATE TABLE IF NOT EXISTS applied
(
    target LowCardinality(String),
    target_uuid UUID,
    release Date,
    from_release Nullable(Date),
    software_version String,
    image_digest Nullable(String),
    applied_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (target, applied_at);

-- +goose Down
DROP TABLE IF EXISTS applied;
