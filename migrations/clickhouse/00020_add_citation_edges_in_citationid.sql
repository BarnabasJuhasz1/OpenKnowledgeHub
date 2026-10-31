-- +goose NO TRANSACTION
-- +goose Up
-- Existing rows read 0 until the next rebuild.
ALTER TABLE citation_edges_in ADD COLUMN IF NOT EXISTS citationid UInt64 CODEC(ZSTD(1));

-- +goose Down
ALTER TABLE citation_edges_in DROP COLUMN IF EXISTS citationid;
