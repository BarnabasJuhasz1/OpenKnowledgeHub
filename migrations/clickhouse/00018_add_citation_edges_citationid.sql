-- +goose NO TRANSACTION
-- +goose Up
-- Existing rows read 0 until the next rebuild.
ALTER TABLE citation_edges_out ADD COLUMN IF NOT EXISTS citationid UInt64 CODEC(ZSTD(1));

-- +goose Down
ALTER TABLE citation_edges_out DROP COLUMN IF EXISTS citationid;
