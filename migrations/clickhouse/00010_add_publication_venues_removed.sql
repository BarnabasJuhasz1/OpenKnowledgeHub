-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE publication_venues ADD COLUMN IF NOT EXISTS removed Bool DEFAULT false;

-- +goose Down
ALTER TABLE publication_venues DROP COLUMN IF EXISTS removed;
