-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE authors MODIFY COLUMN papercount Nullable(UInt32);

-- +goose Down
-- ClickHouse refuses this conversion while NULL counts exist, preserving unknown values.
ALTER TABLE authors MODIFY COLUMN papercount UInt32;
