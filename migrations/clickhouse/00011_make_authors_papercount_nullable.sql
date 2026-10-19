-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE authors MODIFY COLUMN papercount Nullable(UInt32) DEFAULT NULL;
-- Clear the guard if a previous rollback stopped before removing its default.
ALTER TABLE authors MODIFY COLUMN papercount REMOVE DEFAULT;

-- +goose Down
-- Refuse before changing metadata if unknown counts exist. The throwing default also
-- prevents a concurrent NULL from being silently replaced during conversion.
SELECT throwIf(countIf(isNull(papercount)) > 0, 'authors.papercount contains NULL; rollback refused')
FROM authors FORMAT Null;
ALTER TABLE authors MODIFY COLUMN papercount UInt32
DEFAULT toUInt32(throwIf(1, 'authors.papercount contains NULL; rollback refused'))
SETTINGS mutations_sync = 2;
ALTER TABLE authors MODIFY COLUMN papercount REMOVE DEFAULT;
