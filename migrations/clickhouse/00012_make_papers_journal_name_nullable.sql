-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE papers MODIFY COLUMN journal
Tuple(name Nullable(String), pages Nullable(String), volume Nullable(String))
SETTINGS mutations_sync = 2;

-- +goose Down
-- Pause writers before rollback: the check and ALTER are separate statements.
-- Include null whole-journal placeholders; narrowing must preserve every stored value.
SELECT throwIf(countIf(isNull(journal.name)) > 0, 'papers.journal.name contains NULL; rollback refused')
FROM papers FORMAT Null;
-- Tuple conversion rejects NULL natively, unlike scalar conversion with a default.
ALTER TABLE papers MODIFY COLUMN journal
Tuple(name String, pages Nullable(String), volume Nullable(String))
SETTINGS mutations_sync = 2;
