-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE papers
    MODIFY COLUMN url Nullable(String) DEFAULT NULL,
    MODIFY COLUMN title Nullable(String) DEFAULT NULL,
    MODIFY COLUMN venue Nullable(String) DEFAULT NULL;
-- Clear guards left by an interrupted rollback.
ALTER TABLE papers MODIFY COLUMN url REMOVE DEFAULT,
    MODIFY COLUMN title REMOVE DEFAULT, MODIFY COLUMN venue REMOVE DEFAULT;
-- Existing rows have a known authors array, including empty arrays.
ALTER TABLE papers ADD COLUMN IF NOT EXISTS authors_is_null Bool AFTER authors;

-- +goose Down
-- Pause writers before rollback: checking the authors flag and dropping it are separate.
-- Recreate the flag after a completed rollback so rerunning Down remains safe.
ALTER TABLE papers ADD COLUMN IF NOT EXISTS authors_is_null Bool AFTER authors;
SELECT throwIf(countIf(isNull(url) OR isNull(title) OR isNull(venue) OR authors_is_null) > 0,
    'papers metadata contains NULL; rollback refused') FROM papers FORMAT Null;
-- Throwing defaults prevent scalar NULL values from silently becoming empty strings.
ALTER TABLE papers
    MODIFY COLUMN url String DEFAULT toString(throwIf(1, 'papers metadata contains NULL; rollback refused')),
    MODIFY COLUMN title String DEFAULT toString(throwIf(1, 'papers metadata contains NULL; rollback refused')),
    MODIFY COLUMN venue String DEFAULT toString(throwIf(1, 'papers metadata contains NULL; rollback refused'))
SETTINGS mutations_sync = 2;
ALTER TABLE papers MODIFY COLUMN url REMOVE DEFAULT,
    MODIFY COLUMN title REMOVE DEFAULT, MODIFY COLUMN venue REMOVE DEFAULT;
ALTER TABLE papers DROP COLUMN IF EXISTS authors_is_null;
