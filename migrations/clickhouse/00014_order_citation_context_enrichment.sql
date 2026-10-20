-- +goose NO TRANSACTION
-- +goose Up
-- Equal-version legacy merges may have discarded contexts. Do not bless those rows or their
-- checkpoints: preserve the legacy table, create an empty replacement (new UUID), then replay
-- the complete immutable archive. Rerunning after successful migration is harmless.
--
-- A populated legacy table makes this migration fail until it is swapped by hand:
--   1. Stop every citations load and diff (pause their schedules; no run may be in flight).
--   2. RENAME TABLE citations TO citations_legacy;
--   3. Run the Up statement of 00007_create_citations.sql. The new table has a new UUID, so
--      the legacy checkpoints in loads and applied no longer count. Do not delete them.
--   4. Apply the migrations, then load the base release again from all of its raw objects.
--   5. Drop citations_legacy only after that load finishes and passes its checks.
SELECT throwIf(
    ((SELECT count() FROM citations) > 0 OR
     (SELECT count() FROM loads WHERE target = 'citations' AND target_uuid IN
         (SELECT uuid FROM system.tables
          WHERE database = currentDatabase() AND name = 'citations')) > 0) AND
    (SELECT default_expression FROM system.columns
     WHERE database = currentDatabase() AND table = 'citations' AND name = 'version') !=
        '((toUInt64(source_release) * 4) + (toUInt64(is_deleted) * 2)) + toUInt64(NOT contexts_is_null)',
    'citations: swap the populated legacy table by hand first, as 00014 describes'
) FORMAT Null;
ALTER TABLE citations MODIFY COLUMN version UInt64 MATERIALIZED
    ((toUInt64(source_release) * 4) + (toUInt64(is_deleted) * 2)) + toUInt64(NOT contexts_is_null);

-- +goose Down
-- Mixing the version scales is unsafe in either direction.
SELECT throwIf(
    ((SELECT count() FROM citations) > 0 OR
     (SELECT count() FROM loads WHERE target = 'citations' AND target_uuid IN
         (SELECT uuid FROM system.tables
          WHERE database = currentDatabase() AND name = 'citations')) > 0) AND
    (SELECT default_expression FROM system.columns
     WHERE database = currentDatabase() AND table = 'citations' AND name = 'version') !=
        '(toUInt64(source_release) * 2) + is_deleted',
    'citations: rollback requires an empty replacement with a new UUID and a full raw replay'
) FORMAT Null;
ALTER TABLE citations MODIFY COLUMN version UInt64 MATERIALIZED
    (toUInt64(source_release) * 2) + is_deleted;
