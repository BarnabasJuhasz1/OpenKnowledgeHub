-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS papers
(
    corpusid UInt64,
    externalids Map(String, Nullable(String)),
    url String,
    title String,
    authors Array(Tuple(authorId Nullable(String), name String)),
    venue String,
    publicationvenueid Nullable(String),
    year Nullable(Int16),
    referencecount UInt32,
    citationcount UInt32,
    influentialcitationcount UInt32,
    isopenaccess Bool,
    s2fieldsofstudy Array(Tuple(category String, source String)),
    s2fieldsofstudy_is_null Bool,
    publicationtypes Array(String),
    publicationtypes_is_null Bool,
    publicationdate Nullable(String),
    journal Tuple(name String, pages Nullable(String), volume Nullable(String)),
    journal_is_null Bool,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY corpusid;

-- +goose Down
DROP TABLE IF EXISTS papers;
