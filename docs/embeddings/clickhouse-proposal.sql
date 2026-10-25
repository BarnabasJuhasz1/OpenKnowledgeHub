-- PROPOSAL ONLY: outside migrations, production unwired. No ANN index.
-- Preserve replay evidence until validation; do not let merges hide conflicts.
CREATE TABLE paper_embeddings_staging
(
    snapshot String,
    dataset LowCardinality(String),
    release Date,
    model LowCardinality(String),
    model_revision String,
    dimension UInt16 DEFAULT 768,
    dtype LowCardinality(String) DEFAULT 'float32',
    corpusid UInt64,
    sequence UInt64,
    provenance String,
    is_deleted UInt8,
    vector Array(Float32),
    CONSTRAINT valid_identity CHECK
        match(snapshot, '^sha256:[0-9a-f]{64}$')
        AND dataset = 'embeddings-specter_v2' AND model = 'specter_v2'
        AND notEmpty(model_revision) AND dimension = 768 AND dtype = 'float32',
    CONSTRAINT valid_deleted CHECK is_deleted IN (0, 1),
    CONSTRAINT valid_vector CHECK
        (is_deleted = 1 AND empty(vector)) OR
        (is_deleted = 0 AND length(vector) = 768
         AND arrayAll(x -> isFinite(x), vector)
         AND arrayExists(x -> x != 0, vector))
)
ENGINE = MergeTree
ORDER BY (snapshot, corpusid, sequence)
SETTINGS index_granularity = 256;

-- Validation templates: both must return no rows before publication.
-- Caller compares the single metadata tuple to its immutable manifest as well.
SELECT snapshot FROM paper_embeddings_staging
GROUP BY snapshot
HAVING uniqExact(tuple(dataset, release, model, model_revision, dimension, dtype)) != 1;

SELECT snapshot, corpusid, sequence FROM paper_embeddings_staging
GROUP BY snapshot, corpusid, sequence
HAVING uniqExact(tuple(provenance, is_deleted, vector)) != 1;

-- Query template: published, validated snapshots only. At most 3000 distinct ids.
-- Resolve the complete tuple BEFORE filtering tombstones; independent of merges.
SELECT corpusid, winner.1 AS sequence, winner.2 AS provenance, winner.4 AS vector
FROM
(
    SELECT corpusid, argMax(tuple(sequence, provenance, is_deleted, vector), sequence) AS winner
    FROM paper_embeddings_staging
    WHERE snapshot = {snapshot:String} AND corpusid IN {ids:Array(UInt64)}
    GROUP BY corpusid
)
WHERE winner.3 = 0;
