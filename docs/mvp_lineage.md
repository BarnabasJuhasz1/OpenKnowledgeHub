# Lineage from the prototype

The August 2026 prototype lives in the archived repository BarnabasJuhasz1/OpenKnowledge. This repository started empty on 2 October 2026 so that the structure could be rebuilt cleanly. Everything carried over is listed here, so that the history stays traceable and the project report can state it plainly.

Decision per item: reuse (copied with changes), rewrite (same idea, new code), drop.

| Prototype component | Original path | Decision | New location | Who | Date |
| --- | --- | --- | --- | --- | --- |
| Frontend | frontend/ | drop, rebuilt from scratch | frontend/ | Lucia, Barnabas | |
| OpenSearch ingest bundle (BigQuery papers_search export into OpenSearch, BM25) | deploy/opensearch-ingest/ | review for reuse | data/ | Emmanuel, Felix | |
| Backend search API | backend/ | review, keep what holds up | backend/ | Emmanuel, Felix | |
| Composite relevance score, first version | backend/ | reuse the logic, make weights explicit and configurable | backend/ | Barnabas, Basak | |
| Archetype classifier, eight classes | Cloud Run service | retrain, multi label with sub classes | models/ | Felix, Emmanuel | |
| Cluster summariser (quantised language model) | Cloud Run service | review | models/ | Felix, Emmanuel | |
| Hierarchical graph view with saved snapshots | frontend/ | rewrite in the new frontend | frontend/ | Lucia, Barnabas | |
| Plans and notes | plans/ | read, not copied | docs/ | all | |
