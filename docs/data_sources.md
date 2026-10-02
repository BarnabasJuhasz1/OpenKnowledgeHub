# Data sources

Everything the platform shows is built from the data. This file lists every source in use or under evaluation, its license, what it contributes and how it is refreshed. Legal questions are noted here and raised with the supervisor before a source goes live.

## Rule on PDFs and full text

Processing a text under its license is one thing, displaying the PDF is another. Open access text with a permissive license can be processed and shown. Everything else is processed only where the license allows it and the user is sent to the source to read the paper. Closed access PDFs are never downloaded, stored or displayed.

## In use

| Source | What it gives | License | Refresh | Status |
| --- | --- | --- | --- | --- |
| Semantic Scholar Academic Graph, Datasets API | papers (metadata, over 200 million), abstracts, citations with citation context, intent and influential flags, authors, venues, tldrs, SPECTER embeddings, paper id mapping | ODC-BY 1.0, attribution required | Releases roughly weekly, incremental diffs between releases | Loaded into BigQuery by the prototype, inventory in progress |
| S2ORC (same API) | Parsed full text of open access papers, over 12 million, with sections, paragraphs, bibliography entries and inline citation spans linked to the cited corpus id | ODC-BY 1.0 | Same releases | To load for working corpora only |

## Under evaluation

| Source | What it would add | License | Open question |
| --- | --- | --- | --- |
| OpenAlex | Second metadata and citation graph, open access locations, concepts | CC0 | Merge and deduplication rules against Semantic Scholar ids |
| Crossref | DOI metadata, reference lists | Open metadata | Rate limits and polite pool |
| arXiv | Full text for preprints, bulk access | Per paper license, bulk via S3 or Kaggle metadata | Only papers with a license that allows processing |
| Unpaywall | Open access locations and licenses per DOI | Verify snapshot license | Used to find legal full text, not as a content source |
| PubMed Central open access subset | Full text for biomedical papers | Per paper license, CC subset | Field coverage beyond biomedicine |

## Freshness

Target is that a new paper appears on the platform within 24 hours of its release in the source. Semantic Scholar incremental diffs set the floor for that source. One scheduled job applies the diff, nothing is re exported.

## Merging sources

Identifier precedence for deduplication: Semantic Scholar corpus id, then DOI, then arXiv id. Conflicts between sources (same identifier, different abstract or metadata) are logged and resolved by a written rule per field, not by whichever load ran last.
