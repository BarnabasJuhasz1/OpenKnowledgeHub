#!/usr/bin/env bash
# One time GitHub setup for OpenKnowledge: labels, milestones, first issues.
# Infrastructure work is not created here, it lives in issue #1 (OpenTofu) and the private infra repository.
# Needs the GitHub CLI logged in (gh auth login) with access to the repository.
# Edit REPO before running. Safe to rerun, existing labels and milestones are skipped.
set -euo pipefail

REPO="BarnabasJuhasz1/OpenKnowledgeHub"

label() {
  gh label create "$1" --color "$2" --description "$3" --repo "$REPO" 2>/dev/null || echo "label $1 exists"
}

milestone() {
  if gh api "repos/$REPO/milestones?state=all" --jq '.[].title' | grep -qx "$1"; then
    echo "milestone $1 exists"
  else
    gh api -X POST "repos/$REPO/milestones" -f title="$1" -f due_on="$2" -f description="$3" >/dev/null
    echo "milestone $1 created"
  fi
}

issue() {
  # title, labels (comma separated), milestone, body, assignee (may be empty)
  local args=(--repo "$REPO" --title "$1" --label "$2" --milestone "$3" --body "$4")
  if [ -n "${5:-}" ]; then args+=(--assignee "$5"); fi
  gh issue create "${args[@]}"
}

echo "== labels"
label data        1D76DB "Data and infrastructure"
label models      5319E7 "Citation link model, archetype classifier, summariser"
label clustering  0E8A16 "Clustering procedure and baselines"
label evaluation  D93F0B "User study and benchmarks"
label frontend    C5DEF5 "Frontend, UI and UX"
label infra       0052CC "Cloud, OpenTofu, deployment"
label report      FBCA04 "Report, presentation, archive"
label docs        BFD4F2 "Documentation"
label feature     A2EEEF "New work with a clear end state"
label bug         B60205 "Something is broken"

echo "== milestones"
milestone "M1 October"  "2026-10-31T23:59:59Z" "Repository and tooling, prototype introduced, pipeline unified, evaluation protocol drafted"
milestone "M2 November" "2026-11-30T23:59:59Z" "Annotation, archetype classifier retrained, clustering started"
milestone "M3 December" "2026-12-31T23:59:59Z" "Clustering procedure, alpha deployed, pilot, baselines, relevance score ablation"
milestone "M4 January"  "2027-01-31T23:59:59Z" "Citation link model, second pilot, beta deployed"
milestone "M5 February" "2027-02-28T23:59:59Z" "Iteration on second pilot, supplementary features"
milestone "M6 March"    "2027-03-15T23:59:59Z" "Analysis, report, archive, submission"

echo "== issues"
ME="eoyelana"

issue "Repository setup: templates, CI, board, branch rules" "docs,feature" "M1 October" \
"Goal. Working agreement in place so everyone files and ships work the same way.

Done when
- [ ] Labels and milestones created
- [ ] Board OpenKnowledge with Backlog, This week, In progress, In review, Done
- [ ] Ruleset on main, squash merge only, auto delete branches
- [ ] CODEOWNERS filled with real handles
- [ ] docs/project_setup.md posted in the group" "$ME"

issue "Inventory of the Semantic Scholar bulk export" "data,feature" "M1 October" \
"Goal. Know exactly what is in the corpus and what is missing before any pipeline work.

Done when
- [ ] Every dataset of the Datasets API listed with record counts as loaded in BigQuery (papers, abstracts, citations, authors, venues, tldrs, embeddings, paper ids, S2ORC)
- [ ] Field level inventory for papers, abstracts and citations, including citation context and intent coverage
- [ ] Gaps written down in docs/data_sources.md" "$ME"

issue "Legal path to full text" "data,feature" "M1 October" \
"Goal. Decide where full text may come from and what may be displayed.

Done when
- [ ] S2ORC coverage checked for a sample working corpus
- [ ] Rule for process versus display written in docs/data_sources.md
- [ ] Open access fallbacks (Unpaywall, arXiv, PMC OA) assessed with their licenses
- [ ] Questions for legal advice listed, if any remain" "$ME"

issue "Design the continuous ingestion pipeline" "data,feature" "M1 October" \
"Goal. New papers appear within 24 hours of release.

Done when
- [ ] Semantic Scholar incremental diffs understood and tested on one release pair
- [ ] One scheduled job applies a diff into BigQuery and refreshes the search index
- [ ] Deduplication and conflict rules per field written in docs/data_sources.md" "$ME"

issue "Evaluate additional data sources and merge strategy" "data,feature" "M1 October" \
"Goal. Know which sources beyond Semantic Scholar are worth adding and how they merge.

Done when
- [ ] OpenAlex, Crossref, arXiv, Unpaywall, PMC OA compared on coverage, license and refresh
- [ ] Identifier precedence and merge rules proposed
- [ ] Recommendation in docs/data_sources.md"

issue "Search engine evaluation: OpenSearch, Elasticsearch, Meilisearch" "data,feature" "M1 October" \
"Goal. Sub second query over the full corpus, with full text search where text exists.

Done when
- [ ] Current OpenSearch setup measured on latency for typical queries
- [ ] Elasticsearch and Meilisearch compared on latency, cost and operations
- [ ] Decision recorded in docs/adr/"

issue "Review the prototype backend and record what is reused" "data,docs" "M1 October" \
"Goal. Keep what holds up, rewrite the rest, and be transparent about it.

Done when
- [ ] Each component of the archived repository has a row in docs/mvp_lineage.md with a decision
- [ ] Reused code copied into the new structure through pull requests"

issue "Prototype walkthrough session" "docs" "M1 October" \
"Goal. Everyone starts from the same picture of what exists.

Done when
- [ ] Session held, notes in the Drive folder
- [ ] Open questions turned into issues"

issue "Annotation pipeline for citation contexts and archetypes" "models,feature" "M2 November" \
"Goal. Labelled sets for the citation link model and the archetype classifier, produced with LLM pre labelling and human checks.

Done when
- [ ] Label schema for citation type and strength and for archetypes with sub classes
- [ ] LLM pre labelling run on a first sample
- [ ] Human check on a sample with inter annotator agreement reported
- [ ] Labels versioned in GCS"

issue "Evaluation harness for the classifiers" "models,feature" "M2 November" \
"Goal. One script, same metrics table every run.

Done when
- [ ] Fixed train, validation and test split
- [ ] Per class precision, recall and F1 reported
- [ ] Runs reproducible from a config file"

issue "Register at the ETH Student Project House" "docs" "M1 October" \
"Goal. A dedicated space for the team.

Done when
- [ ] Registration submitted
- [ ] Outcome posted in the group"

issue "Room booking rota for the Friday meeting" "docs" "M1 October" \
"Goal. The room is booked every week.

Done when
- [ ] Rota agreed, one person books per week, four hour limit per person respected"

issue "Keep Prof. Tessone in the loop" "report" "M3 December" \
"Goal. The supervisor sees progress before the final presentation.

Done when
- [ ] Check in scheduled around the alpha
- [ ] BDLT lab visited during the pilot phase"

issue "User study signups and outreach list" "evaluation,feature" "M1 October" \
"Goal. The participant list grows toward 50 to 100 researchers.

Done when
- [ ] Sheet in the Drive folder has all contacts from Zurich, Rome and Budapest
- [ ] One outreach message agreed and sent"

issue "Competitor analysis" "docs" "M1 October" \
"Goal. Know what Connected Papers and the others do and where OpenKnowledge is different.

Done when
- [ ] Sheet in the Drive folder filled for every tool found
- [ ] Missing features and our position summarised in docs/"

echo "done"
