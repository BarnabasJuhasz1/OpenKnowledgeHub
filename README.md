# OpenKnowledge

Hierarchical clustering of scientific literature. A free, open source, AI powered tool that helps researchers understand how a body of literature is structured, which lines of work descend from which, and which papers matter most for their own work.

Master project, University of Zurich, supervised by Prof. Dr. Claudio J. Tessone (BDLT Group, UZH), external supervision by the ETH Agentic Systems Lab. Runs from 1 October 2026 to the middle of March 2027.

## Team and scopes

| Scope | Responsible |
| --- | --- |
| Data and infrastructure (corpus ingestion, preprocessing, search backend, deployment, repository, documentation) | Emmanuel Oyelana, Felix Wegener |
| Models (citation link model, archetype taxonomy and classifier, cluster summarisation) | Felix Wegener, Emmanuel Oyelana |
| Clustering (clustering procedure, baselines, offline benchmarking, relevance score and ablation) | Barnabas Juhasz, Basak Ocak |
| Evaluation (user study design, recruitment, data collection, statistical analysis) | Basak Ocak, Lucia Ivan |
| Frontend (frontend development, UI and UX, interaction design) | Lucia Ivan, Barnabas Juhasz |
| Reporting (report, presentation, archiving of code and data) | All members |

## Repository layout

```
backend/      Python, FastAPI. Search API, graph construction, scoring.
frontend/     Web client. Rebuilt from scratch for this project.
data/         Ingestion and preprocessing pipelines. Semantic Scholar bulk and incremental loads, deduplication, normalisation.
models/       Citation link model, archetype classifier, cluster summariser. Training, distillation, evaluation.
clustering/   Clustering procedure, baselines, benchmark harness.
evaluation/   User study instruments, reference sets, analysis scripts.
infra/        Pointer to the private infrastructure repository (OpenTofu for GCP). No cloud code lives here.
docs/         Project documentation. Start with docs/project_setup.md.
scripts/      One off helpers, including the GitHub setup script.
```

No datasets, model weights or secrets live in this repository. Data sits in GCS and BigQuery, models in a GCS bucket, secrets in GitHub Actions secrets and GCP Secret Manager. Copy `.env.example` to `.env` for local work.

## Setup

To be written with the first working backend. Must cover local setup, reproduction of every preprocessing step, and a self hosting guide, as required by the project proposal.

## Data attribution

The corpus is built from the Semantic Scholar Academic Graph, released under the ODC-BY 1.0 license. See `docs/data_sources.md` for every source in use and its license.

Kinney, R. et al. (2023). The Semantic Scholar Open Data Platform. arXiv:2301.10140.

## Transparency

The processing methodology and the exact role of every AI model, classifier and graph construction step are documented in `docs/` so that any result can be traced back to how it was produced. Use of AI coding tools in this repository is declared in `docs/ai_usage.md`.

## Lineage

This repository starts from a clean slate. The August 2026 prototype lives in the archived repository BarnabasJuhasz1/OpenKnowledge. What was carried over, and what was rewritten, is recorded in `docs/mvp_lineage.md`.

## License

MIT, see `LICENSE`.
