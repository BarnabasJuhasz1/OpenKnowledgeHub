# Project setup

This is the working agreement for the OpenKnowledge repository, decided at the first team meeting on 2 October 2026 and set up by Emmanuel Oyelana. Read `CONTRIBUTING.md` for the day to day rules. This page explains the board, the labels, the milestones and the review rule.

## Platforms

| Purpose | Where |
| --- | --- |
| Code, issues, pull requests, board | GitHub, this repository |
| Documents and sheets | Google Drive, folder OpenKnowledge internal, Workspace openknowledgehub.app |
| Chat | Team WhatsApp group |
| Cloud | GCP project owned by Barnabas, Google for Startups credits through the ETH lab, provisioned with OpenTofu from a separate private repository (issue #1) |
| Meeting | Fridays, in person, room booked weekly (one person can book four hours, rotate who books) |

GitLab is not in use. If the supervisor wants a UZH GitLab copy later, it will be a mirror of this repository, nothing else changes.

## Labels

Scope labels, one per issue: `data`, `models`, `clustering`, `evaluation`, `frontend`, `infra`, `report`, `docs`.

Type labels: `feature`, `bug`.

## Milestones

| Milestone | Due | From the proposal timeline |
| --- | --- | --- |
| M1 October | 31 Oct 2026 | Repository and tooling, prototype introduced to all, corpus and preprocessing pipeline unified, review of clustering and literature mapping work, evaluation protocol and user study design drafted |
| M2 November | 30 Nov 2026 | Annotation of citation contexts and paper archetypes, archetype classifier retrained with sub classes and multi label output, clustering procedure started |
| M3 December | 31 Dec 2026 | Clustering procedure, alpha deployed, pilot with a small group, baselines, offline benchmarking, relevance score finalised and ablated |
| M4 January | 31 Jan 2027 | Citation link model trained or distilled, second pilot, beta deployed |
| M5 February | 28 Feb 2027 | Iteration on second pilot feedback, supplementary features |
| M6 March | 15 Mar 2027 | Analysis of study data, report, archive of code and data, submission |

Tags at the deployments: `v0.1` for the December alpha, `v0.2` for the January beta, `v1.0` at submission.

## Board

One GitHub Projects board named OpenKnowledge, Board layout, columns `Backlog`, `This week`, `In progress`, `In review`, `Done`.

Workflows switched on in the project settings: item added to project goes to Backlog, pull request merged goes to Done, item closed goes to Done.

Rule: everyone moves their own cards before the Friday meeting, the meeting runs off the board.

## Review rule

`main` is protected by a ruleset. Pull request required, one approval, the CI status checks `backend` and `frontend` must pass, no force pushes. Squash merge is the only merge method and merged branches are deleted automatically. `CODEOWNERS` requests the scope owner as reviewer.

## How a feature or bug gets in

1. Open an issue with the Task or Bug template. Fill Goal, Done when, scope label, milestone, owner.
2. It lands in Backlog. Move it to This week when you commit to it.
3. Branch `scope/short_name`, work, open a pull request that says `Closes #number`.
4. Scope owner reviews, CI passes, squash merge. The issue closes and the card moves to Done on its own.

## AI tools

Declared per component in `docs/ai_usage.md` and per pull request in the template. This follows the ETH lab's request that we say where AI was used. Level of detail is component and purpose.

## Apply this setup

1. `gh auth login`, then edit `REPO=` at the top of `scripts/setup_github.sh` and run it once. It creates labels, milestones and the first issues.
2. In the browser: Projects, New project, Board, name OpenKnowledge, rename columns, switch on the three workflows, link the repository.
3. Settings, General: allow only squash merging, tick automatically delete head branches.
4. Settings, Rules, Rulesets, New branch ruleset for `main`: require pull request with one approval, require status checks `backend` and `frontend`, block force pushes.
5. Replace the placeholder handles in `.github/CODEOWNERS`.
6. Open the setup as one pull request from `docs/repo_setup` and ask Barnabas for the review.
