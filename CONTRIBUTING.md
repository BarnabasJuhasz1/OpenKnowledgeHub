# How we work

## Where things live

- Tasks, features and bugs: GitHub issues on this repository, tracked on the OpenKnowledge project board.
- Documents, sheets, user study signups, competitor analysis, idea board: the OpenKnowledge internal folder on the team Google Drive (Workspace at openknowledgehub.app).
- Chat: the team WhatsApp group.
- Weekly meeting: Fridays, in person. Cards get moved on the board before the meeting, the meeting runs off the board.

## Filing work

- Feature or task: open an issue with the Task template. Every issue carries one scope label, one milestone and an owner. An issue without a "Done when" line is not ready to start.
- Bug: open an issue with the Bug template.
- Ideas that are not yet tasks go to the idea board in Drive, not to GitHub.

## Branches and pull requests

- One branch per issue, named `scope/short_name`, for example `data/s2_inventory` or `models/citation_link_labels`.
- Commits are short and written in the imperative, for example `Add S2 incremental diff loader`.
- Every change reaches `main` through a pull request. `main` is protected, no direct pushes, no force pushes.
- The pull request template asks for what changed, how it was tested, which issue it closes (`Closes #12`) and whether AI tools were used.
- One approval from the scope owner (CODEOWNERS requests them automatically), CI green, then squash merge. Branches are deleted after merge.

## Code

- Backend is Python with FastAPI. Lint with ruff, test with pytest. CI runs both on every pull request.
- Frontend builds in CI with `npm ci` and `npm run build`.
- Infrastructure is OpenTofu in the separate private infrastructure repository (see `infra/README.md`). Changes to cloud resources go through a pull request there like everything else.
- No data, weights or secrets in git. `.gitignore` already covers the usual paths. `.env.example` lists every variable the code reads.

## AI tools

The ETH lab asked that we declare where AI tools were used. We do that per component in `docs/ai_usage.md` and per pull request in the pull request template. Component and purpose, not prompt by prompt.

## Reuse from the prototype

Anything taken from the archived prototype repository is recorded in `docs/mvp_lineage.md` with the original path, the decision (reuse, rewrite, drop) and the new location.
