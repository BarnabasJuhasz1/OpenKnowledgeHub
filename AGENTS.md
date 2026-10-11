# AGENTS.md

Rules for this repo. CI enforces those it can check.

- Imports point one way: `pipelines.cli` > `pipelines.bootstrap` > `pipelines.flows`, `pipelines.settings`, `adapters` > `core` > `contracts`. Only `bootstrap` imports adapters.
- Core imports no framework, client or adapter. Every port is an `async def` method of a Protocol in `core/<feature>/ports.py`; pure transforms are not ports.
- Vendor objects stay inside adapters, and an adapter subpackage imports only its own extra. Clients are built per run by bootstrap from `PipelineSettings`, never cached globally.
- Settings, commands, results, receipts and metadata are Pydantic models; public functions are typed for pyright strict.
- Checks: `uv run pytest -q`, `uv run ruff check . && uv run ruff format --check .`, `uv run pyright`, `uv run lint-imports`.
