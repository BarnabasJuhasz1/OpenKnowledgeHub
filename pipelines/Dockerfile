# The pipelines image: okp for Batch download tasks and loads, prefect for the worker.
# Build it from the repository root: docker build -f pipelines/Dockerfile .

ARG PYTHON_IMAGE=python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1

FROM ghcr.io/astral-sh/uv:0.13.0@sha256:cdc6093146eb3ff6a40107b38f008b789e050e77ad87865e381d9917da55a168 AS uv

FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependencies first, so a change to the sources does not reinstall them. --locked fails the
# build when uv.lock no longer matches the pyproject files.
COPY pyproject.toml uv.lock .python-version ./
COPY packages/contracts/pyproject.toml packages/contracts/
COPY packages/core/pyproject.toml packages/core/
COPY packages/adapters/pyproject.toml packages/adapters/
COPY pipelines/pyproject.toml pipelines/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-workspace --package openknowledge-pipelines

COPY packages/contracts/src packages/contracts/src
COPY packages/core/src packages/core/src
COPY packages/adapters/src packages/adapters/src
COPY pipelines/src pipelines/src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package openknowledge-pipelines

FROM ${PYTHON_IMAGE}
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/OpenKnowledgeOrg/OpenKnowledge" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.title="openknowledge-pipelines"
RUN useradd --create-home --uid 10001 okp
# Only the virtual environment: the workspace packages are installed into it, not linked.
COPY --from=builder /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1
USER 10001
WORKDIR /home/okp
ENTRYPOINT ["okp"]
