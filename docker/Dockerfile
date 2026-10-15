# TickVault images (linux/arm64).
#
# Targets:
#   ingester      -- tickvault runtime, base dependencies only
#   dashboard     -- ingester + the dashboard dependency group (Streamlit)
#   orchestration -- ingester + the orchestration dependency group (Dagster)
#
# Dependencies are installed with uv from uv.lock; only the requested group is
# added to the base set (`--no-default-groups` drops the dev/orchestration/
# dashboard defaults), and the runtime carries no uv, no build tools and runs
# as an unprivileged user.

FROM --platform=linux/arm64 python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/app/.venv/bin:$PATH
WORKDIR /app

FROM base AS builder
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-default-groups --no-install-project
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-default-groups

FROM builder AS builder-dashboard
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-default-groups --group dashboard

FROM builder AS builder-orchestration
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-default-groups --group orchestration

FROM --platform=linux/arm64 python:3.12-slim AS runtime
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 10001 tickvault \
    && useradd --uid 10001 --gid tickvault --create-home --shell /usr/sbin/nologin tickvault \
    && mkdir --parents /data \
    && chown tickvault:tickvault /data
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/app/.venv/bin:$PATH
WORKDIR /app

FROM runtime AS ingester
COPY --from=builder --chown=tickvault:tickvault /app /app
USER tickvault

FROM runtime AS dashboard
COPY --from=builder-dashboard --chown=tickvault:tickvault /app /app
USER tickvault

FROM runtime AS orchestration
COPY --from=builder-orchestration --chown=tickvault:tickvault /app /app
USER tickvault
