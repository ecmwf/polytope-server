# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# pyfdb and pygribjump[binary] bundle their own C++ stack (fdb5lib, gribjumplib,
# eccodeslib, metkitlib, eckitlib) via PyPI manylinux wheels — no separate
# pre-built ECCR image required.

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/lukemathwalker/cargo-chef:0.1.77-rust-1.94.1-slim-bookworm@sha256:4787c365155bfff657a58c89e6ce05b99e60d343ee57fd4a0fdcbb2547a8e017 AS chef-base
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*

###############################
# Prepare the root workspace Cargo Chef recipe
###############################
FROM chef-base AS chef
WORKDIR /ecmwf/polytope-server
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && cargo chef prepare --recipe-path recipe.json

###############################
# Cache root workspace dependencies without application source
###############################
FROM chef-base AS cacher
ARG PACKAGE_NAME=polytope-fe-worker
WORKDIR /ecmwf/polytope-server
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
# cargo-chef omits excluded path dependencies from root workspace recipes.
COPY utils/metkit/Cargo.toml utils/metkit/Cargo.toml
RUN mkdir -p utils/metkit/src && touch utils/metkit/src/lib.rs \
    && printf 'fn main() {}\n' > utils/metkit/build.rs
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && cargo chef cook --release --locked --package "${PACKAGE_NAME}" --recipe-path recipe.json

###############################
# Build the package from the root workspace
###############################
FROM chef-base AS builder
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
WORKDIR /ecmwf/polytope-server
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && cargo build --release --locked --package "${PACKAGE_NAME}"

###############################
# Python venv
###############################
FROM python:3.11-slim-bookworm AS python-deps
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir uv
RUN uv venv /opt/venv
COPY workers/polytope-fe-worker/requirements.txt /requirements.txt
RUN uv pip install --python /opt/venv/bin/python -r /requirements.txt

###############################
# Production image
###############################
FROM python:3.11-slim-bookworm AS release
ARG VERSION
ARG REVISION
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server"

# Runtime system libs needed by the bundled C++ stack in fdb5lib/gribjumplib
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=python-deps /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH=/app:/opt/venv/lib/python3.11/site-packages

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
COPY workers/polytope-fe-worker/dynamic_grid/ /app/dynamic_grid/
ENTRYPOINT ["/app/polytope-fe-worker"]

###############################
# Debug image (adds bash for exec access)
###############################
FROM python:3.11-slim-bookworm AS debug
ARG VERSION
ARG REVISION
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 bash \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=python-deps /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH=/app:/opt/venv/lib/python3.11/site-packages

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
COPY workers/polytope-fe-worker/dynamic_grid/ /app/dynamic_grid/
ENTRYPOINT ["/app/polytope-fe-worker"]
