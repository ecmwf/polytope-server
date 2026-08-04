# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# Pre-built C++ FDB + gribjump stack (eckit, libaec, eccodes, metkit, fdb,
# gribjump) pulled from ECCR.
# Built and published from docker/cpp-libs/fdb-gribjump by the cpp-libs workflow.
# The stripped image feeds `release`; the -debug (full-symbol) image feeds `debug`.
ARG GJ_LIBS_IMAGE=eccr.ecmwf.int/polytope/cpp-fdb-gribjump-libs:fdb5.21.3-gribjump0.12.0-r1
ARG GJ_LIBS_DEBUG_IMAGE=eccr.ecmwf.int/polytope/cpp-fdb-gribjump-libs-debug:fdb5.21.3-gribjump0.12.0-r1

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/lukemathwalker/cargo-chef:0.1.77-rust-1.94.1-slim-bookworm@sha256:4787c365155bfff657a58c89e6ce05b99e60d343ee57fd4a0fdcbb2547a8e017 AS chef-base
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*

###############################
# FDB + gribjump libraries (pre-built image, /opt/fdb)
###############################
FROM ${GJ_LIBS_IMAGE} AS gj-build

# Full-symbol variant, used only by the `debug` target.
FROM ${GJ_LIBS_DEBUG_IMAGE} AS gj-build-debug

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
# Python Requirements
###############################
FROM python:3.11-slim-bookworm AS python-deps
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir uv
RUN uv venv /opt/venv
COPY workers/polytope-fe-worker/requirements.txt /requirements.txt
RUN uv pip install --python /opt/venv/bin/python -r /requirements.txt

###############################
# Stage 4: Production Release Image
###############################
FROM python:3.11-slim-bookworm AS release
ARG VERSION
ARG REVISION
ARG GJ_LIBS_IMAGE
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server" \
      int.ecmwf.polytope.libs-image="$GJ_LIBS_IMAGE"

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=gj-build /opt/fdb/ /opt/fdb/
COPY --from=python-deps /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH=/opt/venv/lib/python3.11/site-packages
ENV LD_LIBRARY_PATH=/opt/fdb/lib:/usr/local/lib

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
ENTRYPOINT ["/app/polytope-fe-worker"]

###############################
# Stage 5: Debug Image
###############################
FROM python:3.11-slim-bookworm AS debug
ARG VERSION
ARG REVISION
ARG GJ_LIBS_IMAGE
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server" \
      int.ecmwf.polytope.libs-image="$GJ_LIBS_IMAGE"

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=gj-build-debug /opt/fdb/ /opt/fdb/
COPY --from=python-deps /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH=/opt/venv/lib/python3.11/site-packages
ENV LD_LIBRARY_PATH=/opt/fdb/lib:/usr/local/lib

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
ENTRYPOINT ["/app/polytope-fe-worker"]
