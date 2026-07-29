# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

###############################
# Shared base: Rust + cargo-chef + system deps
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# Workspace manifest skeleton — manifests plus stub sources for every member.
# Keep this stage identical across all Dockerfiles so the shared build cache
# reuses it between images. Adding a workspace member requires updating it.
###############################
FROM chef-base AS manifests
WORKDIR /ecmwf/polytope-server
COPY Cargo.toml Cargo.lock ./
COPY observability/Cargo.toml observability/Cargo.toml
COPY frontend/Cargo.toml frontend/Cargo.toml
COPY loadgen/Cargo.toml loadgen/Cargo.toml
COPY client/Cargo.toml client/Cargo.toml
COPY tests/integration/Cargo.toml tests/integration/Cargo.toml
COPY workers/common/Cargo.toml workers/common/Cargo.toml
COPY workers/polytope-fe-worker/Cargo.toml workers/polytope-fe-worker/Cargo.toml
COPY workers/fdb-worker/Cargo.toml workers/fdb-worker/Cargo.toml
COPY workers/test-worker/Cargo.toml workers/test-worker/Cargo.toml
# excluded path dependency of the frontend — needed for workspace resolution
COPY utils/metkit/Cargo.toml utils/metkit/Cargo.toml
RUN mkdir -p observability/src frontend/src loadgen/src client/src \
      tests/integration/src workers/common/src workers/polytope-fe-worker/src \
      workers/fdb-worker/src workers/test-worker/src utils/metkit/src \
    && touch observability/src/lib.rs frontend/src/lib.rs loadgen/src/lib.rs \
      client/src/lib.rs tests/integration/src/lib.rs workers/common/src/lib.rs \
      workers/test-worker/src/lib.rs utils/metkit/src/lib.rs \
    && for crate in frontend loadgen workers/polytope-fe-worker workers/fdb-worker workers/test-worker; do \
      printf 'fn main() {}\n' > "$crate/src/main.rs"; done \
    && printf 'fn main() {}\n' > utils/metkit/build.rs \
    && mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml

###############################
# Prepare the root workspace Cargo Chef recipe (manifests only)
###############################
FROM manifests AS chef
ARG GIT_AUTH_TOKEN=""
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo chef prepare --recipe-path recipe.json

###############################
# Cache root workspace dependencies without application source
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=test-worker
WORKDIR /ecmwf/polytope-server
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
# cargo-chef omits excluded path dependencies from root workspace recipes.
COPY utils/metkit/Cargo.toml utils/metkit/Cargo.toml
RUN mkdir -p utils/metkit/src && touch utils/metkit/src/lib.rs \
    && printf 'fn main() {}\n' > utils/metkit/build.rs
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo chef cook --release --locked --package "${PACKAGE_NAME}" --recipe-path recipe.json

###############################
# Build the package from the root workspace
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=test-worker
ARG BIN_NAME=test-worker
WORKDIR /ecmwf/polytope-server
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY --from=manifests /ecmwf/polytope-server/ /ecmwf/polytope-server/
# real sources only for this package's dependency graph (overwrite the stubs)
COPY observability/ observability/
COPY workers/common/ workers/common/
COPY workers/test-worker/ workers/test-worker/
# bust the cargo-chef skeleton fingerprints of the real-source crates
RUN touch observability/src/lib.rs workers/common/src/lib.rs \
    workers/test-worker/src/lib.rs workers/test-worker/src/main.rs
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo build --release --locked --package "${PACKAGE_NAME}"

###############################
# Stage 4: Production Release Image
###############################
FROM docker.io/library/debian:bookworm-slim AS release
ARG VERSION
ARG BIN_NAME=test-worker
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/test-worker
ENTRYPOINT ["/app/test-worker"]
