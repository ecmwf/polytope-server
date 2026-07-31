# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# Pre-built C++ eckit + metkit stack pulled from ECCR.
# Built and published from docker/cpp-libs/metkit by the cpp-libs workflow.
# The stripped image feeds `release`; the -debug (full-symbol) image feeds `debug`.
ARG METKIT_LIBS_IMAGE=eccr.ecmwf.int/polytope/cpp-metkit-libs:metkit1.17.0-r1
ARG METKIT_LIBS_DEBUG_IMAGE=eccr.ecmwf.int/polytope/cpp-metkit-libs-debug:metkit1.17.0-r1

###############################
# Shared base: Rust + cargo-chef + system deps
###############################
FROM docker.io/library/rust:1.94.1-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# eckit + metkit libraries (pre-built image, /opt/metkit)
###############################
FROM ${METKIT_LIBS_IMAGE} AS metkit-build

# Full-symbol variant, used only by the `debug` target.
FROM ${METKIT_LIBS_DEBUG_IMAGE} AS metkit-build-debug

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
ARG PACKAGE_NAME=polytope-server
ARG FEATURES="metkit,telemetry"
WORKDIR /ecmwf/polytope-server
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
# cargo-chef omits excluded path dependencies from root workspace recipes.
COPY utils/metkit/Cargo.toml utils/metkit/Cargo.toml
RUN mkdir -p utils/metkit/src && touch utils/metkit/src/lib.rs \
    && printf 'fn main() {}\n' > utils/metkit/build.rs
COPY --from=metkit-build /opt/metkit /opt/metkit
ENV METKIT_LIB_DIR=/opt/metkit/lib
ENV METKIT_INCLUDE_DIR=/opt/metkit/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV GIT_TERMINAL_PROMPT=0
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo chef cook --release --locked --package "${PACKAGE_NAME}" \
    $(if [ -n "$FEATURES" ]; then echo "--features $FEATURES"; fi) \
    --recipe-path recipe.json

###############################
# Build the frontend from the root workspace
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=polytope-server
ARG BIN_NAME=polytope-server
ARG FEATURES="metkit,telemetry"
WORKDIR /ecmwf/polytope-server
COPY --from=metkit-build /opt/metkit /opt/metkit
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY --from=manifests /ecmwf/polytope-server/ /ecmwf/polytope-server/
# real sources only for this package's dependency graph (overwrite the stubs)
COPY observability/ observability/
COPY frontend/ frontend/
COPY utils/metkit/ utils/metkit/
# Replace the cached metkit skeleton with the real excluded path dependency and
# bust the cargo-chef skeleton fingerprints of the real-source crates.
RUN touch utils/metkit/build.rs utils/metkit/src/lib.rs \
    observability/src/lib.rs frontend/src/lib.rs frontend/src/main.rs
ENV METKIT_LIB_DIR=/opt/metkit/lib
ENV METKIT_INCLUDE_DIR=/opt/metkit/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV GIT_TERMINAL_PROMPT=0
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo build --release --locked --package "${PACKAGE_NAME}" \
    $(if [ -n "$FEATURES" ]; then echo "--features $FEATURES"; fi)

###############################
# Production Release Image
###############################
FROM gcr.io/distroless/cc-debian12:nonroot AS release
ARG VERSION
ARG REVISION
ARG METKIT_LIBS_IMAGE
ARG BIN_NAME=polytope-server
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server" \
      int.ecmwf.polytope.libs-image="$METKIT_LIBS_IMAGE"
WORKDIR /app
COPY --from=metkit-build /opt/metkit/lib /opt/metkit/lib
COPY --from=metkit-build /opt/metkit/share /opt/metkit/share
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-server
ENV LD_LIBRARY_PATH="/opt/metkit/lib"
ENTRYPOINT ["/app/polytope-server"]

###############################
# Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG REVISION
ARG METKIT_LIBS_IMAGE
ARG BIN_NAME=polytope-server
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server" \
      int.ecmwf.polytope.libs-image="$METKIT_LIBS_IMAGE"
RUN apt-get update && apt-get install -y ca-certificates bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=metkit-build-debug /opt/metkit/lib /opt/metkit/lib
COPY --from=metkit-build-debug /opt/metkit/share /opt/metkit/share
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-server
ENV LD_LIBRARY_PATH="/opt/metkit/lib"
ENTRYPOINT ["/app/polytope-server"]
