# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# Pre-built C++ FDB stack (eckit, eccodes, metkit, fdb) pulled from ECCR.
# Built and published from docker/cpp-libs/fdb by the cpp-libs workflow.
# The stripped image feeds `release`; the -debug (full-symbol) image feeds `debug`.
ARG FDB_LIBS_IMAGE=eccr.ecmwf.int/polytope/cpp-fdb-libs:fdb5.19.2-r1
ARG FDB_LIBS_DEBUG_IMAGE=eccr.ecmwf.int/polytope/cpp-fdb-libs-debug:fdb5.19.2-r1

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/library/rust:1.94.1-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# FDB libraries (pre-built image, /opt/fdb)
###############################
FROM ${FDB_LIBS_IMAGE} AS fdb-build

# Full-symbol variant, used only by the `debug` target.
FROM ${FDB_LIBS_DEBUG_IMAGE} AS fdb-build-debug

###############################
# Prepare the root workspace Cargo Chef recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
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
ARG PACKAGE_NAME=fdb-worker
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
ARG PACKAGE_NAME=fdb-worker
ARG BIN_NAME=fdb-worker
WORKDIR /ecmwf/polytope-server
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
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
ARG REVISION
ARG FDB_LIBS_IMAGE
ARG BIN_NAME=fdb-worker
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server" \
      int.ecmwf.polytope.libs-image="$FDB_LIBS_IMAGE"
RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=fdb-build /opt/fdb/ /opt/fdb/
ENV LD_LIBRARY_PATH=/opt/fdb/lib
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/fdb-worker
ENTRYPOINT ["/app/fdb-worker"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG REVISION
ARG FDB_LIBS_IMAGE
ARG BIN_NAME=fdb-worker
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server" \
      int.ecmwf.polytope.libs-image="$FDB_LIBS_IMAGE"
RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=fdb-build-debug /opt/fdb/ /opt/fdb/
ENV LD_LIBRARY_PATH=/opt/fdb/lib
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/fdb-worker
ENTRYPOINT ["/app/fdb-worker"]
