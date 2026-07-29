# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# Pre-built MARS client libraries pulled from ECCR.
# Built and published from docker/cpp-libs/mars by the cpp-libs workflow
# (source-built bundle by default; RPM variant via MARS_BUILD_FROM_SOURCE=false
# when publishing the library image).
ARG MARS_LIBS_IMAGE=eccr.ecmwf.int/polytope/cpp-mars-libs:7.1.9-r1

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/library/rust:1.94.1-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*

###############################
# MARS client libraries (pre-built image, /opt/mars-client)
###############################
FROM ${MARS_LIBS_IMAGE} AS mars-libs

###############################
# Prepare the standalone Mars Cargo Chef recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY workers/mars-worker/ workers/mars-worker/
COPY observability/ observability/
COPY workers/common/ workers/common/
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cd workers/mars-worker && cargo chef prepare --recipe-path recipe.json

###############################
# Cache standalone Mars dependencies without application source
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=mars-worker
WORKDIR /ecmwf/polytope-server
COPY --from=mars-libs /opt/mars-client /opt/mars-client
COPY --from=chef /ecmwf/polytope-server/workers/mars-worker/recipe.json workers/mars-worker/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
# cargo-chef omits path dependencies outside the standalone crate root.
COPY observability/Cargo.toml observability/Cargo.toml
COPY workers/common/Cargo.toml workers/common/Cargo.toml
RUN mkdir -p observability/src workers/common/src \
    && touch observability/src/lib.rs workers/common/src/lib.rs
ENV MARS_CLIENT_LIB_DIR=/opt/mars-client/lib
ENV MARS_CLIENT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
# cargo-chef rewrites standalone path packages, so its synthetic lock is not reusable.
# The final build below enforces the committed standalone lockfile.
RUN cd workers/mars-worker && cargo chef cook --release --package "${PACKAGE_NAME}" --recipe-path recipe.json

###############################
# Build Mars with its standalone lockfile
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=mars-worker
ARG BIN_NAME=mars-worker
WORKDIR /ecmwf/polytope-server
COPY --from=mars-libs /opt/mars-client /opt/mars-client
COPY --from=cacher /ecmwf/polytope-server/workers/mars-worker/target workers/mars-worker/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY workers/mars-worker/ workers/mars-worker/
COPY observability/ observability/
COPY workers/common/ workers/common/
# Replace the cached path-dependency skeletons with their real sources.
RUN touch observability/src/lib.rs workers/common/src/lib.rs
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
ENV MARS_CLIENT_LIB_DIR=/opt/mars-client/lib
ENV MARS_CLIENT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cd workers/mars-worker && cargo build --release --locked --package "${PACKAGE_NAME}"

###############################
# Stage 4: Production Release Image
###############################
FROM docker.io/library/debian:bookworm-slim AS release
ARG VERSION
ARG BIN_NAME=mars-worker
LABEL version=$VERSION

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libnetcdf19 liblapack3 libaec0 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=mars-libs /opt/mars-client /opt/mars-client

ENV LD_LIBRARY_PATH=/opt/mars-client/lib

COPY --from=builder /ecmwf/polytope-server/workers/mars-worker/target/release/${BIN_NAME} /app/mars-worker
ENTRYPOINT ["/app/mars-worker"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG BIN_NAME=mars-worker
LABEL version=$VERSION

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libnetcdf19 liblapack3 libaec0 bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=mars-libs /opt/mars-client /opt/mars-client

ENV LD_LIBRARY_PATH=/opt/mars-client/lib

COPY --from=builder /ecmwf/polytope-server/workers/mars-worker/target/release/${BIN_NAME} /app/mars-worker
ENTRYPOINT ["/app/mars-worker"]
