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
FROM docker.io/lukemathwalker/cargo-chef:0.1.77-rust-1.94.1-slim-bookworm@sha256:4787c365155bfff657a58c89e6ce05b99e60d343ee57fd4a0fdcbb2547a8e017 AS chef-base
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# eckit + metkit libraries (pre-built image, /opt/metkit)
###############################
FROM ${METKIT_LIBS_IMAGE} AS metkit-build

# Full-symbol variant, used only by the `debug` target.
FROM ${METKIT_LIBS_DEBUG_IMAGE} AS metkit-build-debug

###############################
# Prepare the root workspace Cargo Chef recipe
###############################
FROM chef-base AS chef
WORKDIR /ecmwf/polytope-server
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN cargo chef prepare --recipe-path recipe.json

###############################
# Cache root workspace dependencies without application source
###############################
FROM chef-base AS cacher
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
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && cargo chef cook --release --locked --package "${PACKAGE_NAME}" \
       $(if [ -n "$FEATURES" ]; then echo "--features $FEATURES"; fi) \
       --recipe-path recipe.json

###############################
# Build the frontend from the root workspace
###############################
FROM chef-base AS builder
ARG PACKAGE_NAME=polytope-server
ARG BIN_NAME=polytope-server
ARG FEATURES="metkit,telemetry"
WORKDIR /ecmwf/polytope-server
COPY --from=metkit-build /opt/metkit /opt/metkit
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
# Replace the cached metkit skeleton with the real excluded path dependency.
RUN touch utils/metkit/build.rs
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
ENV METKIT_LIB_DIR=/opt/metkit/lib
ENV METKIT_INCLUDE_DIR=/opt/metkit/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV GIT_TERMINAL_PROMPT=0
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && cargo build --release --locked --package "${PACKAGE_NAME}" \
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
