# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# Cargo's locked -sys dependencies build the native MARS stack and its bridges
# together. Do not mix these libraries with the separately built bundle image.
FROM docker.io/lukemathwalker/cargo-chef:0.1.77-rust-1.94.1-slim-bookworm@sha256:4787c365155bfff657a58c89e6ce05b99e60d343ee57fd4a0fdcbb2547a8e017 AS chef-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates pkg-config build-essential cmake bison flex python3 libclang-dev \
    libssl-dev libcurl4-openssl-dev libaec-dev libopenjp2-7-dev libpng-dev \
    liblapack-dev liblz4-dev libfftw3-dev libeigen3-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
ENV CARGO_BUILD_JOBS=8 CMAKE_BUILD_PARALLEL_LEVEL=8
WORKDIR /ecmwf/polytope-server
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml

FROM chef-base AS chef
COPY workers/mars-worker/ workers/mars-worker/
COPY observability/ observability/
COPY workers/common/ workers/common/
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && git config --global --add url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "ssh://git@github.com/" \
    && cd workers/mars-worker && cargo chef prepare --recipe-path recipe.json

FROM chef-base AS cacher
ARG PACKAGE_NAME=mars-worker
COPY --from=chef /ecmwf/polytope-server/workers/mars-worker/recipe.json workers/mars-worker/recipe.json
# cargo-chef omits path dependencies outside the standalone crate root.
COPY observability/Cargo.toml observability/Cargo.toml
COPY workers/common/Cargo.toml workers/common/Cargo.toml
RUN mkdir -p observability/src workers/common/src \
    && touch observability/src/lib.rs workers/common/src/lib.rs
# The synthetic chef lock rewrites path packages; the final build is locked.
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && git config --global --add url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "ssh://git@github.com/" \
    && cd workers/mars-worker \
    && cargo chef cook --release --package "${PACKAGE_NAME}" --recipe-path recipe.json

FROM chef-base AS builder
ARG PACKAGE_NAME=mars-worker
ARG BIN_NAME=mars-worker
ARG VERSION
ENV POLYTOPE_VERSION=$VERSION
COPY --from=cacher /ecmwf/polytope-server/workers/mars-worker/target workers/mars-worker/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY workers/mars-worker/ workers/mars-worker/
COPY observability/ observability/
COPY workers/common/ workers/common/
RUN touch observability/src/lib.rs workers/common/src/lib.rs
RUN --mount=type=secret,id=GIT_AUTH_TOKEN,required=true \
    export GIT_CONFIG_GLOBAL="$(mktemp)" \
    && trap 'rm -f "$GIT_CONFIG_GLOBAL"' EXIT \
    && git config --global credential.helper '' \
    && git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" \
    && git config --global --add url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "ssh://git@github.com/" \
    && cd workers/mars-worker \
    && cargo build --release --locked --package "${PACKAGE_NAME}"
# Package installed libraries and their resources, not build trees or headers.
# ecCodes installs below install/eccodes; cmake-rs installs MARS in its out dir.
RUN set -eu; mkdir -p /native/lib; \
    for prefix in workers/mars-worker/target/release/build/*/out/install \
                  workers/mars-worker/target/release/build/*/out/install/eccodes \
                  workers/mars-worker/target/release/build/*/out/mars-client-cpp-build; do \
        for dir in lib lib64; do \
            if [ -d "$prefix/$dir" ]; then cp -a "$prefix/$dir/." /native/lib/; fi; \
        done; \
        for dir in share etc; do \
            if [ -d "$prefix/$dir" ]; then mkdir -p "/native/$dir"; cp -a "$prefix/$dir/." "/native/$dir/"; fi; \
        done; \
    done; \
    find /native/lib -name '*.a' -delete; \
    test -f /native/lib/libmars_client_cpp.so; \
    cp "workers/mars-worker/target/release/${BIN_NAME}" /native/mars-worker

FROM docker.io/library/debian:bookworm-slim AS runtime
ARG VERSION
ARG REVISION
LABEL org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.source="https://github.com/ecmwf/polytope-server"
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libgomp1 liblapack3 libaec0 libcurl4 libopenjp2-7 libpng16-16 liblz4-1 libfftw3-double3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV LD_LIBRARY_PATH=/opt/mars-client/lib
COPY --from=builder /native/ /opt/mars-client/
RUN mv /opt/mars-client/mars-worker /app/mars-worker \
    && /app/mars-worker --help >/dev/null
ENTRYPOINT ["/app/mars-worker"]

FROM runtime AS debug
RUN apt-get update && apt-get install -y --no-install-recommends bash gdb \
    && rm -rf /var/lib/apt/lists/*

FROM builder AS stripped
RUN find /native/lib -type f -name '*.so*' -exec strip --strip-unneeded {} + \
    && strip --strip-unneeded /native/mars-worker

FROM runtime AS release
COPY --from=stripped /native/lib/ /opt/mars-client/lib/
COPY --from=stripped /native/mars-worker /app/mars-worker
