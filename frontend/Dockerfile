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
# Build eckit + metkit from public GitHub repos
###############################
FROM docker.io/library/debian:bookworm-slim AS metkit-build
ARG ECBUILD_VERSION=3.12.0
ARG ECKIT_VERSION=1.33.0
ARG METKIT_VERSION=1.17.0

RUN apt-get update && apt-get install -y \
    cmake build-essential git libaec-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${ECBUILD_VERSION} https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

RUN git clone --depth 1 --branch ${ECKIT_VERSION} https://github.com/ecmwf/eckit.git /src/eckit \
    && mkdir -p /build/eckit && cd /build/eckit \
    && ecbuild --prefix=/opt/metkit \
        -DENABLE_FORTRAN=OFF \
        -DENABLE_BUILD_TOOLS=OFF \
        -DENABLE_TESTS=OFF \
        -DENABLE_GEO=OFF \
        /src/eckit \
    && make -j$(nproc) \
    && make install

RUN git clone --depth 1 --branch ${METKIT_VERSION} https://github.com/ecmwf/metkit.git /src/metkit \
    && mkdir -p /build/metkit && cd /build/metkit \
    && ecbuild --prefix=/opt/metkit \
        -DENABLE_FORTRAN=OFF \
        -DENABLE_BUILD_TOOLS=OFF \
        -DENABLE_TESTS=OFF \
        -DENABLE_GEO=OFF \
        -DENABLE_ECCODES=OFF \
        -DENABLE_MARS2GRIB=OFF \
        -Deckit_DIR=/opt/metkit/lib/cmake/eckit \
        /src/metkit \
    && make -j$(nproc) metkit metkit.pc-pkg-config \
    && : > /build/metkit/src/tools/cmake_install.cmake \
    && cmake --install /build/metkit

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
ARG BIN_NAME=polytope-server
LABEL version=$VERSION
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
ARG BIN_NAME=polytope-server
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=metkit-build /opt/metkit/lib /opt/metkit/lib
COPY --from=metkit-build /opt/metkit/share /opt/metkit/share
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-server
ENV LD_LIBRARY_PATH="/opt/metkit/lib"
ENTRYPOINT ["/app/polytope-server"]
