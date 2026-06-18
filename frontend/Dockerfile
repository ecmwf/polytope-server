###############################
# Shared base: Rust + system deps
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS rust-base
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# Build eckit + metkit from public GitHub repos
###############################
FROM docker.io/library/debian:bookworm AS metkit-build
ARG ECBUILD_VERSION=3.12.0
ARG ECKIT_VERSION=2.0.4
ARG METKIT_VERSION=1.17.1

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
# Build the frontend package with the workspace lockfile
###############################
FROM rust-base AS builder
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=polytope-server
ARG BIN_NAME=polytope-server
ARG FEATURES="metkit"

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY utils/metkit/ utils/metkit/
COPY observability/ observability/
COPY frontend/ frontend/
COPY client/ client/
COPY loadgen/ loadgen/
COPY tests/integration/ tests/integration/
COPY workers/common/ workers/common/
COPY workers/polytope-fe-worker/ workers/polytope-fe-worker/
COPY workers/fdb-worker/ workers/fdb-worker/
COPY workers/test-worker/ workers/test-worker/
COPY --from=metkit-build /opt/metkit /opt/metkit

RUN mkdir -p .cargo && echo '[net]' > .cargo/config.toml && echo 'git-fetch-with-cli = true' >> .cargo/config.toml
ENV METKIT_LIB_DIR=/opt/metkit/lib
ENV METKIT_INCLUDE_DIR=/opt/metkit/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo build --release --locked -p ${PACKAGE_NAME} \
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
