###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
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
        -DENABLE_ECCODES=OFF \
        -DENABLE_TESTS=OFF \
        -DENABLE_MARS2GRIB=OFF \
        -Deckit_DIR=/opt/metkit/lib/cmake/eckit \
        /src/metkit \
    && make -j$(nproc) metkit metkit.pc-pkg-config \
    && : > /build/metkit/src/tools/cmake_install.cmake \
    && cmake --install /build/metkit

###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
COPY utils/metkit/ utils/metkit/
COPY frontend/ frontend/
RUN mkdir -p .cargo && echo '[net]' > .cargo/config.toml && echo 'git-fetch-with-cli = true' >> .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd frontend && cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
COPY utils/metkit/ utils/metkit/
COPY --from=chef /ecmwf/polytope-server/frontend/recipe.json frontend/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd frontend && cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=polytope-server
ARG BIN_NAME=polytope-server
ARG FEATURES="metkit"
COPY Cargo.lock Cargo.lock
COPY utils/metkit/ utils/metkit/
COPY --from=metkit-build /opt/metkit /opt/metkit
COPY frontend/ frontend/
RUN mkdir -p .cargo && echo '[net]' > .cargo/config.toml && echo 'git-fetch-with-cli = true' >> .cargo/config.toml
COPY --from=cacher /ecmwf/polytope-server/frontend/target frontend/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
ENV METKIT_LIB_DIR=/opt/metkit/lib
ENV METKIT_INCLUDE_DIR=/opt/metkit/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_INCLUDE_DIR=/opt/metkit/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/metkit/include
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd frontend && cargo build --release -p ${PACKAGE_NAME} \
    $(if [ -n "$FEATURES" ]; then echo "--features $FEATURES"; fi)

###############################
# Stage 4: Production Release Image
###############################
FROM gcr.io/distroless/cc-debian12:nonroot AS release
ARG VERSION
ARG BIN_NAME=polytope-server
LABEL version=$VERSION
WORKDIR /app
COPY --from=metkit-build /opt/metkit/lib /opt/metkit/lib
COPY --from=builder /ecmwf/polytope-server/frontend/target/release/${BIN_NAME} /app/polytope-server
ENV LD_LIBRARY_PATH="/opt/metkit/lib"
ENTRYPOINT ["/app/polytope-server"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG BIN_NAME=polytope-server
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=metkit-build /opt/metkit/lib /opt/metkit/lib
COPY --from=builder /ecmwf/polytope-server/frontend/target/release/${BIN_NAME} /app/polytope-server
ENV LD_LIBRARY_PATH="/opt/metkit/lib"
ENTRYPOINT ["/app/polytope-server"]
