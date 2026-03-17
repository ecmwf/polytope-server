FROM scratch AS bits
FROM scratch AS rsfdb
FROM scratch AS rsfindlibs

###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS chef
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY workers/common/ workers/common/
COPY workers/fdb-worker/ workers/fdb-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfdb"]' >> .cargo/config.toml && echo 'rsfdb = { path = "/ecmwf/rsfdb" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfindlibs"]' >> .cargo/config.toml && echo 'rsfindlibs = { path = "/ecmwf/rsfindlibs" }' >> .cargo/config.toml; fi
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/fdb-worker && cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS cacher
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY --from=chef /ecmwf/polytope-server/workers/fdb-worker/recipe.json workers/fdb-worker/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/fdb-worker && cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS builder
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=fdb-worker
ARG BIN_NAME=fdb-worker
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY workers/common/ workers/common/
COPY workers/fdb-worker/ workers/fdb-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfdb"]' >> .cargo/config.toml && echo 'rsfdb = { path = "/ecmwf/rsfdb" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfindlibs"]' >> .cargo/config.toml && echo 'rsfindlibs = { path = "/ecmwf/rsfindlibs" }' >> .cargo/config.toml; fi
COPY --from=cacher /ecmwf/polytope-server/workers/fdb-worker/target workers/fdb-worker/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/fdb-worker && cargo build --release -p ${PACKAGE_NAME}

###############################
# FDB Build (from source)
###############################
FROM python:3.11-bookworm AS fdb-build
ARG ecbuild_version=3.12.0
ARG eccodes_version=2.46.0
ARG eckit_version=1.33.1
ARG fdb_version=5.19.2
ARG pyfdb_version=0.1.0

RUN apt-get update && apt-get install -y cmake gnupg build-essential net-tools bison flex git libaec-dev && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${ecbuild_version} https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

RUN git clone --depth 1 --branch ${eckit_version} https://github.com/ecmwf/eckit.git /source/eckit \
    && mkdir -p /build/eckit && cd /build/eckit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/eckit \
    && make -j4 && make install

RUN git clone --depth 1 --branch ${eccodes_version} https://github.com/ecmwf/eccodes.git /source/eccodes \
    && mkdir -p /build/eccodes && cd /build/eccodes \
    && ecbuild --prefix=/opt/fdb -- -DENABLE_FORTRAN=OFF -DENABLE_AEC=OFF -DCMAKE_PREFIX_PATH=/opt/fdb /source/eccodes \
    && make -j4 && make install

RUN git clone --depth 1 --branch 1.17.0 https://github.com/ecmwf/metkit.git /source/metkit \
    && mkdir -p /build/metkit && cd /build/metkit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/metkit \
    && make -j4 && make install

RUN git clone --depth 1 --branch ${fdb_version} https://github.com/ecmwf/fdb.git /source/fdb \
    && mkdir -p /build/fdb && cd /build/fdb \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH="/opt/fdb;/opt/fdb/eckit;/opt/fdb/metkit" /source/fdb \
    && make -j4 && make install

RUN rm -rf /source /build

###############################
# Switch base image
###############################
FROM fdb-build AS fdb-base-final

###############################
# Stage 4: Production Release Image
###############################
FROM docker.io/library/debian:bookworm-slim AS release
ARG VERSION
ARG BIN_NAME=fdb-worker
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=fdb-base-final /opt/fdb/ /opt/fdb/
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/fdb/lib
COPY --from=builder /ecmwf/polytope-server/workers/fdb-worker/target/release/${BIN_NAME} /app/fdb-worker
ENTRYPOINT ["/app/fdb-worker"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG BIN_NAME=fdb-worker
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=fdb-base-final /opt/fdb/ /opt/fdb/
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/fdb/lib
COPY --from=builder /ecmwf/polytope-server/workers/fdb-worker/target/release/${BIN_NAME} /app/fdb-worker
ENTRYPOINT ["/app/fdb-worker"]
