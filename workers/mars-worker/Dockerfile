ARG MARS_BUILD_METHOD=rpm

FROM scratch AS bits
FROM scratch AS rsfdb
FROM scratch AS rsfindlibs
FROM scratch AS mars-client-bundle

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*

###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
WORKDIR /ecmwf/polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY --from=mars-client-bundle . /ecmwf/mars-client-bundle/
COPY workers/common/ workers/common/
COPY workers/mars-worker/ workers/mars-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/mars-client-bundle"]' >> .cargo/config.toml && echo 'mars-client = { path = "/ecmwf/mars-client-bundle/mars-client-cpp/bindings/rust/mars-client" }' >> .cargo/config.toml; fi
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/mars-worker && cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
WORKDIR /ecmwf/polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY --from=mars-client-bundle . /ecmwf/mars-client-bundle/
COPY --from=chef /ecmwf/polytope-server/workers/mars-worker/recipe.json workers/mars-worker/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
COPY workers/common/ workers/common/
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/mars-worker && cargo chef cook --release --recipe-path recipe.json

###############################
# RPM-based MARS client library
###############################
FROM python:3.11-bookworm AS mars-base
ARG rpm_repo

RUN curl -o stable-public.gpg.key "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" \
    && echo "deb ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" >> /etc/apt/sources.list \
    && apt-key add stable-public.gpg.key \
    && apt-get update \
    && apt-get install -y libnetcdf19 liblapack3 \
    && rm -rf /var/lib/apt/lists/*

FROM mars-base AS mars-base-cpp
ARG mars_client_cpp_version=7.1.9.1
RUN apt-get update && apt-get install -y mars-client-cpp=${mars_client_cpp_version} && rm -rf /var/lib/apt/lists/*

FROM mars-base-cpp AS mars-libs-rpm
RUN mkdir -p /opt/mars-client && cp -a /opt/ecmwf/mars-client-cpp/. /opt/mars-client/

###############################
# Source-built MARS client library
###############################
FROM debian:bookworm AS mars-build
ARG MARS_CLIENT_CPP_BRANCH=feature/library-api
ARG ECKIT_VERSION=1.33.0
ARG ECCODES_VERSION=2.46.0
ARG ODC_VERSION=1.6.2
ARG METKIT_VERSION=1.17.0
ARG ATLAS_VERSION=0.45.0
ARG ATLAS_ORCA_VERSION=0.4.3
ARG MIR_VERSION=1.23.7
ARG FDB_VERSION=5.19.0

RUN apt-get update && apt-get install -y \
    cmake build-essential git libaec-dev bison flex net-tools \
    libssl-dev libnetcdf-dev curl \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch 3.12.0 https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

COPY --from=mars-client-bundle . /source/mars-client-bundle/
RUN if [ ! -f /source/mars-client-bundle/CMakeLists.txt ]; then \
    git clone --single-branch --branch cpp https://github.com/ecmwf/mars-client-bundle.git /source/mars-client-bundle; \
    fi

RUN sed -i \
    -e "s|PROJECT eckit.*UPDATE)|PROJECT eckit GIT \"https://github.com/ecmwf/eckit\" TAG ${ECKIT_VERSION} UPDATE)|" \
    -e "s|PROJECT eccodes.*UPDATE)|PROJECT eccodes GIT \"https://github.com/ecmwf/eccodes\" TAG ${ECCODES_VERSION} UPDATE)|" \
    -e "s|PROJECT odc.*UPDATE)|PROJECT odc GIT \"https://github.com/ecmwf/odc\" TAG ${ODC_VERSION} UPDATE)|" \
    -e "s|PROJECT metkit.*UPDATE)|PROJECT metkit GIT \"https://github.com/ecmwf/metkit\" TAG ${METKIT_VERSION} UPDATE)|" \
    -e "s|PROJECT atlas .*UPDATE)|PROJECT atlas GIT \"https://github.com/ecmwf/atlas\" TAG ${ATLAS_VERSION} UPDATE)|" \
    -e "s|PROJECT atlas-orca.*UPDATE)|PROJECT atlas-orca GIT \"https://github.com/ecmwf/atlas-orca\" TAG ${ATLAS_ORCA_VERSION} UPDATE)|" \
    -e "s|PROJECT mir .*UPDATE)|PROJECT mir GIT \"https://github.com/ecmwf/mir\" TAG ${MIR_VERSION} UPDATE)|" \
    -e "s|PROJECT fdb5.*UPDATE)|PROJECT fdb5 GIT \"https://github.com/ecmwf/fdb\" TAG ${FDB_VERSION} UPDATE)|" \
    -e "s|PROJECT mars-client-cpp.*|PROJECT mars-client-cpp GIT \"https://github.com/ecmwf/mars-client-cpp\" BRANCH ${MARS_CLIENT_CPP_BRANCH} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt

RUN mkdir -p /build/mars-client-bundle && cd /build/mars-client-bundle \
    && ecbuild \
        --prefix=/opt/mars-client \
        -DENABLE_FORTRAN=OFF \
        -DENABLE_PYTHON=OFF \
        -DENABLE_AEC=ON \
        -DENABLE_POLYTOPE=OFF \
        -DENABLE_BUILD_TOOLS=OFF \
        /source/mars-client-bundle \
    && make -j$(nproc) \
    && make install

FROM mars-build AS mars-libs-source

###############################
# Select library provider
###############################
ARG MARS_BUILD_METHOD=rpm
FROM mars-libs-${MARS_BUILD_METHOD} AS mars-libs

###############################
# Stage 3: Build the Project
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
ARG MARS_BUILD_METHOD=rpm
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=mars-worker
ARG BIN_NAME=mars-worker
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY --from=mars-client-bundle . /ecmwf/mars-client-bundle/
COPY --from=mars-libs /opt/mars-client /opt/mars-client
COPY workers/common/ workers/common/
COPY workers/mars-worker/ workers/mars-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/mars-client-bundle"]' >> .cargo/config.toml && echo 'mars-client = { path = "/ecmwf/mars-client-bundle/mars-client-cpp/bindings/rust/mars-client" }' >> .cargo/config.toml; fi
COPY --from=cacher /ecmwf/polytope-server/workers/mars-worker/target workers/mars-worker/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
ENV MARS_CLIENT_LIB_DIR=/opt/mars-client/lib
ENV MARS_CLIENT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/mars-worker && cargo build --release -p ${PACKAGE_NAME}

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

ENV LD_LIBRARY_PATH="/opt/mars-client/lib:${LD_LIBRARY_PATH}"

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

ENV LD_LIBRARY_PATH="/opt/mars-client/lib:${LD_LIBRARY_PATH}"

COPY --from=builder /ecmwf/polytope-server/workers/mars-worker/target/release/${BIN_NAME} /app/mars-worker
ENTRYPOINT ["/app/mars-worker"]
