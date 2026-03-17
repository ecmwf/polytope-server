# ARG before first FROM → global scope, available in FROM instruction below
ARG MARS_BUILD_METHOD=rpm

FROM scratch AS bits
FROM scratch AS rsfdb
FROM scratch AS rsfindlibs
FROM scratch AS mars-client-bundle

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
COPY --from=mars-client-bundle . /ecmwf/mars-client-bundle/
COPY . .
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfdb"]' >> .cargo/config.toml && echo 'rsfdb = { path = "/ecmwf/rsfdb" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfindlibs"]' >> .cargo/config.toml && echo 'rsfindlibs = { path = "/ecmwf/rsfindlibs" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/mars-client-bundle"]' >> .cargo/config.toml && echo 'mars-client = { path = "/ecmwf/mars-client-bundle/mars-client-cpp/bindings/rust/mars-client" }' >> .cargo/config.toml; fi
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo chef prepare --recipe-path recipe.json

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
COPY --from=mars-client-bundle . /ecmwf/mars-client-bundle/
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo chef cook --release --recipe-path recipe.json

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

# Normalize RPM install path to /opt/mars-client/
FROM mars-base-cpp AS mars-libs-rpm
RUN mkdir -p /opt/mars-client && cp -a /opt/ecmwf/mars-client-cpp/. /opt/mars-client/

###############################
# Source-built MARS client library
###############################
FROM debian:bookworm AS mars-build
ARG ecbuild_version=3.12.0

RUN apt-get update && apt-get install -y \
    cmake build-essential git libaec-dev bison flex net-tools \
    libssl-dev libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${ecbuild_version} https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

COPY --from=mars-client-bundle . /source/mars-client-bundle/
RUN if [ ! -f /source/mars-client-bundle/CMakeLists.txt ]; then \
    git clone --single-branch --branch cpp https://github.com/ecmwf/mars-client-bundle.git /source/mars-client-bundle; \
    fi

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
FROM docker.io/library/rust:1.90-slim-bookworm AS builder
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
ARG MARS_BUILD_METHOD=rpm
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=mars-worker
ARG BIN_NAME=mars-worker
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY --from=mars-client-bundle . /ecmwf/mars-client-bundle/
COPY --from=mars-libs /opt/mars-client /opt/mars-client
COPY . .
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfdb"]' >> .cargo/config.toml && echo 'rsfdb = { path = "/ecmwf/rsfdb" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfindlibs"]' >> .cargo/config.toml && echo 'rsfindlibs = { path = "/ecmwf/rsfindlibs" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/mars-client-bundle"]' >> .cargo/config.toml && echo 'mars-client = { path = "/ecmwf/mars-client-bundle/mars-client-cpp/bindings/rust/mars-client" }' >> .cargo/config.toml; fi
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
ENV MARS_CLIENT_LIB_DIR=/opt/mars-client/lib
ENV MARS_CLIENT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo build --release -p ${PACKAGE_NAME}

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

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/mars-worker
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

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/mars-worker
ENTRYPOINT ["/app/mars-worker"]
