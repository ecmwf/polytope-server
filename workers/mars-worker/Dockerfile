ARG MARS_BUILD_FROM_SOURCE=false

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*

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

FROM mars-base-cpp AS mars-libs-false
RUN mkdir -p /opt/mars-client && cp -a /opt/ecmwf/mars-client-cpp/. /opt/mars-client/

###############################
# Source-built MARS client library
###############################
FROM debian:bookworm AS mars-build
ARG GIT_AUTH_TOKEN=""
ARG MARS_CLIENT_CPP_BRANCH=""
ARG ECKIT_VERSION=""
ARG ECCODES_VERSION=""
ARG ODC_VERSION=""
ARG METKIT_VERSION=""
ARG ATLAS_VERSION=""
ARG ATLAS_ORCA_VERSION=""
ARG MIR_VERSION=""
ARG FDB_VERSION=""

RUN apt-get update && apt-get install -y \
    build-essential git bison flex net-tools \
    libssl-dev libnetcdf-dev curl \
    && curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.31.6/cmake-3.31.6-linux-x86_64.sh -o /tmp/cmake.sh \
    && sh /tmp/cmake.sh --skip-license --prefix=/usr/local \
    && rm /tmp/cmake.sh \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch v1.1.4 https://github.com/Deutsches-Klimarechenzentrum/libaec.git /source/libaec \
    && mkdir -p /build/libaec && cd /build/libaec \
    && cmake -DCMAKE_INSTALL_PREFIX=/opt/mars-client /source/libaec \
    && make -j$(nproc) && make install \
    && rm -rf /source/libaec /build/libaec

RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
    printf "machine github.com\nlogin x-access-token\npassword %s\n" "$GIT_AUTH_TOKEN" > /root/.netrc && \
    chmod 600 /root/.netrc && \
    git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"; \
    fi
RUN git clone --depth 1 --branch 3.12.0 https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH
RUN git clone --single-branch --branch cpp https://github.com/ecmwf/mars-client-bundle.git /source/mars-client-bundle

RUN if [ -n "$ECKIT_VERSION" ]; then sed -i \
    -e "s|PROJECT eckit.*UPDATE)|PROJECT eckit GIT \"https://github.com/ecmwf/eckit\" TAG ${ECKIT_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$ECCODES_VERSION" ]; then sed -i \
    -e "s|PROJECT eccodes.*UPDATE)|PROJECT eccodes GIT \"https://github.com/ecmwf/eccodes\" TAG ${ECCODES_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$ODC_VERSION" ]; then sed -i \
    -e "s|PROJECT odc.*UPDATE)|PROJECT odc GIT \"https://github.com/ecmwf/odc\" TAG ${ODC_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$METKIT_VERSION" ]; then sed -i \
    -e "s|PROJECT metkit.*UPDATE)|PROJECT metkit GIT \"https://github.com/ecmwf/metkit\" TAG ${METKIT_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$ATLAS_VERSION" ]; then sed -i \
    -e "s|PROJECT atlas .*UPDATE)|PROJECT atlas GIT \"https://github.com/ecmwf/atlas\" TAG ${ATLAS_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$ATLAS_ORCA_VERSION" ]; then sed -i \
    -e "s|PROJECT atlas-orca.*UPDATE)|PROJECT atlas-orca GIT \"https://github.com/ecmwf/atlas-orca\" TAG ${ATLAS_ORCA_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$MIR_VERSION" ]; then sed -i \
    -e "s|PROJECT mir .*UPDATE)|PROJECT mir GIT \"https://github.com/ecmwf/mir\" TAG ${MIR_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$FDB_VERSION" ]; then sed -i \
    -e "s|PROJECT fdb5.*UPDATE)|PROJECT fdb5 GIT \"https://github.com/ecmwf/fdb\" TAG ${FDB_VERSION} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi
RUN if [ -n "$MARS_CLIENT_CPP_BRANCH" ]; then sed -i \
    -e "s|PROJECT mars-client-cpp.*|PROJECT mars-client-cpp GIT \"https://github.com/ecmwf/mars-client-cpp\" TAG ${MARS_CLIENT_CPP_BRANCH} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi

RUN mkdir -p /build/mars-client-bundle && cd /build/mars-client-bundle \
    && ecbuild \
        --prefix=/opt/mars-client \
        -DCMAKE_PREFIX_PATH=/opt/mars-client \
        -DENABLE_FORTRAN=OFF \
        -DENABLE_PYTHON=OFF \
        -DENABLE_AEC=ON \
        -DENABLE_POLYTOPE=OFF \
        -DENABLE_BUILD_TOOLS=OFF \
        /source/mars-client-bundle \
    && make -j$(nproc) \
    && make install

FROM mars-build AS mars-libs-true

###############################
# Select library provider
###############################
ARG MARS_BUILD_FROM_SOURCE=false
FROM mars-libs-${MARS_BUILD_FROM_SOURCE} AS mars-libs

###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
COPY workers/common/ workers/common/
COPY workers/mars-worker/ workers/mars-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/mars-worker && cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
COPY --from=mars-libs /opt/mars-client /opt/mars-client
ENV MARS_CLIENT_LIB_DIR=/opt/mars-client/lib
ENV MARS_CLIENT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_INCLUDE_DIR=/opt/mars-client/include
ENV ECKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_INCLUDE_DIR=/opt/mars-client/include
ENV METKIT_BUILD_INCLUDE_DIR=/opt/mars-client/include
COPY --from=chef /ecmwf/polytope-server/workers/mars-worker/recipe.json workers/mars-worker/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
COPY workers/common/ workers/common/
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/mars-worker && cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
ARG PACKAGE_NAME=mars-worker
ARG BIN_NAME=mars-worker
COPY --from=mars-libs /opt/mars-client /opt/mars-client
COPY workers/common/ workers/common/
COPY workers/mars-worker/ workers/mars-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml
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
