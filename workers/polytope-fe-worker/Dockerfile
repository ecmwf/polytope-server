# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

###############################
# Shared base: Rust + cargo-chef + system deps (built once)
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*

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
ARG PACKAGE_NAME=polytope-fe-worker
WORKDIR /ecmwf/polytope-server
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo chef cook --release --locked --package "${PACKAGE_NAME}" --recipe-path recipe.json

###############################
# Build the package from the root workspace
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
WORKDIR /ecmwf/polytope-server
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo build --release --locked --package "${PACKAGE_NAME}"

###############################
# FDB Build (from source)
###############################
FROM docker.io/library/debian:bookworm-slim AS fdb-build
ARG ecbuild_version=3.12.0
ARG eccodes_version=2.46.0
ARG eckit_version=1.33.2
ARG metkit_version=1.18.1
ARG fdb_version=5.21.3
ARG gribjump_version=0.12.0

RUN apt-get update && apt-get install -y --no-install-recommends cmake build-essential bison flex git libaec-dev ca-certificates && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${ecbuild_version} https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

RUN git clone --depth 1 --branch ${eckit_version} https://github.com/ecmwf/eckit.git /source/eckit \
    && mkdir -p /build/eckit && cd /build/eckit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/eckit \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch v1.1.3 https://github.com/Deutsches-Klimarechenzentrum/libaec.git /source/libaec \
    && mkdir -p /build/libaec && cd /build/libaec \
    && cmake -DCMAKE_INSTALL_PREFIX=/opt/fdb /source/libaec \
    && make -j$(nproc) && make install \
    && rm -rf /source/libaec /build/libaec

RUN git clone --depth 1 --branch ${eccodes_version} https://github.com/ecmwf/eccodes.git /source/eccodes \
    && mkdir -p /build/eccodes && cd /build/eccodes \
    && ecbuild --prefix=/opt/fdb -- -DENABLE_FORTRAN=OFF -DCMAKE_PREFIX_PATH=/opt/fdb /source/eccodes \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch ${metkit_version} https://github.com/ecmwf/metkit.git /source/metkit \
    && mkdir -p /build/metkit && cd /build/metkit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb -DENABLE_TESTS=OFF -DENABLE_BUILD_TOOLS=OFF -DENABLE_MARS2GRIB=OFF /source/metkit \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch ${fdb_version} https://github.com/ecmwf/fdb.git /source/fdb \
    && mkdir -p /build/fdb && cd /build/fdb \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH="/opt/fdb;/opt/fdb/eckit;/opt/fdb/metkit" -DENABLE_TESTS=OFF -DCMAKE_CXX_FLAGS=-fpermissive /source/fdb \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch ${gribjump_version} https://github.com/ecmwf/gribjump.git /source/gribjump \
    && mkdir -p /build/gribjump && cd /build/gribjump \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb -DENABLE_TESTS=OFF /source/gribjump \
    && make -j$(nproc) && make install

RUN rm -rf /source /build




###############################
# Python Requirements
###############################
FROM python:3.11-slim-bookworm AS python-deps
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir uv
RUN uv venv /opt/venv
COPY workers/polytope-fe-worker/requirements.txt /requirements.txt
RUN uv pip install --python /opt/venv/bin/python -r /requirements.txt

###############################
# Stage 4: Production Release Image
###############################
FROM python:3.11-slim-bookworm AS release
ARG VERSION
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
LABEL version=$VERSION

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=fdb-build /opt/fdb/ /opt/fdb/
COPY --from=python-deps /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH=/opt/venv/lib/python3.11/site-packages
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/fdb/lib:/usr/local/lib

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
ENTRYPOINT ["/app/polytope-fe-worker"]

###############################
# Stage 5: Debug Image
###############################
FROM python:3.11-slim-bookworm AS debug
ARG VERSION
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
LABEL version=$VERSION

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libaec0 libcurl4 libopenjp2-7 bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=fdb-build /opt/fdb/ /opt/fdb/
COPY --from=python-deps /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH=/opt/venv/lib/python3.11/site-packages
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/fdb/lib:/usr/local/lib

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
ENTRYPOINT ["/app/polytope-fe-worker"]
