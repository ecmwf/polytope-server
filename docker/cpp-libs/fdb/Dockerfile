# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# FDB library stack (eckit, eccodes, metkit, fdb) for the fdb-worker.
# Published as eccr.ecmwf.int/polytope/cpp-fdb-libs:<tag> by the
# cpp-libs workflow; consumed via `FROM ${FDB_LIBS_IMAGE}`.

###############################
# FDB Build (from source)
###############################
FROM docker.io/library/debian:bookworm-slim AS fdb-build
ARG ecbuild_version=3.12.0
ARG eccodes_version=2.46.0
ARG eckit_version=1.33.0
ARG metkit_version=1.17.0
ARG fdb_version=5.19.2

RUN apt-get update && apt-get install -y --no-install-recommends cmake build-essential bison flex git libaec-dev ca-certificates && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${ecbuild_version} https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

RUN git clone --depth 1 --branch ${eckit_version} https://github.com/ecmwf/eckit.git /source/eckit \
    && mkdir -p /build/eckit && cd /build/eckit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/eckit \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch ${eccodes_version} https://github.com/ecmwf/eccodes.git /source/eccodes \
    && mkdir -p /build/eccodes && cd /build/eccodes \
    && ecbuild --prefix=/opt/fdb -- -DENABLE_FORTRAN=OFF -DENABLE_AEC=OFF -DCMAKE_PREFIX_PATH=/opt/fdb /source/eccodes \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch ${metkit_version} https://github.com/ecmwf/metkit.git /source/metkit \
    && mkdir -p /build/metkit && cd /build/metkit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/metkit \
    && make -j$(nproc) && make install

RUN git clone --depth 1 --branch ${fdb_version} https://github.com/ecmwf/fdb.git /source/fdb \
    && mkdir -p /build/fdb && cd /build/fdb \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH="/opt/fdb;/opt/fdb/eckit;/opt/fdb/metkit" /source/fdb \
    && make -j$(nproc) && make install

###############################
# Strip debug symbols (binutils is only needed in this throwaway stage)
###############################
FROM docker.io/library/debian:bookworm-slim AS strip-libs
RUN apt-get update && apt-get install -y --no-install-recommends binutils file && rm -rf /var/lib/apt/lists/*
COPY --from=fdb-build /opt/fdb /opt/fdb
# Strip failures are real errors; only a missing bin/ or non-ELF files there are tolerated.
RUN find /opt/fdb -type f -name '*.so*' -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-unneeded \
    && find /opt/fdb -type f -name '*.a' -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-debug \
    && { [ ! -d /opt/fdb/bin ] || find /opt/fdb/bin -type f -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-all; }

###############################
# Debug image: unstripped prefix (full symbols, ~3x larger). Built by the
# `libs-debug` target and published as the `-debug` companion tag.
###############################
FROM docker.io/library/debian:bookworm-slim AS libs-debug
ARG VERSION
LABEL version=$VERSION
COPY --from=fdb-build /opt/fdb /opt/fdb

###############################
# Library image (default): stripped prefix
###############################
FROM docker.io/library/debian:bookworm-slim AS libs
ARG VERSION
LABEL version=$VERSION
COPY --from=strip-libs /opt/fdb /opt/fdb
