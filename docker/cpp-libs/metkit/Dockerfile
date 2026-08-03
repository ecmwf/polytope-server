# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# eckit + metkit library image for the polytope-server frontend.
# Published as eccr.ecmwf.int/polytope/cpp-metkit-libs:<tag> by the
# cpp-libs workflow; consumed via `FROM ${METKIT_LIBS_IMAGE}`.

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
# Strip debug symbols (binutils is only needed in this throwaway stage)
###############################
FROM docker.io/library/debian:bookworm-slim AS strip-libs
RUN apt-get update && apt-get install -y --no-install-recommends binutils file && rm -rf /var/lib/apt/lists/*
COPY --from=metkit-build /opt/metkit /opt/metkit
# Strip failures are real errors; only a missing bin/ or non-ELF files there are tolerated.
RUN find /opt/metkit -type f -name '*.so*' -exec strip --strip-unneeded {} + \
    && find /opt/metkit -type f -name '*.a' -exec strip --strip-debug {} + \
    && { [ ! -d /opt/metkit/bin ] || find /opt/metkit/bin -type f -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-all; }

###############################
# Debug image: unstripped prefix (full symbols). Built by the `libs-debug`
# target and published as the `-debug` companion tag.
###############################
FROM docker.io/library/debian:bookworm-slim AS libs-debug
ARG VERSION
LABEL version=$VERSION
COPY --from=metkit-build /opt/metkit /opt/metkit

###############################
# Library image (default): stripped prefix
###############################
FROM docker.io/library/debian:bookworm-slim AS libs
ARG VERSION
LABEL version=$VERSION
COPY --from=strip-libs /opt/metkit /opt/metkit
