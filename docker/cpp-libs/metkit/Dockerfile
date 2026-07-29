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
# Library image: installed prefix only
###############################
FROM docker.io/library/debian:bookworm-slim AS libs
ARG VERSION
LABEL version=$VERSION
COPY --from=metkit-build /opt/metkit /opt/metkit
