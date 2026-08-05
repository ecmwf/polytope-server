# check=skip=SecretsUsedInArgOrEnv
# ^ GIT_AUTH_TOKEN arrives as a build arg because skaffold passes it via
#   buildArgs (same pattern as the app Dockerfiles); it only exists in the
#   discarded mars-build stage, never in a published image.

# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

# MARS client library image for the mars-worker.
# Published as eccr.ecmwf.int/polytope/cpp-mars-libs:<tag> by the cpp-libs
# workflow; consumed via `FROM ${MARS_LIBS_IMAGE}`.
#
# Two providers, selected with --build-arg MARS_BUILD_FROM_SOURCE=<true|false>:
#   true  (default) — build the mars-client-bundle from source
#   false           — install the pre-built mars-client-cpp package from Nexus
#                     (requires --build-arg rpm_repo=<Nexus repository URL>)

ARG MARS_BUILD_FROM_SOURCE=true

###############################
# RPM-based MARS client library
###############################
FROM python:3.11-bookworm AS mars-base
ARG rpm_repo

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gpg \
    && mkdir -p /usr/share/keyrings \
    && curl -fsSL "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" \
      | gpg --dearmor -o /usr/share/keyrings/ecmwf-stable.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/ecmwf-stable.gpg] ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" \
      > /etc/apt/sources.list.d/ecmwf-stable.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends libnetcdf19 liblapack3 \
    && rm -rf /var/lib/apt/lists/*

FROM mars-base AS mars-base-cpp
ARG mars_client_cpp_version=7.1.9.1
RUN apt-get update && apt-get install -y mars-client-cpp=${mars_client_cpp_version} && rm -rf /var/lib/apt/lists/*

FROM mars-base-cpp AS mars-libs-false
RUN mkdir -p /opt/mars-client && cp -a /opt/ecmwf/mars-client-cpp/. /opt/mars-client/

###############################
# Source-built MARS client library
###############################
FROM docker.io/library/debian:bookworm-slim AS mars-build
ARG GIT_AUTH_TOKEN=""
ARG MARS_CLIENT_BUNDLE_REF=5d0430a5a1793274118254bbb15a5be824a063ba
ARG MARS_CLIENT_CPP_REF=d0670436490d1c62e916a04fc15d3291b98027e7
# All other dependency versions come from the immutable bundle reference above;
# only the downstream Rust-enabled mars-client-cpp source is overridden.

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git bison flex net-tools \
    libssl-dev libnetcdf-dev liblz4-dev curl ca-certificates \
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
RUN git init /source/mars-client-bundle \
    && git -C /source/mars-client-bundle remote add origin https://github.com/ecmwf/mars-client-bundle.git \
    && git -C /source/mars-client-bundle fetch --depth 1 origin "${MARS_CLIENT_BUNDLE_REF}" \
    && git -C /source/mars-client-bundle checkout --detach FETCH_HEAD

RUN if [ -n "$MARS_CLIENT_CPP_REF" ]; then sed -i \
    -e "s|PROJECT mars-client-cpp.*|PROJECT mars-client-cpp GIT \"https://github.com/ecmwf/mars-client-cpp\" TAG ${MARS_CLIENT_CPP_REF} UPDATE)|" \
    /source/mars-client-bundle/CMakeLists.txt; fi

RUN mkdir -p /build/mars-client-bundle && cd /build/mars-client-bundle \
    && ecbuild \
        --prefix=/opt/mars-client \
        -DCMAKE_PREFIX_PATH=/opt/mars-client \
        -DENABLE_FORTRAN=OFF \
        -DENABLE_PYTHON=OFF \
        -DENABLE_AEC=ON \
        -DENABLE_LZ4=ON \
        -DENABLE_POLYTOPE=OFF \
        -DENABLE_BUILD_TOOLS=OFF \
        /source/mars-client-bundle \
    && make -j$(nproc) \
    && make install

FROM mars-build AS mars-libs-true

###############################
# Select library provider
###############################
ARG MARS_BUILD_FROM_SOURCE=true
FROM mars-libs-${MARS_BUILD_FROM_SOURCE} AS mars-libs

###############################
# Strip debug symbols (binutils is only needed in this throwaway stage)
###############################
FROM docker.io/library/debian:bookworm-slim AS strip-libs
RUN apt-get update && apt-get install -y --no-install-recommends binutils file && rm -rf /var/lib/apt/lists/*
COPY --from=mars-libs /opt/mars-client /opt/mars-client
# Strip failures are real errors; only a missing bin/ or non-ELF files there
# (e.g. the mars wrapper scripts) are tolerated.
RUN find /opt/mars-client -type f -name '*.so*' -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-unneeded \
    && find /opt/mars-client -type f -name '*.a' -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-debug \
    && { [ ! -d /opt/mars-client/bin ] || find /opt/mars-client/bin -type f -exec file {} + \
         | awk -F': ' '/ELF/{print $1}' | xargs -r strip --strip-all; }

###############################
# Debug image: unstripped prefix (full symbols). Built by the `libs-debug`
# target and published as the `-debug` companion tag.
###############################
FROM docker.io/library/debian:bookworm-slim AS libs-debug
ARG VERSION
LABEL version=$VERSION
COPY --from=mars-libs /opt/mars-client /opt/mars-client

###############################
# Library image (default): stripped prefix
###############################
FROM docker.io/library/debian:bookworm-slim AS libs
ARG VERSION
LABEL version=$VERSION
COPY --from=strip-libs /opt/mars-client /opt/mars-client
