# syntax=docker/dockerfile:1.4
##
## Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##


FROM python:3.11-bookworm AS gribjump-source-build-base

ARG ecbuild_version
ARG libaec_version
ARG eckit_version
ARG eccodes_version
ARG metkit_version
ARG fdb_version
ARG gribjump_version

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    curl \
    git \
    liblz4-dev \
    ninja-build \
    python3-dev \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir uv cmake

COPY ./docker/gribjump /env_build

RUN set -eux \
    && mkdir -p /opt/polytope/gribjump-source-src /opt/polytope/gribjump-source-build \
    && PYTHON_VERSION=3.11 \
    SRC_BUNDLE=/opt/polytope/gribjump-source-src \
    BUILD_DIR=/opt/polytope/gribjump-source-build \
    INSTALL_PREFIX=/opt/polytope/gribjump-source \
    ECBUILD_VERSION=${ecbuild_version} \
    LIBAEC_VERSION=${libaec_version} \
    ECKIT_VERSION=${eckit_version} \
    ECCODES_VERSION=${eccodes_version} \
    METKIT_VERSION=${metkit_version} \
    FDB_VERSION=${fdb_version} \
    GRIBJUMP_VERSION=${gribjump_version} \
    bash /env_build/build.sh

## TODO: these can be sourced from profile
ENV FDB5_DIR=/opt/polytope/gribjump-source
ENV GRIBJUMP_DIR=/opt/polytope/gribjump-source
ENV ECCODES_DIR=/opt/polytope/gribjump-source
ENV ECCODES_DEFINITION_PATH=/opt/polytope/gribjump-source/share/eccodes/definitions
ENV ECCODES_SAMPLES_PATH=/opt/polytope/gribjump-source/share/eccodes/samples
ENV FINDLIBS_DISABLE_PACKAGE=yes
ENV LD_LIBRARY_PATH=/opt/polytope/gribjump-source/lib64:/opt/polytope/gribjump-source/lib

RUN /opt/polytope/gribjump-source/.venv/bin/python -c "import pyfdb, pygribjump; print('source bundle imports OK')"

FROM gribjump-source-build-base AS gribjump-source-worker-python

ARG gribjump_version
ARG ecbuild_version
ARG libaec_version
ARG eckit_version
ARG eccodes_version
ARG metkit_version
ARG fdb_version

LABEL org.ecmwf.polytope.gribjump_version="${gribjump_version}" \
    org.ecmwf.polytope.ecbuild_version="${ecbuild_version}" \
    org.ecmwf.polytope.libaec_version="${libaec_version}" \
    org.ecmwf.polytope.eckit_version="${eckit_version}" \
    org.ecmwf.polytope.eccodes_version="${eccodes_version}" \
    org.ecmwf.polytope.metkit_version="${metkit_version}" \
    org.ecmwf.polytope.fdb_version="${fdb_version}"

RUN set -eux \
    && mkdir -p /opt/polytope/gribjump-source-wheels \
    && cp /opt/polytope/gribjump-source-build/pyfdb*.whl /opt/polytope/gribjump-source-wheels/ \
    && VIRTUAL_ENV=/opt/polytope/gribjump-source/.venv PATH="/opt/polytope/gribjump-source/.venv/bin:${PATH}" \
    python -m pip wheel --no-binary eccodes "eccodes==${eccodes_version}" -w /opt/polytope/gribjump-source-wheels \
    && VIRTUAL_ENV=/opt/polytope/gribjump-source/.venv PATH="/opt/polytope/gribjump-source/.venv/bin:${PATH}" \
    python -m pip wheel --no-deps /opt/polytope/gribjump-source-src/gribjump -w /opt/polytope/gribjump-source-wheels
