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
## In applying this licence, ECMWF does not waive the privileges and immunities
## granted to it by virtue of its status as an intergovernmental organisation nor
## does it submit to any jurisdiction.
##

ARG mars_client_c_version=6.34.4.11
ARG mars_client_cpp_version=7.1.9.1
ARG gribjump_version=0.12.0
ARG image_repo=
ARG gribjump_base=${image_repo}gribjump-base:${gribjump_version}
ARG worker_mars_c_mode=image
ARG worker_mars_c_image=${image_repo}mars-base-c:${mars_client_c_version}
ARG worker_mars_cpp_mode=image
ARG worker_mars_cpp_image=${image_repo}mars-base-cpp:${mars_client_cpp_version}
ARG worker_gribjump_mode=image

#######################################################
#           P Y T H O N   W H E E L H O U S E
#      Build/download all wheels from requirements
#######################################################

FROM python:3.11-bookworm AS polytope-requirements-wheel-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libldap2-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/root/.cargo
ENV RUSTUP_HOME=/root/.rustup
ENV PATH=/root/.cargo/bin:${PATH}

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable

RUN python -m pip install --no-cache-dir --upgrade pip wheel

WORKDIR /build/polytope-server
COPY ./requirements.txt .
RUN GIT_TERMINAL_PROMPT=0 python -m pip wheel -r requirements.txt -w /wheels

#######################################################
#                     C O M M O N
#             based on python bookworm slim
#######################################################

FROM python:3.11-slim-bookworm AS polytope-common

ARG HOME_DIR=/home/polytope
ARG developer_mode

# Install build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends bash gcc libc6-dev libldap2-dev curl git \
    && rm -rf /var/lib/apt/lists/*

# Create user and group
RUN set -eux \
    && addgroup --system polytope --gid 474 \
    && adduser --system polytope --ingroup polytope --home ${HOME_DIR} \
    && mkdir -p /polytope \
    && chown -R polytope:polytope ${HOME_DIR} /polytope

# Switch to user polytope
USER polytope

WORKDIR /polytope

# Copy requirements.txt with correct ownership
COPY --chown=polytope:polytope ./requirements.txt ./requirements.txt
COPY --from=polytope-requirements-wheel-builder --chown=polytope:polytope /wheels ${HOME_DIR}/wheels

# Use one application virtual environment.
ENV VIRTUAL_ENV=${HOME_DIR}/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Install every wheel produced from requirements.txt. This avoids re-resolving
# direct git requirements in the runtime stage.
RUN python -m venv ${VIRTUAL_ENV} \
    && pip install --no-index --find-links=${HOME_DIR}/wheels ${HOME_DIR}/wheels/*.whl \
    && rm -rf ${HOME_DIR}/wheels

# Copy the rest of the application code
COPY --chown=polytope:polytope . .

RUN set -eux \
    && if [ "${developer_mode}" = true ]; then \
    pip install ./polytope-mars ./polytope ./covjsonkit; \
    fi

# Install the application. Dependencies are already installed from requirements.txt.
RUN pip install --no-deps --upgrade .


#######################################################
#                N O O P   I M A G E
#######################################################
FROM python:3.11-bookworm AS blank-base
# create blank directories to copy from in the final stage, optional dependencies aren't built
RUN set -eux \
    && mkdir -p /opt/ecmwf/mars-client \
    && mkdir -p /opt/ecmwf/mars-client-cpp \
    && mkdir -p /opt/polytope/gribjump-source \
    && mkdir -p /opt/polytope/gribjump-source-wheels \
    && touch /usr/local/bin/mars

#######################################################
#               M A R S    B A S E
#######################################################
FROM python:3.11-bookworm AS mars-base
ARG rpm_repo
ARG mars_client_cpp_version
ARG mars_client_c_version

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN response=$(curl -s -w "%{http_code}" ${rpm_repo}) \
    && if [ "$response" = "403" ]; then echo "Unauthorized access to ${rpm_repo} "; fi

RUN set -eux \
    && curl -o /tmp/stable-public.gpg.key "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" \
    && gpg --dearmor -o /usr/share/keyrings/mars-client.gpg /tmp/stable-public.gpg.key \
    && echo "deb [signed-by=/usr/share/keyrings/mars-client.gpg] ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" > /etc/apt/sources.list.d/mars-client.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends libnetcdf19 liblapack3 \
    && rm -rf /var/lib/apt/lists/* /tmp/stable-public.gpg.key

FROM mars-base AS mars-rpm-c
RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends liblapack3 mars-client=${mars_client_c_version} \
    && rm -rf /var/lib/apt/lists/* \
    && if [ -x /opt/ecmwf/mars-client/bin/mars.bin ] && [ ! -e /opt/ecmwf/mars-client/bin/mars ]; then ln -s mars.bin /opt/ecmwf/mars-client/bin/mars; fi

FROM mars-base AS mars-cloud-wrapper
RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends mars-client-cloud=0.2.1 \
    && rm -rf /var/lib/apt/lists/* \
    && test -x /usr/local/bin/mars

FROM mars-base AS mars-rpm-cpp
RUN apt-get update \
    && apt-get install -y --no-install-recommends mars-client-cpp=${mars_client_cpp_version} \
    && rm -rf /var/lib/apt/lists/*

FROM mars-rpm-c AS mars-rpm-c-with-wrapper
COPY --from=mars-cloud-wrapper /usr/local/bin/mars /usr/local/bin/mars


FROM blank-base AS worker-mars-c-off
FROM mars-rpm-c-with-wrapper AS worker-mars-c-rpm
FROM ${worker_mars_c_image} AS worker-mars-c-image
FROM worker-mars-c-${worker_mars_c_mode} AS worker-mars-c-final

FROM blank-base AS worker-mars-cpp-off
FROM mars-rpm-cpp AS worker-mars-cpp-rpm
FROM ${worker_mars_cpp_image} AS worker-mars-cpp-image
FROM worker-mars-cpp-${worker_mars_cpp_mode} AS worker-mars-cpp-final

FROM blank-base AS worker-gribjump-off
FROM ${gribjump_base} AS worker-gribjump-image
FROM worker-gribjump-${worker_gribjump_mode} AS worker-gribjump-final


#######################################################
#                    W O R K E R
#               based on debian bookworm
#######################################################

FROM polytope-common AS worker

ARG mars_config_branch
ARG mars_config_repo
ARG rpm_repo
ARG worker_mars_c_mode
ARG worker_mars_cpp_mode
ARG worker_gribjump_mode

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    bash \
    curl \
    git \
    libfftw3-bin \
    libgomp1 \
    liblapack3 \
    libnetcdf19 \
    libopenjp2-7 \
    libproj25 \
    ssh \
    vim \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux \
    && case "${worker_mars_c_mode}" in off|rpm|image) ;; *) echo "Invalid worker_mars_c_mode: ${worker_mars_c_mode}. Expected off, rpm, or image." >&2; exit 1 ;; esac \
    && case "${worker_mars_cpp_mode}" in off|rpm|image) ;; *) echo "Invalid worker_mars_cpp_mode: ${worker_mars_cpp_mode}. Expected off, rpm, or image." >&2; exit 1 ;; esac \
    && case "${worker_gribjump_mode}" in off|image) ;; *) echo "Invalid worker_gribjump_mode: ${worker_gribjump_mode}. Expected off, rpm, or image." >&2; exit 1 ;; esac

WORKDIR /polytope

# Copy MARS-related artifacts
COPY --chown=polytope --from=worker-mars-cpp-final /opt/ecmwf/mars-client-cpp  /opt/ecmwf/mars-client-cpp
COPY --chown=polytope --from=worker-mars-c-final   /opt/ecmwf/mars-client      /opt/ecmwf/mars-client
COPY --chown=polytope --from=worker-mars-c-final   /usr/local/bin/mars         /usr/local/bin/mars

# all of this is needed by the C client, would be nice to remove it at some point
RUN set -eux \
    && mkdir -p /home/polytope/.ssh \
    && chmod 0700 /home/polytope/.ssh \
    && ssh-keyscan git.ecmwf.int > /home/polytope/.ssh/known_hosts \
    && mkdir -p /home/polytope/data \
    && chown -R polytope:polytope /home/polytope

ENV MARS_CONFIGS_REPO=${mars_config_repo}
ENV MARS_CONFIGS_BRANCH=${mars_config_branch}
ENV PATH="/home/polytope/.venv/bin:/opt/ecmwf/mars-client/bin:${PATH}"
ENV FINDLIBS_DISABLE_PACKAGE=yes
ENV FDB5_DIR=/opt/polytope/gribjump-source
ENV GRIBJUMP_DIR=/opt/polytope/gribjump-source
ENV ECCODES_DIR=/opt/polytope/gribjump-source

# Copy gribjump-related artifacts
COPY --chown=polytope --from=worker-gribjump-final /opt/polytope/gribjump-source /opt/polytope/gribjump-source
COPY --chown=polytope --from=worker-gribjump-final /opt/polytope/gribjump-source-wheels /tmp/gribjump-source-wheels

RUN set -eux \
    && if ls /tmp/gribjump-source-wheels/*.whl >/dev/null 2>&1; then \
    /home/polytope/.venv/bin/pip install --no-index --find-links=/tmp/gribjump-source-wheels /tmp/gribjump-source-wheels/*.whl; \
    fi \
    && rm -rf /tmp/gribjump-source-wheels /opt/polytope/gribjump-source/.venv

RUN set -eux \
    && mkdir -p /opt/polytope/gribjump-source/lib64 /opt/polytope/gribjump-source/lib \
    && printf '%s\n' /opt/polytope/gribjump-source/lib64 /opt/polytope/gribjump-source/lib > /etc/ld.so.conf.d/polytope-gribjump.conf \
    && ldconfig

RUN set -eux \
    && if [ -f /opt/polytope/gribjump-source/profile ]; then \
    /home/polytope/.venv/bin/python -c "import eccodes, pyfdb, pygribjump; print('worker source bundle imports OK')"; \
    fi

USER polytope
