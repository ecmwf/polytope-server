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

ARG fdb_base=blank-base
ARG mars_base_c=blank-base
ARG mars_base_cpp=blank-base
ARG gribjump_base=blank-base
ARG gribjump_source_base=blank-base
ARG polytope_python_git=https://github.com/ecmwf/polytope.git
ARG polytope_python_ref=feat/new-pyfdb-api

#######################################################
#                     C O M M O N
#             based on python bookworm slim
#######################################################

FROM python:3.11-slim-bookworm AS polytope-common

ARG HOME_DIR=/home/polytope
ARG developer_mode

# Install build dependencies
RUN apt update && apt install -y --no-install-recommends gcc libc6-dev libldap2-dev curl git \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

# Create user and group
RUN set -eux \
    && addgroup --system polytope --gid 474 \
    && adduser --system polytope --ingroup polytope --home ${HOME_DIR} \
    && mkdir -p ${HOME_DIR}/polytope-server \
    && chown -R polytope:polytope ${HOME_DIR}

# Switch to user polytope
USER polytope

WORKDIR ${HOME_DIR}/polytope-server

# Copy requirements.txt with correct ownership
COPY --chown=polytope:polytope ./requirements.txt $PWD

# Install uv in user space
RUN pip install --user uv

# **Update PATH to include virtual environment and user local bin**
# This makes sure that the default python and pip commands
# point to the versions in the virtual environment.
ENV PATH="${HOME_DIR}/.venv/bin:${HOME_DIR}/.local/bin:${PATH}"

# Create a virtual environment
RUN uv venv ${HOME_DIR}/.venv

# Install requirements
RUN uv pip install -r requirements.txt

# Copy the rest of the application code
COPY --chown=polytope:polytope . $PWD

RUN set -eux \
    && if [ $developer_mode = true ]; then \
    uv pip install ./polytope-mars ./polytope ./covjsonkit; \
    fi

# Install the application
RUN uv pip install --upgrade .


#######################################################
#                N O O P   I M A G E
#######################################################
FROM python:3.11-bookworm AS blank-base
# create blank directories to copy from in the final stage, optional dependencies aren't built
RUN set -eux \
    && mkdir -p /root/.local \
    && mkdir -p /opt/ecmwf/mars-client \
    && mkdir -p /opt/ecmwf/mars-client-cpp \
    && mkdir -p /opt/ecmwf/mars-client-cloud \
    && mkdir -p /opt/ecmwf/gribjump-server \
    && mkdir -p /opt/polytope/gribjump-source \
    && touch /usr/local/bin/mars

#######################################################
#             G R I B  J U M P   R P M
#######################################################

FROM python:3.11-bookworm AS gribjump-base
ARG rpm_repo
ARG gribjump_version=0.10.0

RUN response=$(curl -s -w "%{http_code}" ${rpm_repo}) \
    && if [ "$response" = "403" ]; then echo "Unauthorized access to ${rpm_repo} "; fi

RUN set -eux \
    && apt-get update \
    && apt-get install -y gnupg2 curl ca-certificates \
    && curl -fsSL "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" | gpg --dearmor -o /usr/share/keyrings/mars-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/mars-archive-keyring.gpg] ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" | tee /etc/apt/sources.list.d/mars.list

RUN set -eux \
    && apt-get update \
    && apt install -y gribjump-server=${gribjump_version}-gribjumpserver

RUN set -eux \
    ls -R /opt

RUN set -eux \
    && git clone --single-branch --branch ${gribjump_version} https://github.com/ecmwf/gribjump.git
# Install pygribjump
RUN set -eux \
    && cd /gribjump \
    && python -m pip install . --user \
    && rm -rf /gribjump

FROM python:3.11-bookworm AS source-gribjump-base

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    liblz4-dev \
    ninja-build \
    python3-dev \
    python3-venv \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir uv cmake

COPY ./env_build /env_build

RUN set -eux \
    && mkdir -p /opt/polytope/gribjump-source-src /opt/polytope/gribjump-source-build \
    && PYTHON_VERSION=3.11 \
    SRC_BUNDLE=/opt/polytope/gribjump-source-src \
    BUILD_DIR=/opt/polytope/gribjump-source-build \
    INSTALL_PREFIX=/opt/polytope/gribjump-source \
    bash /env_build/build.sh

ENV FDB5_DIR=/opt/polytope/gribjump-source
ENV GRIBJUMP_DIR=/opt/polytope/gribjump-source
ENV ECCODES_DIR=/opt/polytope/gribjump-source
ENV ECCODES_DEFINITION_PATH=/opt/polytope/gribjump-source/share/eccodes/definitions
ENV ECCODES_SAMPLES_PATH=/opt/polytope/gribjump-source/share/eccodes/samples
ENV FINDLIBS_DISABLE_PACKAGE=yes
ENV LD_LIBRARY_PATH=/opt/polytope/gribjump-source/lib

RUN /opt/polytope/gribjump-source/.venv/bin/python -c "import pyfdb, pygribjump; print('source bundle imports OK')"

FROM python:3.11-bookworm AS polytope-python-wheel-builder

ARG polytope_python_git
ARG polytope_python_ref

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/root/.cargo
ENV RUSTUP_HOME=/root/.rustup
ENV PATH=/root/.cargo/bin:${PATH}

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable

RUN python -m pip install --no-cache-dir build uv

WORKDIR /build/polytope-python
RUN GIT_TERMINAL_PROMPT=0 git clone --branch "${polytope_python_ref}" --depth 1 "${polytope_python_git}" .
RUN python -m build --wheel

FROM source-gribjump-base AS source-gribjump-worker-python

COPY --from=polytope-python-wheel-builder /build/polytope-python/dist/*.whl /tmp/polytope-python/

RUN VIRTUAL_ENV=/opt/polytope/gribjump-source/.venv PATH="/opt/polytope/gribjump-source/.venv/bin:${PATH}" uv pip install --no-binary eccodes "eccodes>=2.45" \
    && VIRTUAL_ENV=/opt/polytope/gribjump-source/.venv PATH="/opt/polytope/gribjump-source/.venv/bin:${PATH}" uv pip install /tmp/polytope-python/*.whl \
    && VIRTUAL_ENV=/opt/polytope/gribjump-source/.venv PATH="/opt/polytope/gribjump-source/.venv/bin:${PATH}" python -c "import eccodes, pyfdb, pygribjump; print('source worker imports OK')"

#######################################################
#               M A R S    B A S E
#######################################################
FROM python:3.11-bookworm AS mars-base
ARG rpm_repo
ARG mars_client_cpp_version=6.99.3.0
ARG mars_client_c_version=6.33.20.2

RUN response=$(curl -s -w "%{http_code}" ${rpm_repo}) \
    && if [ "$response" = "403" ]; then echo "Unauthorized access to ${rpm_repo} "; fi

RUN set -eux \
    && curl -o stable-public.gpg.key "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" \
    && echo "deb ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" >> /etc/apt/sources.list \
    && apt-key add stable-public.gpg.key \
    && apt-get update \
    && apt install -y libnetcdf19 liblapack3

FROM mars-base AS mars-base-c
RUN apt update && apt install -y liblapack3 mars-client=${mars_client_c_version} mars-client-cloud

FROM mars-base AS mars-base-cpp
ARG pyfdb_version=0.1.0
RUN apt update && apt install -y mars-client-cpp=${mars_client_cpp_version}

FROM blank-base AS blank-base-c
FROM blank-base AS blank-base-cpp

#######################################################
#         S W I T C H   B A S E    I M A G E S
#######################################################

FROM ${mars_base_c} AS mars-c-base-final

FROM ${mars_base_cpp} AS mars-cpp-base-final

FROM ${gribjump_base} AS gribjump-base-final

FROM ${gribjump_source_base} AS source-gribjump-base-final


#######################################################
#           P Y T H O N   R E Q U I R E M E N T S
#######################################################
FROM python:3.11-slim-bookworm AS worker-base
ARG developer_mode

# contains compilers for building wheels which we don't want in the final image
RUN apt update
RUN apt-get install -y --no-install-recommends gcc libc6-dev make gnupg2 git

COPY ./requirements.txt /requirements.txt
RUN pip install uv --user
ENV PATH="/root/.venv/bin:/root/.local/bin:${PATH}"
ENV VIRTUAL_ENV=/root/.venv
RUN uv venv /root/.venv
RUN uv pip install -r requirements.txt

COPY . ./polytope
RUN set -eux \
    && if [ $developer_mode = true ]; then \
    uv pip install ./polytope/polytope-mars ./polytope/polytope ./polytope/covjsonkit; \
    fi

#######################################################
#                    W O R K E R
#               based on debian bookworm
#######################################################

FROM python:3.11-slim-bookworm AS worker

ARG mars_config_branch
ARG mars_config_repo
ARG rpm_repo


RUN set -eux \
    && addgroup --system polytope --gid 474 \
    && adduser --system polytope --ingroup polytope --home /home/polytope \
    && mkdir /polytope && chmod -R o+rw /polytope

RUN apt update \
    && apt install -y curl nano sudo ssh libgomp1 vim

# Add polytope user to passwordless sudo group during build
RUN usermod -aG sudo polytope
RUN echo "%sudo  ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

WORKDIR /polytope
USER polytope


# Copy MARS-related artifacts
COPY --chown=polytope --from=mars-cpp-base-final   /opt/ecmwf/mars-client-cpp  /opt/ecmwf/mars-client-cpp
COPY --chown=polytope --from=mars-c-base-final     /opt/ecmwf/mars-client      /opt/ecmwf/mars-client
COPY --chown=polytope --from=mars-c-base-final     /usr/local/bin/mars      /usr/local/bin/mars
RUN sudo apt update \
    && sudo apt install -y libgomp1 git libnetcdf19 liblapack3  libfftw3-bin libproj25 \
    && sudo rm -rf /var/lib/apt/lists/*


# all of this is needed by the C client, would be nice to remove it at some point
RUN set -eux \
    && mkdir -p /home/polytope/.ssh \
    && chmod 0700 /home/polytope/.ssh \
    && ssh-keyscan git.ecmwf.int > /home/polytope/.ssh/known_hosts

ENV MARS_CONFIGS_REPO=${mars_config_repo}
ENV MARS_CONFIGS_BRANCH=${mars_config_branch}
ENV PATH="/opt/polytope/gribjump-source/.venv/bin:/home/polytope/.local/bin:/polytope/bin/:/opt/ecmwf/mars-client/bin:/opt/ecmwf/mars-client-cloud/bin:${PATH}"
ENV PYTHONPATH="/opt/polytope/gribjump-source/.venv/lib/python3.11/site-packages:/home/polytope/.local/lib/python3.11/site-packages"

# Copy gribjump-related artifacts
# COPY --chown=polytope --from=gribjump-base-final /opt/fdb/ /opt/fdb/
COPY --chown=polytope --from=gribjump-base-final /opt/ecmwf/gribjump-server/ /opt/ecmwf/gribjump-server/
COPY --chown=polytope --from=source-gribjump-base-final /opt/polytope/gribjump-source /opt/polytope/gribjump-source
RUN sudo apt update \
    && sudo apt install -y libopenjp2-7 \
    && sudo rm -rf /var/lib/apt/lists/*
# COPY polytope-deployment/common/default_fdb_schema /polytope/config/fdb/default

RUN --mount=from=gribjump-base-final,source=/root/.local,target=/tmp/gribjump-local,readonly \
    --mount=from=worker-base,source=/root/.venv,target=/tmp/worker-venv,readonly \
    set -eux \
    && if [ ! -d /opt/polytope/gribjump-source/.venv ]; then \
    sudo mkdir -p /home/polytope/.local; \
    sudo cp -a /tmp/gribjump-local/. /home/polytope/.local/; \
    sudo cp -a /tmp/worker-venv/. /home/polytope/.local/; \
    fi

# Install the server source
COPY --chown=polytope . /polytope/

RUN set -eux \
    && mkdir /home/polytope/data

# Remove itself from sudo group
RUN sudo deluser polytope sudo
