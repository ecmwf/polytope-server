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

#######################################################
#                     C O M M O N
#             based on python bookworm slim
#######################################################

FROM python:3.11-slim-bookworm AS polytope-common

ARG HOME_DIR=/home/polytope
ARG developer_mode

# Install build dependencies
RUN apt update && apt install -y --no-install-recommends gcc libc6-dev libldap2-dev curl \
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
    && mkdir -p /opt/fdb \
    && mkdir -p /opt/fdb-gribjump \
    && touch /usr/local/bin/mars

#######################################################
#                  F D B   B U I L D
#######################################################
FROM python:3.11-bookworm AS fdb-base
ARG ecbuild_version=3.8.2
ARG eccodes_version=2.33.1
ARG eckit_version=1.28.0
ARG fdb_version=5.13.2
ARG pyfdb_version=0.1.0
RUN apt update
# COPY polytope-deployment/common/default_fdb_schema /polytope/config/fdb/default

# Install FDB from open source repositories
RUN set -eux && \
    apt install -y cmake gnupg build-essential libtinfo5 net-tools libnetcdf19 libnetcdf-dev bison flex && \
    rm -rf source && \
    rm -rf build && \
    mkdir -p source && \
    mkdir -p build && \
    mkdir -p /opt/fdb/

# Download ecbuild
RUN set -eux && \
    git clone --depth 1 --branch ${ecbuild_version} https://github.com/ecmwf/ecbuild.git /ecbuild

ENV PATH=/ecbuild/bin:$PATH

# Install eckit
RUN set -eux && \
    git clone --depth 1 --branch ${eckit_version} https://github.com/ecmwf/eckit.git /source/eckit && \
    cd /source/eckit && \
    mkdir -p /build/eckit && \
    cd /build/eckit && \
    ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/eckit && \
    make -j4 && \
    make install

# Install eccodes
RUN set -eux && \
    git clone --depth 1 --branch ${eccodes_version} https://github.com/ecmwf/eccodes.git /source/eccodes && \
    mkdir -p /build/eccodes && \
    cd /build/eccodes && \
    ecbuild --prefix=/opt/fdb -- -DENABLE_FORTRAN=OFF -DCMAKE_PREFIX_PATH=/opt/fdb /source/eccodes && \
    make -j4 && \
    make install

# Install metkit
RUN set -eux && \
    git clone --depth 1 --branch develop https://github.com/ecmwf/metkit.git /source/metkit && \
    cd /source/metkit && \
    mkdir -p /build/metkit && \
    cd /build/metkit && \
    ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/metkit && \
    make -j4 && \
    make install

# Install fdb \
RUN set -eux && \
    git clone --depth 1 --branch ${fdb_version} https://github.com/ecmwf/fdb.git /source/fdb && \
    cd /source/fdb && \
    mkdir -p /build/fdb && \
    cd /build/fdb && \
    ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH="/opt/fdb;/opt/fdb/eckit;/opt/fdb/metkit" /source/fdb && \
    make -j4 && \
    make install

RUN set -eux && \
    rm -rf /source && \ 
    rm -rf /build 

# Install pyfdb \
RUN set -eux \
    && git clone --single-branch --branch ${pyfdb_version} https://github.com/ecmwf/pyfdb.git \
    && python -m pip install "numpy<2.0" --user\
    && python -m pip install ./pyfdb --user

#######################################################
#             G R I B  J U M P   B U I L D
#######################################################

FROM python:3.11-bookworm AS gribjump-base
ARG rpm_repo
ARG gribjump_version=0.5.4

RUN response=$(curl -s -w "%{http_code}" ${rpm_repo}) \
    && if [ "$response" = "403" ]; then echo "Unauthorized access to ${rpm_repo} "; fi

RUN set -eux \
    && apt-get update \
    && apt-get install -y gnupg2 curl ca-certificates \
    && curl -fsSL "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" | gpg --dearmor -o /usr/share/keyrings/mars-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/mars-archive-keyring.gpg] ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" | tee /etc/apt/sources.list.d/mars.list

RUN set -eux \
    && apt-get update \
    && apt install -y gribjump-client=${gribjump_version}-gribjump

RUN set -eux \
    ls -R /opt

RUN set -eux \
    && git clone --single-branch --branch ${gribjump_version} https://github.com/ecmwf/gribjump.git
# Install pygribjump
RUN set -eux \
    && cd /gribjump \
    && python -m pip install . --user \
    && rm -rf /gribjump

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
RUN set -eux \
    && git clone --single-branch --branch ${pyfdb_version} https://github.com/ecmwf/pyfdb.git \
    && python -m pip install "numpy<2.0" --user\
    && python -m pip install ./pyfdb --user


FROM blank-base AS blank-base-c
FROM blank-base AS blank-base-cpp

#######################################################
#         S W I T C H   B A S E    I M A G E S
#######################################################

FROM ${fdb_base} AS fdb-base-final

FROM ${mars_base_c} AS mars-c-base-final

FROM ${mars_base_cpp} AS mars-cpp-base-final

FROM ${gribjump_base} AS gribjump-base-final


#######################################################
#           P Y T H O N   R E Q U I R E M E N T S
#######################################################
FROM python:3.11-slim-bookworm AS worker-base
ARG developer_mode

# contains compilers for building wheels which we don't want in the final image
RUN apt update
RUN apt-get install -y --no-install-recommends gcc libc6-dev make gnupg2

COPY ./requirements.txt /requirements.txt
RUN pip install uv --user
ENV PATH="/root/.venv/bin:/root/.local/bin:${PATH}"
RUN uv venv /root/.venv
RUN uv pip install -r requirements.txt
RUN uv pip install geopandas==1.0.1

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
COPY --chown=polytope ./aux/mars-wrapper.py  /polytope/bin/mars-wrapper.py 
COPY --chown=polytope ./aux/mars-wrapper-docker.py  /polytope/bin/mars-wrapper-docker.py

COPY --chown=polytope --from=mars-cpp-base-final   /opt/ecmwf/mars-client-cpp  /opt/ecmwf/mars-client-cpp
COPY --chown=polytope --from=mars-cpp-base-final    /root/.local /home/polytope/.local
COPY --chown=polytope --from=mars-c-base-final     /opt/ecmwf/mars-client      /opt/ecmwf/mars-client
COPY --chown=polytope --from=mars-c-base-final     /usr/local/bin/mars      /usr/local/bin/mars
RUN sudo apt update \
    && sudo apt install -y libgomp1 git libnetcdf19 liblapack3  libfftw3-bin libproj25 \
    && sudo rm -rf /var/lib/apt/lists/*


# all of this is needed by the C client, would be nice to remove it at some point
RUN set -eux \
    && mkdir -p /home/polytope/.ssh \
    && chmod 0700 /home/polytope/.ssh \
    && ssh-keyscan git.ecmwf.int > /home/polytope/.ssh/known_hosts \
    && chmod 755 /polytope/bin/mars-wrapper.py \
    && chmod 755 /polytope/bin/mars-wrapper-docker.py

ENV MARS_CONFIGS_REPO=${mars_config_repo}
ENV MARS_CONFIGS_BRANCH=${mars_config_branch}
ENV PATH="/polytope/bin/:/opt/ecmwf/mars-client/bin:/opt/ecmwf/mars-client-cloud/bin:${PATH}"

# Copy FDB-related artifacts
COPY --chown=polytope --from=fdb-base-final /opt/fdb/ /opt/fdb/
COPY --chown=polytope ./aux/default_fdb_schema /polytope/config/fdb/default
RUN mkdir -p /polytope/fdb/ && sudo chmod -R o+rw /polytope/fdb
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/fdb/lib:/opt/ecmwf/gribjump-client/lib
COPY --chown=polytope --from=fdb-base-final /root/.local /home/polytope/.local

# Copy gribjump-related artifacts, including python libraries
# COPY --chown=polytope --from=gribjump-base-final /opt/fdb/ /opt/fdb/
COPY --chown=polytope --from=gribjump-base-final /opt/ecmwf/gribjump-client/ /opt/ecmwf/gribjump-client/
COPY --chown=polytope --from=gribjump-base-final /root/.local /home/polytope/.local
# RUN sudo apt install -y libopenjp2-7
# COPY polytope-deployment/common/default_fdb_schema /polytope/config/fdb/default

# Copy python requirements
COPY --chown=polytope --from=worker-base /root/.venv /home/polytope/.local

# Install the server source
COPY --chown=polytope . /polytope/

RUN set -eux \
    && mkdir /home/polytope/data

# Remove itself from sudo group
RUN sudo deluser polytope sudo