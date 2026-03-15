FROM scratch AS bits
FROM scratch AS rsfdb
FROM scratch AS rsfindlibs

###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS chef
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY . .
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfdb"]' >> .cargo/config.toml && echo 'rsfdb = { path = "/ecmwf/rsfdb" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfindlibs"]' >> .cargo/config.toml && echo 'rsfindlibs = { path = "/ecmwf/rsfindlibs" }' >> .cargo/config.toml; fi
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS cacher
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS builder
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=polytope-fe-worker
ARG BIN_NAME=polytope-fe-worker
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY . .
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfdb"]' >> .cargo/config.toml && echo 'rsfdb = { path = "/ecmwf/rsfdb" }' >> .cargo/config.toml && echo '[patch."https://github.com/ecmwf/rsfindlibs"]' >> .cargo/config.toml && echo 'rsfindlibs = { path = "/ecmwf/rsfindlibs" }' >> .cargo/config.toml; fi
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cargo build --release -p ${PACKAGE_NAME}

###############################
# FDB Build (from source)
###############################
FROM python:3.11-bookworm AS fdb-build
ARG ecbuild_version=3.8.2
ARG eccodes_version=2.33.1
ARG eckit_version=1.28.3
ARG fdb_version=5.14.0
ARG gribjump_version=0.10.2
ARG pyfdb_version=0.1.0

RUN apt-get update && apt-get install -y cmake gnupg build-essential net-tools bison flex git libaec-dev && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch ${ecbuild_version} https://github.com/ecmwf/ecbuild.git /ecbuild
ENV PATH=/ecbuild/bin:$PATH

RUN git clone --depth 1 --branch ${eckit_version} https://github.com/ecmwf/eckit.git /source/eckit \
    && mkdir -p /build/eckit && cd /build/eckit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/eckit \
    && make -j4 && make install

RUN git clone --depth 1 --branch ${eccodes_version} https://github.com/ecmwf/eccodes.git /source/eccodes \
    && mkdir -p /build/eccodes && cd /build/eccodes \
    && ecbuild --prefix=/opt/fdb -- -DENABLE_FORTRAN=OFF -DCMAKE_PREFIX_PATH=/opt/fdb /source/eccodes \
    && make -j4 && make install

RUN git clone --depth 1 --branch 1.11.22 https://github.com/ecmwf/metkit.git /source/metkit \
    && mkdir -p /build/metkit && cd /build/metkit \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/metkit \
    && make -j4 && make install

RUN git clone --depth 1 --branch ${fdb_version} https://github.com/ecmwf/fdb.git /source/fdb \
    && mkdir -p /build/fdb && cd /build/fdb \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH="/opt/fdb;/opt/fdb/eckit;/opt/fdb/metkit" -DCMAKE_CXX_FLAGS=-fpermissive /source/fdb \
    && make -j4 && make install

RUN git clone --depth 1 --branch v1.1.3 https://github.com/Deutsches-Klimarechenzentrum/libaec.git /source/libaec \
    && mkdir -p /build/libaec && cd /build/libaec \
    && cmake -DCMAKE_INSTALL_PREFIX=/opt/fdb /source/libaec \
    && make -j4 && make install \
    && rm -rf /source/libaec /build/libaec

RUN git clone --depth 1 --branch ${gribjump_version} https://github.com/ecmwf/gribjump.git /source/gribjump \
    && mkdir -p /build/gribjump && cd /build/gribjump \
    && ecbuild --prefix=/opt/fdb -- -DCMAKE_PREFIX_PATH=/opt/fdb /source/gribjump \
    && make -j4 && make install

RUN rm -rf /source /build

RUN git clone --single-branch --branch ${pyfdb_version} https://github.com/ecmwf/pyfdb.git \
    && python -m pip install "numpy<2.0" --user \
    && python -m pip install ./pyfdb --user



###############################
# Python Requirements
###############################
FROM python:3.11-slim-bookworm AS python-deps
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git && rm -rf /var/lib/apt/lists/*
RUN pip install uv --user
ENV PATH="/root/.venv/bin:/root/.local/bin:${PATH}"
RUN uv venv /root/.venv
COPY workers/polytope-fe-worker/requirements.txt /requirements.txt
RUN uv pip install -r /requirements.txt

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
COPY --from=fdb-build /root/.local /root/.local
COPY --from=python-deps /root/.venv /root/.local

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
COPY --from=fdb-build /root/.local /root/.local
COPY --from=python-deps /root/.venv /root/.local

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/fdb/lib:/usr/local/lib

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/polytope-fe-worker
COPY workers/polytope-fe-worker/run_polytope_worker.py /app/
COPY workers/polytope-fe-worker/polytope.py /app/
ENTRYPOINT ["/app/polytope-fe-worker"]
