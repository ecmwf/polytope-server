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
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
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
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
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
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=mars-worker
ARG BIN_NAME=mars-worker
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
# MARS Client C (from RPM)
###############################
FROM python:3.11-bookworm AS mars-base
ARG rpm_repo

RUN curl -o stable-public.gpg.key "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" \
    && echo "deb ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" >> /etc/apt/sources.list \
    && apt-key add stable-public.gpg.key \
    && apt-get update \
    && apt-get install -y libnetcdf19 liblapack3 \
    && rm -rf /var/lib/apt/lists/*

FROM mars-base AS mars-base-c
ARG mars_client_c_version=6.33.20.2
RUN apt-get update && apt-get install -y liblapack3 mars-client=${mars_client_c_version} mars-client-cloud && rm -rf /var/lib/apt/lists/*

FROM mars-base AS mars-base-cpp
ARG mars_client_cpp_version=7.1.9.1
ARG pyfdb_version=0.1.0
RUN apt-get update && apt-get install -y mars-client-cpp=${mars_client_cpp_version} && rm -rf /var/lib/apt/lists/*
RUN git clone --single-branch --branch ${pyfdb_version} https://github.com/ecmwf/pyfdb.git \
    && python -m pip install "numpy<2.0" --user \
    && python -m pip install ./pyfdb --user



###############################
# Stage 4: Production Release Image
###############################
FROM docker.io/library/debian:bookworm-slim AS release
ARG VERSION
ARG BIN_NAME=mars-worker
LABEL version=$VERSION

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libnetcdf19 liblapack3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=mars-base-cpp /opt/ecmwf/mars-client-cpp /opt/ecmwf/mars-client-cpp
COPY --from=mars-base-cpp /root/.local /root/.local

ENV PATH="/opt/ecmwf/mars-client/bin:/opt/ecmwf/mars-client-cloud/bin:${PATH}"

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/mars-worker
ENTRYPOINT ["/app/mars-worker"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG BIN_NAME=mars-worker
LABEL version=$VERSION

RUN apt-get update && apt-get install -y ca-certificates libgomp1 libnetcdf19 liblapack3 bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=mars-base-cpp /opt/ecmwf/mars-client-cpp /opt/ecmwf/mars-client-cpp
COPY --from=mars-base-cpp /root/.local /root/.local

ENV PATH="/opt/ecmwf/mars-client/bin:/opt/ecmwf/mars-client-cloud/bin:${PATH}"

COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/mars-worker
ENTRYPOINT ["/app/mars-worker"]
