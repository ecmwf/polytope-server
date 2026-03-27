###############################
# Shared base: Rust + cargo-chef + system deps
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
COPY workers/common/ workers/common/
COPY workers/test-worker/ workers/test-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/test-worker && cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
COPY --from=chef /ecmwf/polytope-server/workers/test-worker/recipe.json workers/test-worker/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
COPY workers/common/ workers/common/
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/test-worker && cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY Cargo.lock Cargo.lock
ARG PACKAGE_NAME=test-worker
ARG BIN_NAME=test-worker
COPY workers/common/ workers/common/
COPY workers/test-worker/ workers/test-worker/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml
COPY --from=cacher /ecmwf/polytope-server/workers/test-worker/target workers/test-worker/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd workers/test-worker && cargo build --release -p ${PACKAGE_NAME}

###############################
# Stage 4: Production Release Image
###############################
FROM docker.io/library/debian:bookworm-slim AS release
ARG VERSION
ARG BIN_NAME=test-worker
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /ecmwf/polytope-server/workers/test-worker/target/release/${BIN_NAME} /app/test-worker
ENTRYPOINT ["/app/test-worker"]
