# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

###############################
# Shared base: Rust + cargo-chef + system deps
###############################
FROM docker.io/library/rust:1.93-slim-bookworm AS chef-base
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

###############################
# Prepare the root workspace Cargo Chef recipe
###############################
FROM chef-base AS chef
ARG GIT_AUTH_TOKEN=""
WORKDIR /ecmwf/polytope-server
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo chef prepare --recipe-path recipe.json

###############################
# Cache root workspace dependencies without application source
###############################
FROM chef-base AS cacher
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=test-worker
WORKDIR /ecmwf/polytope-server
COPY --from=chef /ecmwf/polytope-server/recipe.json recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
# cargo-chef omits excluded path dependencies from root workspace recipes.
COPY utils/metkit/Cargo.toml utils/metkit/Cargo.toml
RUN mkdir -p utils/metkit/src && touch utils/metkit/src/lib.rs \
    && printf 'fn main() {}\n' > utils/metkit/build.rs
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo chef cook --release --locked --package "${PACKAGE_NAME}" --recipe-path recipe.json

###############################
# Build the package from the root workspace
###############################
FROM chef-base AS builder
ARG GIT_AUTH_TOKEN=""
ARG PACKAGE_NAME=test-worker
ARG BIN_NAME=test-worker
WORKDIR /ecmwf/polytope-server
COPY --from=cacher /ecmwf/polytope-server/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN mkdir -p .cargo && printf '[net]\ngit-fetch-with-cli = true\n' > .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then \
      git config --global credential.helper '' && \
      git config --global url."https://git:${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi
RUN cargo build --release --locked --package "${PACKAGE_NAME}"

###############################
# Stage 4: Production Release Image
###############################
FROM docker.io/library/debian:bookworm-slim AS release
ARG VERSION
ARG BIN_NAME=test-worker
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /ecmwf/polytope-server/target/release/${BIN_NAME} /app/test-worker
ENTRYPOINT ["/app/test-worker"]
