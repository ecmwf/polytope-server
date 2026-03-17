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
COPY frontend/ frontend/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml; fi
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd frontend && cargo chef prepare --recipe-path recipe.json

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
COPY --from=chef /ecmwf/polytope-server/frontend/recipe.json frontend/recipe.json
COPY --from=chef /ecmwf/polytope-server/.cargo/config.toml .cargo/config.toml
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd frontend && cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS builder
ARG GIT_AUTH_TOKEN=""
ARG LOCAL_DEPS=""
RUN apt-get update && apt-get install -y git pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /ecmwf/polytope-server
ARG PACKAGE_NAME=polytope-server
ARG BIN_NAME=polytope-server
COPY --from=bits . /ecmwf/bits/
COPY --from=rsfdb . /ecmwf/rsfdb/
COPY --from=rsfindlibs . /ecmwf/rsfindlibs/
COPY frontend/ frontend/
RUN mkdir -p .cargo && echo "[net]" > .cargo/config.toml && echo "git-fetch-with-cli = true" >> .cargo/config.toml && if [ "$LOCAL_DEPS" = "1" ]; then echo '[patch."https://github.com/ecmwf/bits-broker"]' >> .cargo/config.toml && echo 'bits = { path = "/ecmwf/bits/bits" }' >> .cargo/config.toml && echo 'bits-ecmwf = { path = "/ecmwf/bits/bits-ecmwf" }' >> .cargo/config.toml; fi
COPY --from=cacher /ecmwf/polytope-server/frontend/target frontend/target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN if [ -n "$GIT_AUTH_TOKEN" ]; then git config --global url."https://${GIT_AUTH_TOKEN}@github.com/".insteadOf "https://github.com/"; fi
RUN cd frontend && cargo build --release -p ${PACKAGE_NAME}

###############################
# Stage 4: Production Release Image
###############################
FROM gcr.io/distroless/cc-debian12:nonroot AS release
ARG VERSION
ARG BIN_NAME=polytope-server
LABEL version=$VERSION
WORKDIR /app
COPY --from=builder /ecmwf/polytope-server/frontend/target/release/${BIN_NAME} /app/polytope-server
ENTRYPOINT ["/app/polytope-server"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
ARG BIN_NAME=polytope-server
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /ecmwf/polytope-server/frontend/target/release/${BIN_NAME} /app/polytope-server
ENTRYPOINT ["/app/polytope-server"]
