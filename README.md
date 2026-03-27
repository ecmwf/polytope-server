# polytope-server

A Rust workspace containing a Polytope frontend plus separate worker crates, backed by [bits](../bits) for request routing and processing.

## Workspace layout

This repository is intentionally split so the frontend and workers can be moved independently later.

- `frontend/` — the Polytope HTTP frontend crate (`polytope-server` binary)
- `workers/common/` — shared remote-worker runtime and protocol client
- `workers/polytope-fe-worker/` — Polytope worker crate
- `workers/fdb-worker/` — FDB worker crate
- `workers/mars-worker/` — Mars worker crate (requires native eckit/metkit C++ libraries; excluded from default workspace build)
- `workers/test-worker/` — Test worker crate for integration tests

The frontend does not depend on worker crates. The only shared worker-side dependency is `workers/common/`.

## Prerequisites

- [Rust](https://rustup.rs) 1.75+

## Build

Build the whole workspace (excludes mars-worker, which requires native C++ libraries):

```bash
cargo build --release
```

Build a single crate:

```bash
cargo build -p polytope-server
cargo build -p polytope-fe-worker
cargo build -p fdb-worker
cargo build -p mars-worker   # requires eckit/metkit C++ libraries installed
```

The workspace produces separate binaries under `target/release/`:

- `polytope-server`
- `polytope-fe-worker`
- `fdb-worker`
- `mars-worker`
- `test-worker`

## Configuration

The frontend is configured with a single YAML file. The top-level `server` block controls the HTTP listener; the `bits` block is passed directly to the bits routing engine. The `collections` block maps collection names to bits route pipelines — each collection gets its own route, sharing the same action registries and target instances.

```yaml
server:
  host: "0.0.0.0"
  port: 3000

collections:
  climate:
    - target::http:
        url: "http://climate-backend/api"
  operational:
    - check::has_role:
        roles:
          ecmwf:
            - admin
    - target::http:
        url: "http://ops-backend/api"
```

See `config.example.yaml` for a starting point, and the [bits documentation](../bits) for the full bits config schema.

## Running

Frontend:

```bash
cargo run -p polytope-server -- config.yaml
```

Workers:

```bash
cargo run -p polytope-fe-worker -- --broker-url http://127.0.0.1:9001 --config-path worker-config.yaml
cargo run -p fdb-worker -- --broker-url http://127.0.0.1:9001 --config-path worker-config.yaml
cargo run -p mars-worker -- --broker-url http://127.0.0.1:9001 --config-path worker-config.yaml
```

Set `RUST_LOG` to control log verbosity:

```bash
RUST_LOG=info cargo run -p polytope-server -- config.yaml
```

## API

The frontend exposes the legacy v1 and newer v2 HTTP APIs.

### v1 (legacy)

- `GET /api/v1/test`
- `GET /api/v1/collections`
- `GET /api/v1/requests`
- `POST /api/v1/requests/{id}`
- `GET /api/v1/requests/{id}`
- `DELETE /api/v1/requests/{id}`
- `GET /api/v1/downloads/{id}` (deprecated)

### v2

- `GET /api/v2/health`
- `GET /api/v2/collections`
- `POST /api/v2/{collection}/requests`
- `GET /api/v2/requests/{id}`
- `DELETE /api/v2/requests/{id}`

v2 routes requests through the named collection — each collection maps to a separate bits route pipeline. The collection name must match a key in the `collections` config block.

Successful responses are streamed back to the client over HTTP.

## Images

`skaffold.yaml` builds separate images for the frontend and each worker. The Docker build is workspace-aware:

- `PACKAGE_NAME=polytope-server`, `BIN_NAME=polytope-server`
- `PACKAGE_NAME=polytope-fe-worker`, `BIN_NAME=polytope-fe-worker`
- `PACKAGE_NAME=fdb-worker`, `BIN_NAME=fdb-worker`
- `PACKAGE_NAME=mars-worker`, `BIN_NAME=mars-worker`

That mapping is what keeps the images separate even though they live in one workspace.

## Future extraction

The current layout is designed so the crates can be moved later with minimal churn:

- the frontend is self-contained under `frontend/`
- each worker is self-contained under its own directory
- the only in-repo worker dependency is `workers/common/`

If a worker needs to move to its own repository later, it should mostly be a matter of copying that crate plus `workers/common/` (or publishing `workers/common/` as its own crate).
