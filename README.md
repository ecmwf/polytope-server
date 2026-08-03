<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# polytope-server

A Rust workspace containing a Polytope frontend plus separate worker crates, backed by [Bits](https://github.com/ecmwf/bits-broker) for request routing and processing, and [Bobs](https://github.com/ecmwf/bobs) for efficient data staging.

[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/incubating_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity#incubating)

> [!IMPORTANT]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

## Workspace layout

This repository is intentionally split so the frontend and workers can be moved independently later.

- `frontend/` — the Polytope HTTP frontend crate (`polytope-server` binary, `frontend` image)
- `workers/common/` — shared remote-worker runtime and protocol client
- `workers/polytope-fe-worker/` — Polytope worker crate
- `workers/fdb-worker/` — FDB worker crate
- `workers/mars-worker/` — Mars worker crate (requires native eckit/metkit C++ libraries; excluded from default workspace build)
- `workers/test-worker/` — Test worker crate for integration tests
- `loadgen/` — load-generation crate (`loadgen` binary, `polytope-loadgen` image)
- `observability/`, `client/`, `tests/integration/` — shared support crates (telemetry helpers, client library, integration test harness)

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
- `loadgen`

## Configuration

The frontend is configured with a single YAML file. The top-level `server` block controls the HTTP listener. The `polytope` block identifies the deployment with stable one-to-three-character lower-case alphanumeric `site` and `env` tags used for opaque request IDs. The `bits` block is passed directly to the bits routing engine. The `collections` block maps collection names to bits route pipelines — each collection gets its own route, sharing the same action registries and target instances.

```yaml
server:
  host: "0.0.0.0"
  port: 3000

polytope:
  site: bol
  env: dev

bits:
  # Keep these in sync with polytope.site/env until server config plumbing
  # injects them automatically.
  site: bol
  env: dev

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

See `config.example.yaml` for a starting point, [docs/request-ids.md](docs/request-ids.md) for request ID and rollout guidance, and the [bits documentation](https://github.com/ecmwf/bits-broker) for the full bits config schema.

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

## Metrics

Build the frontend with `--features telemetry` and enable `metrics:` in the config to expose Prometheus at `/metrics`.

See [docs/metrics.md](docs/metrics.md) for the raw metric reference and `dev/otel/grafana/dashboards/raw-metrics.json` for the raw Grafana dashboard.

## API

The frontend exposes the legacy v1 and newer v2 HTTP APIs. Optional frontends
can also expose OpenMeteo, EDR, and MCP-compatible facades over the same BITS
routing engine.

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

Request IDs returned by these APIs are opaque strings. Clients should pass them back unchanged to status, cancel, and download routes, but must not parse broker identity, site, environment, or ordering from the ID text.

Successful responses are streamed back to the client over HTTP.

### MCP

When `mcp:` is configured, the frontend exposes a Model Context Protocol
endpoint at `/mcp`. The MCP frontend is asynchronous-first: agents submit
retrievals, poll opaque request IDs, and receive download URLs or small inline
JSON/text results. See [docs/mcp.md](docs/mcp.md) and
[examples/mcp-config.yaml](examples/mcp-config.yaml).

## Images

### App images

`skaffold.yaml` builds separate images for the frontend, each worker, and the load generator from the same Rust workspace:

| Image | Binary | Version source |
|---|---|---|
| `eccr.ecmwf.int/polytope/frontend` | `polytope-server` | `frontend/TAG` |
| `eccr.ecmwf.int/polytope/polytope-fe-worker` | `polytope-fe-worker` | `workers/polytope-fe-worker/TAG` |
| `eccr.ecmwf.int/polytope/fdb-worker` | `fdb-worker` | `workers/fdb-worker/TAG` |
| `eccr.ecmwf.int/polytope/mars-worker` | `mars-worker` | `workers/mars-worker/TAG` |
| `eccr.ecmwf.int/polytope/test-worker` | `test-worker` | `workers/test-worker/TAG` |
| `eccr.ecmwf.int/polytope/polytope-loadgen` | `loadgen` | `loadgen/TAG` |

Each image is versioned **independently** via its own `TAG` file. On a GitHub release each image is published under the version in its `TAG` file — **not** the git release tag — so image versions can drift independently of each other and of the top-level `VERSION` file. Image tags are immutable: if an image's `TAG` already exists in ECCR it was published by an earlier release and is skipped rather than overwritten.

The top-level `VERSION` file is the app-wide version. On every push to `main`, CI creates a `{VERSION}.dev0` git tag if it doesn't already exist; a human promotes that to the real release tag (e.g. `2.2.0`) to trigger the release workflow.

For dev builds, skaffold tags images with the current git commit SHA by default (`tagPolicy.gitCommit`). Set `FIXED_TAG` to override, or `PREFIX` to prepend to the SHA.

### C++ library images

The fdb-worker, polytope-fe-worker, and mars-worker images depend on pre-built C++ library images (eckit, metkit, FDB, gribjump, MARS client). These are published separately and versioned independently of the app images — each has its own `TAG` file under `docker/cpp-libs/<name>/TAG`.

See [`docker/cpp-libs/README.md`](docker/cpp-libs/README.md) for the full list of images, how to build and publish them, and how to bump a C++ library version.

Each Dockerfile copies only its own crate's sources (not the whole workspace), so a change to the frontend does not rebuild the workers and vice versa.

## Future extraction

The current layout is designed so the crates can be moved later with minimal churn:

- the frontend is self-contained under `frontend/`
- each worker is self-contained under its own directory
- the only in-repo worker dependency is `workers/common/`

If a worker needs to move to its own repository later, it should mostly be a matter of copying that crate plus `workers/common/` (or publishing `workers/common/` as its own crate).

## License

[Apache License 2.0](LICENSE) In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
