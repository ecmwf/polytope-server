# polytope-server

A Rust implementation of the [Polytope](https://polytope.ecmwf.int) data retrieval API, backed by [bits](../bits) for request routing and processing.

## Prerequisites

- [Rust](https://rustup.rs) 1.75+

## Build

```bash
cargo build --release
```

The binary is written to `target/release/polytope-server`.

### Local bits override

By default this repository pulls `bits` from the `ecmwf/bits-broker` Git repository at a pinned revision, which keeps CI and releases reproducible.

For local development against a sibling checkout, copy the provided Cargo patch template:

```bash
mkdir -p .cargo
cp .cargo/config.toml.example .cargo/config.toml
```

That local override makes Cargo use `../bits/bits` instead of the pinned Git dependency.

## Configuration

The server is configured with a single YAML file. The top-level `server` block controls the HTTP listener; the `bits` block is passed directly to the bits routing engine.

```yaml
server:
  host: "0.0.0.0"   # optional, default 0.0.0.0
  port: 3000         # optional, default 3000

bits:
  routes:
    default:
      - type: noop
```

See `config.example.yaml` for a starting point, and the [bits documentation](../bits) for the full `bits` config schema.

## Running

```bash
polytope-server config.yaml
```

Or directly via Cargo:

```bash
cargo run -- config.yaml
```

Set the `RUST_LOG` environment variable to control log verbosity:

```bash
RUST_LOG=info polytope-server config.yaml
```

## API

All endpoints are under `/api/v1`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/test` | Health check — returns `"Polytope server is alive"` |
| `GET` | `/api/v1/collections` | Returns `["all"]` _(deprecated)_ |
| `POST` | `/api/v1/requests/:collection` | Submit a request. The collection is ignored; routing is determined by the bits config. |
| `GET` | `/api/v1/requests/:id` | Poll for a result. Long-polls for up to 30 s, then returns `202` if still pending. |
| `DELETE` | `/api/v1/requests/:id` | Cancel a request. |
| `GET` | `/api/v1/downloads/:id` | _(deprecated — returns 410)_ |

### Submitting a request

```
POST /api/v1/requests/any-collection
Content-Type: application/json

{
  "verb": "retrieve",
  "request": {
    "class": "od",
    "stream": "oper"
  }
}
```

Response (`202 Accepted`):

```json
{ "status": "queued", "id": "a1b2c3d4-..." }
```

### Polling for a result

```
GET /api/v1/requests/a1b2c3d4-...
```

- `202 Accepted` — still processing, retry
- `200 OK` — complete, body contains the result stream
- `303 See Other` — result available at `Location` header
- `400 Bad Request` — request-level error
- `500 Internal Server Error` — system failure
