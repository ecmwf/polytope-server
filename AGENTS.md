# POLYTOPE-SERVER KNOWLEDGE BASE

## OVERVIEW

Rust workspace: Axum HTTP frontend for data retrieval + 3 remote workers (FDB, MARS, Polytope-FE), backed by bits-broker for job routing. Designed for future extraction — each worker is self-contained.

## STRUCTURE

```
polytope-server/
├── frontend/                   # Main API server binary
│   └── src/
│       ├── main.rs             # Entry: CLI arg → config → bits init → Axum server
│       ├── config.rs           # YAML config: server block + bits block
│       ├── state.rs            # AppState { bits: Bits }
│       └── api/
│           ├── v1.rs           # Legacy API (test, collections, requests CRUD)
│           ├── v2.rs           # ★ Modern API (health, submit, poll, cancel)
│           └── openmeteo/      # OpenMeteo compatibility layer (variable mapping, CoverageJSON)
├── workers/
│   ├── common/src/lib.rs       # ★ Worker protocol: WorkItem, Completion, Processor trait, run_worker_loop
│   ├── fdb-worker/src/main.rs  # FDB data retrieval via rsfdb (streaming GRIB)
│   ├── mars-worker/src/        # MARS retrieval via C++ client (main.rs + convert.rs)
│   └── polytope-fe-worker/     # Python worker via PyO3 (calls run_polytope_worker)
├── .cargo/config.toml          # ★ Local patches: bits-broker, rsfdb, rsfindlibs → sibling paths
├── skaffold.yaml               # Docker build for 4 images (frontend + 3 workers)
└── tests/                      # Python integration + e2e tests (pytest)
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add API endpoint | `frontend/src/api/v2.rs` | Follow submit/poll pattern |
| Modify worker protocol | `workers/common/src/lib.rs` | WorkItem, Completion, Processor trait |
| Add new worker type | Copy `fdb-worker/`, implement `Processor` trait | See workers/README.md |
| Change config | `frontend/src/config.rs` | server + bits YAML sections |
| OpenMeteo integration | `frontend/src/api/openmeteo/` | Variable mapping + CoverageJSON |
| Docker builds | `skaffold.yaml` + `*/Dockerfile` | Multi-stage with cargo-chef |
| Integration tests | `tests/test_integration.py` | Spawns mock backend + polytope-server |

## KEY PATTERNS

**API handler signature**: `async fn handler(State(state): State<Arc<AppState>>, ...) -> Response`

**Submit + poll flow**:
1. `state.bits.submit(Job::new(body))` → `JobHandle { id }`
2. `state.bits.poll(&id, Some(POLL_TIMEOUT))` → match `PollOutcome`
3. `Pending` → 303 redirect to poll URL
4. `Ready(Success)` → stream body via `Body::from_stream()`

**Worker protocol** (HTTP long-poll):
- `GET  /{pool}/work?timeout_ms=N` → WorkItem JSON or 204
- `POST /{pool}/heartbeat/{job_id}` → 200 (alive) or 404 (job gone)
- `POST /{pool}/complete/data/{job_id}` → stream binary body
- `POST /{pool}/complete/reject/{job_id}` → JSON reason
- `POST /{pool}/complete/error/{job_id}` → JSON message

**Streaming from blocking I/O** (fdb-worker pattern):
```
mpsc::channel(16) → spawn_blocking { read chunks → tx.blocking_send } → ReceiverStream → reqwest::Body::wrap_stream
```

## CONVENTIONS

- **Workspace deps**: all versions in root `[workspace.dependencies]`, members use `.workspace = true`
- **Axum 0.8**: path params via `{id}` (not `:id`)
- **POLL_TIMEOUT**: 30s for v1/v2, 120s for openmeteo
- **Error responses**: JSON `{"error": "message"}` with appropriate status code
- **Tracing**: `tracing::info/warn/error` with structured fields (`job.id = %id`)

## ANTI-PATTERNS

- Polling without timeout — always pass `Some(Duration)` to `bits.poll()`
- Blocking in async context — use `tokio::task::spawn_blocking` for I/O
- Hardcoding broker URLs — use config/CLI args
- Ignoring 204 from worker poll — means no work; just loop again

## COMMANDS

```bash
cargo build --release
cargo build -p polytope-server                              # frontend only
cargo run -p polytope-server -- config.yaml                 # run frontend
cargo run -p fdb-worker -- --broker-url http://127.0.0.1:9001
cargo run -p mars-worker -- --broker-url http://127.0.0.1:9001
RUST_LOG=info cargo run -p polytope-server -- config.yaml   # with logging
pytest tests/test_integration.py                             # integration tests
pytest -m e2e tests/test_e2e.py                              # e2e (needs live cluster)
```

## NOTES

- **CI currently disabled** — image testing phase
- **Local deps**: `.cargo/config.toml` patches bits-broker/rsfdb/rsfindlibs to sibling paths — remove for CI/Docker builds
- **Docker**: multi-stage builds with cargo-chef caching; FDB/MARS libraries built from source inside containers
- **Extraction-ready**: frontend and each worker can move to separate repos; only `workers/common/` is shared
- **Python integration**: polytope-fe-worker uses PyO3 0.23 + requirements.txt for Python deps
