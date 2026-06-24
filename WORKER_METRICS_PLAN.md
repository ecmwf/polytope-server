# Worker Metrics Plan

## Context

Workers currently emit structured log events (`worker.job.started`, `worker.job.completed`, etc.) but no OTel metrics. The broker has `bits.jobs.*` metrics; BOBS has `bobs.*` metrics. Workers have nothing — this fills the gap.

All instrumentation goes in `workers/common/` so every worker (fdb, mars, polytope-fe) gets it for free.

## Labels

Every metric carries:

| Label | Source | Cardinality |
|-------|--------|-------------|
| `worker_pool` | env var `POLYTOPE_WORKER_POOL` (set by Helm from pool name) | bounded, ~5-10 pools |
| `worker_instance` | pod name via `HOSTNAME` or Downward API | bounded by replica count |

These two labels give you: per-worker graphs (filter `worker_instance`), per-pool graphs (group by `worker_pool`), and fleet-wide (sum all).

## Metrics

| Instrument | Type | Description |
|---|---|---|
| `polytope.worker.jobs.processed.total` | Counter\<u64\> | Jobs that reached a terminal state |
| — label `outcome` | | `success`, `reject`, `error`, `redirect` |
| `polytope.worker.job.duration.seconds` | Histogram\<f64\> | Wall-clock time from process start to completion post |
| — label `outcome` | | same as above |
| `polytope.worker.job.processing.seconds` | Histogram\<f64\> | Time spent in `Processor::process()` only (excludes delivery + completion post) |
| — label `outcome` | | same as above |
| `polytope.worker.jobs.active` | UpDownCounter\<i64\> | In-flight jobs right now |
| `polytope.worker.polls.total` | Counter\<u64\> | Broker poll attempts |
| — label `result` | | `work`, `empty`, `error` |
| `polytope.worker.delivery.duration.seconds` | Histogram\<f64\> | Time spent in the delivery step (encode + upload to bobs/s3/direct) |
| `polytope.worker.delivery.bytes.total` | Counter\<u64\> | Bytes delivered (post-encoding) |

Seven instruments, max cardinality: `pools × instances × outcomes` ≈ small.

### Byte counting

A thin `CountingBody` wrapper sits between `encode_stream` output and the `deliver()` call. It passes bytes through unchanged but accumulates a total in a shared `Arc<AtomicU64>`. After delivery completes, read the count and record it. This works for all three delivery types without modifying any `ResultDelivery` impl.

## Architecture

```
workers/common/src/metrics.rs    — instruments + record_* helpers (mirrors bits/src/metrics.rs pattern)
workers/common/src/lib.rs        — calls record_* at the right lifecycle points
workers/common/Cargo.toml        — add opentelemetry deps (behind `telemetry` feature)
```

The `MeterProvider` is installed by each worker binary's `main()` (same pattern as polytope-server/frontend). When no provider is configured, instruments are no-ops — zero cost.

## Implementation Steps

### 1. Dependencies

Add to `workers/common/Cargo.toml`:
```toml
[features]
default = ["telemetry"]
telemetry = ["opentelemetry", "opentelemetry_sdk", "opentelemetry-otlp"]

[dependencies]
opentelemetry = { version = "0.30", optional = true }
opentelemetry_sdk = { version = "0.30", features = ["rt-tokio"], optional = true }
opentelemetry-otlp = { version = "0.30", optional = true }
```

### 2. `workers/common/src/metrics.rs`

- `OnceLock`-based lazy instrument initialization (same pattern as `bits/src/metrics.rs`)
- `WorkerMetricsConfig { worker_pool: String, worker_instance: String }` read from env
- `record_job_started()` — increments active gauge
- `record_job_finished(outcome, duration, processing_duration, delivery_duration)` — decrements active, increments counter, records histograms
- `record_poll(result)` — increments poll counter

### 3. Instrument `worker_task` in `lib.rs`

```
poll → record_poll(result)
process start → record_job_started(), start = Instant::now()
process end → processing_duration captured
delivery end → delivery_duration captured
completion post done → record_job_finished(...)
```

Minimal diff — ~10 lines of timing + record calls inserted around existing code.

### 4. OTel provider init in each worker `main.rs`

Add a shared helper in `common` (e.g. `init_meter_provider()`) that:
- Reads `OTEL_EXPORTER_OTLP_ENDPOINT` env var
- If set, builds a push-based OTLP exporter
- If absent, does nothing (no-ops)

Each worker calls it before `run_worker_loop`. One line per worker binary.

### 5. Helm: inject `POLYTOPE_WORKER_POOL` env var

In `polytope-chart/templates/worker-pool.yaml`, add:
```yaml
- name: POLYTOPE_WORKER_POOL
  value: {{ $poolName }}
```

Already has `HOSTNAME` from Kubernetes by default.

### 6. Grafana dashboard

`dev/otel/grafana/dashboards/worker-metrics.json` with:
- **Per-pool row**: throughput, outcome breakdown, duration percentiles — grouped by `worker_pool`
- **Per-worker row**: same metrics filtered by `worker_instance` with a variable selector
- **Fleet row**: active jobs gauge, poll error rate

Queries follow the same style as `bits-job-metrics.json` (PromQL over OTLP-exported metrics).

### 7. Validation

- `cargo check --all-features` passes
- Local dev stack: worker emits metrics to collector, visible in Grafana
- No new deps when `telemetry` feature is off

## Out of Scope

- Per-request-type breakdown (would need the processor to report what kind of request it was — future label)
- Retry/backoff metrics (low value, visible from poll error rate)

## Naming Convention

Prefix: `polytope.worker.*` — distinct from `bits.*` (broker) and `bobs.*` (storage). Dots in OTel become underscores in Prometheus (`polytope_worker_jobs_processed_total`).
