<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Metrics

This page lists the raw metrics emitted by Polytope Server components and related scrape targets in the local monitoring stack, and what each one means. Prometheus names assume the `opentelemetry-prometheus` exporter used by the local dev stack.

Raw Grafana dashboard: `dev/otel/grafana/dashboards/raw-metrics.json`.

## Label notes

- Custom metric samples include `otel_scope_name` from the OpenTelemetry meter (`bits`, `bobs`, or `polytope.worker`). The current meters do not set scope attributes/version, so `otel_scope_info` is not emitted.
- Resource attributes are emitted as the generated `target_info` series, not repeated on every custom metric sample with the current exporter setup.
- `route_handle`: BITS route handle; in Polytope this is the collection name.
- `outcome`: terminal state. Broker values: `success`, `redirect`, `error`, `failed`, `overloaded`, `cancelled`, `client_gone`. Worker values: `success`, `reject`, `error`, `redirect`. BOBS read values currently recorded by the read path: `success`, `error`, `timeout`, `client_gone`.
- `collection`: caller-provided BOBS label, normally the Polytope collection name. BOBS can pass any caller label allowed by `metrics.allowed_labels`, with values truncated by `metrics.max_label_value_length`.
- `mode`: BOBS read mode, `follow` for streaming until completion or `range` for bounded HTTP range reads.
- `reason`: BOBS deletion reason: `client`, `idle_ttl`, `full_read_ttl`, `writer_timeout`.
- `state`: BOBS spool state: `writing`, `write_locked`, `complete`, `readable`.
- `result`: worker poll result: `work`, `empty`, or `error`.
- `worker_pool`, `worker_instance`: worker deployment pool and pod/host name.

## Exporter-generated metrics

| Metric | Prometheus series | Type | Labels | What it is |
|---|---|---|---|---|
| OpenTelemetry target info | `target_info` | Gauge | Frontend: `service_name`, `service_instance_id`, `service_version`, `deployment_environment`, `bits_site`, `bits_env`, `telemetry_sdk_language`, `telemetry_sdk_name`, `telemetry_sdk_version`. BOBS: `service_name`, `service_instance_id`, `service_version`, `deployment_environment`, `k8s_pod_name`, `telemetry_sdk_language`, `telemetry_sdk_name`, `telemetry_sdk_version`. Worker: depends on the worker binary's meter-provider setup. | One sample carrying resource attributes for the scraped process. |

## Frontend / broker metrics

The frontend installs the meter provider and renames raw BITS metrics from `bits.*` to `polytope.broker.*` via SDK views. Dispatcher rows require a BITS revision with dispatcher instrumentation; older pins expose only the job and collection rows.

| Metric | Prometheus series | Type | Labels | What it is |
|---|---|---|---|---|
| `polytope.broker.requests.accepted` | `polytope_broker_requests_accepted_total` | Counter | `otel_scope_name` | Requests accepted into the broker job map. Recovery re-submits are not counted as new accepts. |
| `polytope.broker.requests.finished` | `polytope_broker_requests_finished_total` | Counter | `outcome`, `otel_scope_name` | Requests that reached a terminal broker result. |
| `polytope.broker.request.duration` | `polytope_broker_request_duration_seconds_bucket`, `_sum`, `_count` | Histogram | `outcome`, `le` on buckets, `otel_scope_name` | End-to-end broker lifetime from submit time to terminal result. |
| `polytope.broker.collection.requests.accepted` | `polytope_broker_collection_requests_accepted_total` | Counter | `route_handle`, `otel_scope_name` | Requests accepted through a named route handle / collection. |
| `polytope.broker.collection.requests.finished` | `polytope_broker_collection_requests_finished_total` | Counter | `route_handle`, `outcome`, `otel_scope_name` | Collection-scoped requests that reached a terminal broker result. |
| `polytope.broker.collection.request.duration` | `polytope_broker_collection_request_duration_seconds_bucket`, `_sum`, `_count` | Histogram | `route_handle`, `outcome`, `le` on buckets, `otel_scope_name` | Collection-scoped broker lifetime from submit to terminal result. |
| `polytope.broker.dispatcher.queue_depth` | `polytope_broker_dispatcher_queue_depth` | Gauge | `otel_scope_name` | Current number of jobs waiting in BITS dispatcher queues. Incremented on enqueue, decremented on dequeue/drain. |
| `polytope.broker.dispatcher.queue_wait` | `polytope_broker_dispatcher_queue_wait_seconds_bucket`, `_sum`, `_count` | Histogram | `le` on buckets, `otel_scope_name` | Time a job spent waiting in a dispatcher queue before execution started. |

## BOBS metrics

BOBS exposes these from its own scrape endpoint when built with `--features telemetry` and `metrics.enabled: true`. The scrape port is configured separately from the main data port (default `9090`).

| Metric | Prometheus series | Type | Labels | What it is |
|---|---|---|---|---|
| `bobs.spools.created` | `bobs_spools_created_total` | Counter | caller labels such as `collection`, `otel_scope_name` | Spools successfully created. |
| `bobs.spools.completed` | `bobs_spools_completed_total` | Counter | caller labels such as `collection`, `otel_scope_name` | Spools successfully finalized by the writer. |
| `bobs.spools.deleted` | `bobs_spools_deleted_total` | Counter | caller labels such as `collection`, `reason`, `otel_scope_name` | Spools deleted by client request or cleanup. |
| `bobs.create.duration` | `bobs_create_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels such as `collection`, `otel_scope_name` | Wall time from spool creation request to stored first page. |
| `bobs.complete.duration` | `bobs_complete_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels such as `collection`, `otel_scope_name` | Wall time for the complete request to flush and finalize a spool. |
| `bobs.write.bytes` | `bobs_write_bytes_total` | Counter | caller labels such as `collection`, `otel_scope_name` | Bytes written into spools. Recorded after write batches complete. |
| `bobs.write.duration` | `bobs_write_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels such as `collection`, `otel_scope_name` | Wall time for a write handler to receive and persist a streaming write. |
| `bobs.read.bytes` | `bobs_read_bytes_total` | Counter | caller labels such as `collection`, `mode`, `otel_scope_name` | Bytes served from spools to clients. |
| `bobs.read.duration` | `bobs_read_duration_seconds_bucket`, `_sum`, `_count` | Histogram | caller labels such as `collection`, `mode`, `outcome`, `otel_scope_name` | Wall time for a read stream. |
| `bobs.read.active` | `bobs_read_active` | Gauge | caller labels such as `collection`, `otel_scope_name` | Current active readers. Incremented when a reader is acquired and decremented when released. |
| `bobs.spools.active` | `bobs_spools_active` | Gauge | `state`, `otel_scope_name` | Current active spools by state. Updated on spool state transitions and removal. |
| `bobs.disk.usage.bytes` | `bobs_disk_usage_bytes` | Gauge | `otel_scope_name` | Current disk usage of the BOBS spool data directory. Sampled during cleanup sweeps. |
| `bobs.pages.cache.hits` | `bobs_pages_cache_hits_total` | Counter | `otel_scope_name` | Page reads served from the in-memory page cache. |
| `bobs.pages.cache.misses` | `bobs_pages_cache_misses_total` | Counter | `otel_scope_name` | Page reads that missed cache and loaded from disk. |

## Worker metrics

Worker metrics are instrumented in `workers/common`. They emit if the worker process installs an OpenTelemetry meter provider.

| Metric | Prometheus series | Type | Labels | What it is |
|---|---|---|---|---|
| `polytope.worker.polls` | `polytope_worker_polls_total` | Counter | `result`, `worker_pool`, `worker_instance`, `otel_scope_name` | Worker poll attempts against the broker. |
| `polytope.worker.jobs.active` | `polytope_worker_jobs_active` | Gauge | `worker_pool`, `worker_instance`, `otel_scope_name` | Current jobs being processed by a worker. Incremented after work is claimed, decremented on finish. |
| `polytope.worker.jobs.processed` | `polytope_worker_jobs_processed_total` | Counter | `outcome`, `worker_pool`, `worker_instance`, `otel_scope_name` | Worker jobs that reached a terminal completion path. |
| `polytope.worker.job.duration.seconds` | `polytope_worker_job_duration_seconds_bucket`, `_sum`, `_count` | Histogram | `outcome`, `worker_pool`, `worker_instance`, `le` on buckets, `otel_scope_name` | Wall time from worker job start to completion post, including processing and delivery. |
| `polytope.worker.job.processing.seconds` | `polytope_worker_job_processing_seconds_bucket`, `_sum`, `_count` | Histogram | `outcome`, `worker_pool`, `worker_instance`, `le` on buckets, `otel_scope_name` | Time spent inside the worker `Processor::process()` implementation only. |
| `polytope.worker.delivery.duration.seconds` | `polytope_worker_delivery_duration_seconds_bucket`, `_sum`, `_count` | Histogram | `outcome`, `worker_pool`, `worker_instance`, `le` on buckets, `otel_scope_name` | Time spent delivering the result body after processing, for example uploading to BOBS/S3 or posting direct completion. |
| `polytope.worker.delivery.bytes` | `polytope_worker_delivery_bytes_total` | Counter | `outcome`, `worker_pool`, `worker_instance`, `otel_scope_name` | Bytes delivered by the worker after encoding/compression, when the result body has non-zero size. |
