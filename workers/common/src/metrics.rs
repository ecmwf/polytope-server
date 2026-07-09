// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Worker lifecycle metrics.
//!
//! Uses the OpenTelemetry global meter provider. All instruments are
//! no-ops when no provider is installed (tests, local dev).

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::{KeyValue, global};

const METER_NAME: &str = "polytope.worker";

/// Install a Prometheus-backed global meter provider for this worker process.
///
/// The returned provider must be kept alive for the lifetime of the process.
pub fn init_meter_provider() -> (
    opentelemetry_sdk::metrics::SdkMeterProvider,
    prometheus::Registry,
) {
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("prometheus exporter should build");

    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", "polytope-worker"),
            KeyValue::new(
                "service.instance.id",
                std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            ),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new(
                "deployment.environment",
                std::env::var("POLYTOPE_ENV").unwrap_or_else(|_| "unknown".into()),
            ),
            KeyValue::new(
                "worker.pool",
                std::env::var("POLYTOPE_WORKER_POOL").unwrap_or_else(|_| "unknown".into()),
            ),
        ])
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(exporter)
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    (provider, registry)
}

struct Instruments {
    jobs_processed: Counter<u64>,
    job_duration: Histogram<f64>,
    job_processing: Histogram<f64>,
    jobs_active: UpDownCounter<i64>,
    polls: Counter<u64>,
    delivery_duration: Histogram<f64>,
    delivery_bytes: Counter<u64>,
}

fn meter() -> Meter {
    global::meter(METER_NAME)
}

fn instruments() -> &'static Instruments {
    static INSTANCE: OnceLock<Instruments> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let m = meter();
        Instruments {
            jobs_processed: m.u64_counter("polytope.worker.jobs.processed").build(),
            job_duration: m
                .f64_histogram("polytope.worker.job.duration.seconds")
                .build(),
            job_processing: m
                .f64_histogram("polytope.worker.job.processing.seconds")
                .build(),
            jobs_active: m.i64_up_down_counter("polytope.worker.jobs.active").build(),
            polls: m.u64_counter("polytope.worker.polls").build(),
            delivery_duration: m
                .f64_histogram("polytope.worker.delivery.duration.seconds")
                .build(),
            delivery_bytes: m.u64_counter("polytope.worker.delivery.bytes").build(),
        }
    })
}

fn common_attrs() -> &'static [KeyValue] {
    static INSTANCE: OnceLock<Vec<KeyValue>> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        vec![
            KeyValue::new(
                "worker_pool",
                std::env::var("POLYTOPE_WORKER_POOL").unwrap_or_else(|_| "unknown".into()),
            ),
            KeyValue::new(
                "worker_instance",
                std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            ),
        ]
    })
}

fn attrs_with(extra: &[KeyValue]) -> Vec<KeyValue> {
    let base = common_attrs();
    let mut attrs = Vec::with_capacity(base.len() + extra.len());
    attrs.extend_from_slice(base);
    attrs.extend_from_slice(extra);
    attrs
}

pub fn record_poll(result: &str) {
    instruments().polls.add(
        1,
        &attrs_with(&[KeyValue::new("result", result.to_owned())]),
    );
}

pub fn record_job_started() {
    instruments().jobs_active.add(1, common_attrs());
}

pub fn record_job_finished(
    outcome: &str,
    duration_secs: f64,
    processing_secs: f64,
    delivery_secs: f64,
    delivered_bytes: u64,
) {
    let i = instruments();
    let attrs = attrs_with(&[KeyValue::new("outcome", outcome.to_owned())]);
    i.jobs_active.add(-1, common_attrs());
    i.jobs_processed.add(1, &attrs);
    i.job_duration.record(duration_secs, &attrs);
    i.job_processing.record(processing_secs, &attrs);
    if delivery_secs > 0.0 {
        i.delivery_duration.record(delivery_secs, &attrs);
    }
    if delivered_bytes > 0 {
        i.delivery_bytes.add(delivered_bytes, &attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_functions_are_noop_without_provider() {
        record_poll("work");
        record_poll("empty");
        record_poll("error");
        record_job_started();
        record_job_finished("success", 1.0, 0.5, 0.3, 1024);
    }

    /// Verify that the Prometheus exporter produces single `_total` suffixes
    /// for counters, not double `_total_total`. This catches the bug where
    /// OTel instrument names ending in `.total` get an extra `_total` from
    /// the exporter.
    #[test]
    fn rendered_counter_names_have_single_total_suffix() {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::SdkMeterProvider;
        use prometheus::Encoder as _;

        let registry = prometheus::Registry::new();
        let reader = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .expect("exporter builds");
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter(METER_NAME);

        // Create counters with the same names used in production
        meter
            .u64_counter("polytope.worker.jobs.processed")
            .build()
            .add(1, &[]);
        meter
            .u64_counter("polytope.worker.polls")
            .build()
            .add(1, &[]);
        meter
            .u64_counter("polytope.worker.delivery.bytes")
            .build()
            .add(1, &[]);

        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .expect("encode");
        let rendered = String::from_utf8(buf).expect("utf8");

        // Each counter should have exactly one _total suffix
        assert!(
            rendered.contains("polytope_worker_jobs_processed_total"),
            "expected single-total counter, got:\n{rendered}"
        );
        assert!(
            rendered.contains("polytope_worker_polls_total"),
            "expected single-total counter, got:\n{rendered}"
        );
        assert!(
            rendered.contains("polytope_worker_delivery_bytes_total"),
            "expected single-total counter, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("_total_total"),
            "counter must not be double-suffixed, got:\n{rendered}"
        );
    }
}
