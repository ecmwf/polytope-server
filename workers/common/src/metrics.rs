//! Worker lifecycle metrics.
//!
//! Uses the OpenTelemetry global meter provider. All instruments are
//! no-ops when no provider is installed (tests, local dev).

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::{KeyValue, global};

const METER_NAME: &str = "polytope.worker";

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
            jobs_processed: m.u64_counter("polytope.worker.jobs.processed.total").build(),
            job_duration: m.f64_histogram("polytope.worker.job.duration.seconds").build(),
            job_processing: m.f64_histogram("polytope.worker.job.processing.seconds").build(),
            jobs_active: m.i64_up_down_counter("polytope.worker.jobs.active").build(),
            polls: m.u64_counter("polytope.worker.polls.total").build(),
            delivery_duration: m.f64_histogram("polytope.worker.delivery.duration.seconds").build(),
            delivery_bytes: m.u64_counter("polytope.worker.delivery.bytes.total").build(),
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
    instruments()
        .polls
        .add(1, &attrs_with(&[KeyValue::new("result", result.to_owned())]));
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
}
