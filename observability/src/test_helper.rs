use crate::formatter::{JsonFieldFormatter, OtelJsonFormatter};
use serde_json::Value;
use std::io;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
pub struct CapturedLogs {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    pub fn raw_lines(&self) -> Vec<String> {
        let bytes = self.buffer.lock().unwrap().clone();
        String::from_utf8_lossy(&bytes)
            .lines()
            .map(str::to_string)
            .collect()
    }
    pub fn json_lines(&self) -> Vec<Value> {
        self.raw_lines()
            .into_iter()
            .map(|line| {
                serde_json::from_str(&line).unwrap_or_else(|err| {
                    panic!("captured log line was not valid JSON: {err}; line={line}")
                })
            })
            .collect()
    }
    pub fn assert_required_fields(&self) {
        for line in self.json_lines() {
            assert!(
                line.get("timestamp").and_then(Value::as_str).is_some(),
                "missing timestamp: {line}"
            );
            assert!(
                line.get("severityText").and_then(Value::as_str).is_some(),
                "missing severityText: {line}"
            );
            assert!(
                line.get("severityNumber").and_then(Value::as_u64).is_some(),
                "missing severityNumber: {line}"
            );
            assert!(
                line.get("body").and_then(Value::as_str).is_some(),
                "missing body: {line}"
            );
            assert!(
                line.pointer("/resource/service.name")
                    .and_then(Value::as_str)
                    .is_some(),
                "missing resource.service.name: {line}"
            );
            assert!(
                line.pointer("/resource/service.version")
                    .and_then(Value::as_str)
                    .is_some(),
                "missing resource.service.version: {line}"
            );
            assert!(
                line.pointer("/attributes/code.target")
                    .and_then(Value::as_str)
                    .is_some(),
                "missing attributes.code.target: {line}"
            );
        }
    }
    pub fn assert_event_emitted(&self, event_name: &str) -> Value {
        self.json_lines()
            .into_iter()
            .find(|line| line["attributes"]["event.name"] == event_name)
            .unwrap_or_else(|| {
                panic!(
                    "event {event_name} was not emitted; events={:?}",
                    self.event_names()
                )
            })
    }
    pub fn events_named(&self, event_name: &str) -> Vec<Value> {
        self.json_lines()
            .into_iter()
            .filter(|line| line["attributes"]["event.name"] == event_name)
            .collect()
    }
    pub fn assert_no_substring(&self, substring: &str) {
        let raw = self.raw_lines().join("\n");
        assert!(
            !raw.contains(substring),
            "captured logs contained forbidden substring"
        );
    }
    fn event_names(&self) -> Vec<String> {
        self.json_lines()
            .iter()
            .filter_map(|v| v["attributes"]["event.name"].as_str().map(str::to_string))
            .collect()
    }
}

#[derive(Clone, Default)]
pub struct CaptureMakeWriter {
    logs: CapturedLogs,
}

impl<'a> MakeWriter<'a> for CaptureMakeWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        CaptureWriter {
            buffer: self.logs.buffer.clone(),
        }
    }
}

pub struct CaptureWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}
impl io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub type CaptureLayer = tracing_subscriber::fmt::Layer<
    tracing_subscriber::Registry,
    JsonFieldFormatter,
    OtelJsonFormatter,
    CaptureMakeWriter,
>;

pub fn capturing_subscriber(service_name: &'static str) -> (CaptureLayer, CapturedLogs) {
    let logs = CapturedLogs::default();
    let writer = CaptureMakeWriter { logs: logs.clone() };
    let layer = tracing_subscriber::fmt::layer()
        .event_format(OtelJsonFormatter::new(service_name))
        .fmt_fields(JsonFieldFormatter)
        .with_writer(writer)
        .with_ansi(false);
    (layer, logs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;
    #[test]
    fn captures_json_and_helpers_work() {
        let (layer, logs) = capturing_subscriber("svc");
        let sub = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(sub);
        tracing::info!(
            "event.name" = "startup.config.loaded",
            outcome = "success",
            "loaded"
        );
        logs.assert_required_fields();
        logs.assert_event_emitted("startup.config.loaded");
        logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
    }
}
