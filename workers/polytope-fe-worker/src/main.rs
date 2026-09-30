// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use clap::Parser;
use polytope_worker_common::config::{DEFAULT_CONFIG_PATH, WorkerConfigFile};
use polytope_worker_common::gribjump::{
    ExtractMetrics, ExtractPlan, GribJumpExtractor, PathRequest,
};
use polytope_worker_common::{ProcessResult, Processor, WorkItem, WorkerConfig, run_worker_loop};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

#[derive(serde::Deserialize)]
struct PyStatus {
    ok: bool,
    #[serde(default)]
    timings: serde_json::Value,
    #[serde(default)]
    logs: Vec<PyLogRecord>,
    #[serde(default)]
    error: Option<PyError>,
    /// MIME type of the body. Absent => legacy CoverageJSON. `/chunks/v1`
    /// extract jobs report `application/octet-stream`.
    #[serde(default)]
    content_type: Option<String>,
}

const DEFAULT_CONTENT_TYPE: &str = "application/prs.coverage+json";

#[derive(serde::Deserialize, serde::Serialize)]
struct PyLogRecord {
    level: String,
    logger: String,
    message: String,
}

#[derive(serde::Deserialize)]
struct PyError {
    message: String,
}

enum PythonOutput {
    Bytes(Vec<u8>),
    ExtractPlan(Box<ExtractPlan>),
}

#[derive(serde::Deserialize)]
struct WarmPlan {
    paths: Vec<PathRequest>,
    grid_hash: String,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum LogSeverity {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogSeverity {
    fn from_python_level(level: &str) -> Self {
        match level.to_uppercase().as_str() {
            "CRITICAL" | "ERROR" => LogSeverity::Error,
            "WARNING" | "WARN" => LogSeverity::Warn,
            "INFO" => LogSeverity::Info,
            "DEBUG" => LogSeverity::Debug,
            _ => LogSeverity::Info,
        }
    }
}

fn emit_python_logs(job_id: &str, logs: &[PyLogRecord]) {
    if logs.is_empty() {
        return;
    }

    let max_severity = logs
        .iter()
        .map(|log| LogSeverity::from_python_level(&log.level))
        .max()
        .unwrap_or(LogSeverity::Info);

    let log_count = logs.len();
    let logs_json = serde_json::to_string(logs)
        .unwrap_or_else(|e| format!("[failed to serialize logs: {}]", e));

    match max_severity {
        LogSeverity::Error => {
            error!(
                job_id = %job_id,
                python_log_count = log_count,
                python_logs = %logs_json,
                "python worker logs"
            );
        }
        LogSeverity::Warn => {
            warn!(
                job_id = %job_id,
                python_log_count = log_count,
                python_logs = %logs_json,
                "python worker logs"
            );
        }
        LogSeverity::Info => {
            info!(
                job_id = %job_id,
                python_log_count = log_count,
                python_logs = %logs_json,
                "python worker logs"
            );
        }
        LogSeverity::Debug => {
            debug!(
                job_id = %job_id,
                python_log_count = log_count,
                python_logs = %logs_json,
                "python worker logs"
            );
        }
    }
}

struct PolytopeProcessor {
    config_path: String,
    extractor: Option<Arc<GribJumpExtractor>>,
}

#[async_trait]
impl Processor for PolytopeProcessor {
    async fn process(&self, work: WorkItem) -> ProcessResult {
        info!(job_id = %work.job_id, "processing request");

        let payload = json!({
            "request": work.request,
            "user": work.user,
            "metadata": work.metadata,
            "config_path": self.config_path,
            "job_id": work.job_id,
        });

        let payload_str = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(err) => {
                error!(job_id = %work.job_id, error = %err, "failed to serialize payload");
                return ProcessResult::error(format!("failed to serialize payload: {err}"));
            }
        };

        let job_id = work.job_id.clone();
        let result = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<(PythonOutput, String)> {
                let wrapper = py.import("run_polytope_worker")?;
                let result = wrapper.call_method1("process", (&payload_str,))?;
                let tuple = result.downcast::<PyTuple>().map_err(|e| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "expected (bytes|dict, str) from process(), got: {e}"
                    ))
                })?;
                let item0 = tuple.get_item(0)?;
                let output = if let Ok(py_bytes) = item0.downcast::<PyBytes>() {
                    PythonOutput::Bytes(py_bytes.as_bytes().to_vec())
                } else {
                    let json_module = py.import("json")?;
                    let plan_json: String =
                        json_module.call_method1("dumps", (&item0,))?.extract()?;
                    let plan = serde_json::from_str(&plan_json).map_err(|error| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "invalid Rust extract plan: {error}"
                        ))
                    })?;
                    PythonOutput::ExtractPlan(Box::new(plan))
                };
                let status_json: String = tuple.get_item(1)?.extract()?;
                Ok((output, status_json))
            })
        })
        .await;

        match result {
            Ok(Ok((output, status_json))) => {
                let mut status: PyStatus = match serde_json::from_str(&status_json) {
                    Ok(s) => s,
                    Err(err) => {
                        error!(job_id = %job_id, error = %err, status_json = %status_json, "failed to parse status_json");
                        return ProcessResult::error(format!(
                            "protocol error: failed to parse status_json: {err}"
                        ));
                    }
                };

                emit_python_logs(&job_id, &status.logs);
                if !status.ok {
                    let message = status.error.map(|e| e.message).unwrap_or_else(|| {
                        "python worker reported failure with no message".to_string()
                    });
                    return ProcessResult::error(message);
                }

                let (bytes, content_type) = match output {
                    PythonOutput::Bytes(bytes) => {
                        let content_type = status
                            .content_type
                            .filter(|ct| !ct.is_empty())
                            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string());
                        (bytes, content_type)
                    }
                    PythonOutput::ExtractPlan(plan) => {
                        let Some(extractor) = self.extractor.clone() else {
                            return ProcessResult::error(
                                "Python returned a Rust extract plan while native extraction is disabled",
                            );
                        };
                        let extraction_started = Instant::now();
                        let plan_profile = plan.profile.clone();
                        let native =
                            tokio::task::spawn_blocking(move || extractor.extract(&plan)).await;
                        let output = match native {
                            Ok(Ok(output)) => output,
                            Ok(Err(message)) => {
                                error!(job_id = %job_id, error = %message, "native GribJump extraction failed");
                                return ProcessResult::error(format!(
                                    "gribjump extraction failed: {message}"
                                ));
                            }
                            Err(error) => {
                                return ProcessResult::error(format!(
                                    "native extraction task join error: {error}"
                                ));
                            }
                        };
                        let rust_wall_ms = extraction_started.elapsed().as_secs_f64() * 1000.0;
                        update_native_timings(&mut status.timings, &output.metrics, rust_wall_ms);
                        emit_chunks_profile(&plan_profile, &output.metrics, rust_wall_ms);
                        (output.payload, "application/octet-stream".to_string())
                    }
                };

                let len = bytes.len() as u64;
                let timings = serde_json::to_string(&status.timings).unwrap_or_default();
                info!(job_id = %job_id, bytes = len, timings = %timings, "request completed");
                ProcessResult::success_bytes(content_type, bytes::Bytes::from(bytes))
            }
            Ok(Err(py_err)) => {
                error!(job_id = %job_id, error = %py_err, "python error");
                ProcessResult::error(format!("{py_err}"))
            }
            Err(join_err) => {
                error!(job_id = %job_id, error = %join_err, "task join error");
                ProcessResult::error(format!("task join error: {join_err}"))
            }
        }
    }
}

fn update_native_timings(
    timings: &mut serde_json::Value,
    metrics: &ExtractMetrics,
    rust_wall_ms: f64,
) {
    let Some(values) = timings.as_object_mut() else {
        return;
    };
    values.insert("extract_ms".to_string(), json!(metrics.extract_ms));
    values.insert("assemble_ms".to_string(), json!(metrics.assemble_ms));
    values.insert("shuffle_ms".to_string(), json!(metrics.shuffle_ms));
    values.insert("compress_ms".to_string(), json!(metrics.zstd_ms));
    values.insert("gj_subbatches".to_string(), json!(metrics.gj_subbatches));
    values.insert("gj_inflight".to_string(), json!(metrics.inflight));
    values.insert("raw_bytes".to_string(), json!(metrics.raw_bytes));
    values.insert("payload_bytes".to_string(), json!(metrics.payload_bytes));
    values.insert("proc".to_string(), json!("rust"));
    let retrieve_ms = values
        .get("retrieve_ms")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default()
        + rust_wall_ms;
    values.insert(
        "retrieve_ms".to_string(),
        json!((retrieve_ms * 10.0).round() / 10.0),
    );
    let total_ms = values
        .get("total_ms")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default()
        + rust_wall_ms;
    values.insert(
        "total_ms".to_string(),
        json!((total_ms * 10.0).round() / 10.0),
    );
}

fn emit_chunks_profile(
    profile: &polytope_worker_common::gribjump::PlanProfile,
    metrics: &ExtractMetrics,
    rust_wall_ms: f64,
) {
    let total_ms = profile.python_ms + rust_wall_ms;
    info!(
        "chunks-profile job={} status=ok phase=done proc=rust fields={} ranges={} points={} \
         dtype={} shuffle={} cache={}/{} fallback={} lookup_mode={} \
         lookup_fallbacks={} subbatches={}/{} inflight={} t_lookup={:.1}ms \
         t_parse={:.1}ms t_enum={:.1}ms t_extract={:.1}ms t_assemble={:.1}ms \
         t_shuffle={:.1}ms t_zstd={:.1}ms t_total={:.1}ms \
         raw_bytes={} bytes={} zstd_level={}",
        profile.job,
        profile.fields,
        profile.ranges,
        profile.points,
        profile.dtype,
        profile.shuffle,
        profile.cache_hits,
        profile.cache_misses,
        profile.fallbacks,
        profile.lookup_mode,
        profile.lookup_fallbacks,
        profile.lookup_subbatches,
        metrics.gj_subbatches,
        metrics.inflight,
        profile.lookup_ms,
        profile.parse_ms,
        profile.enum_ms,
        metrics.extract_ms,
        metrics.assemble_ms,
        metrics.shuffle_ms,
        metrics.zstd_ms,
        total_ms,
        metrics.raw_bytes,
        metrics.payload_bytes,
        std::env::var("POLYTOPE_CHUNKS_ZSTD_LEVEL").unwrap_or_else(|_| "3".to_string()),
    );
}

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:9001")]
    broker_url: String,
    #[arg(long, default_value_t = polytope_worker_common::DEFAULT_POLL_TIMEOUT_MS)]
    poll_timeout_ms: u64,
    #[arg(long, default_value_t = 10.0)]
    heartbeat_secs: f64,
    #[arg(long, default_value = "/app")]
    python_path: String,
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config_path: String,
    #[arg(long, default_value_t = 1)]
    worker_concurrency: usize,
}

fn resolved_worker_concurrency(cli_value: usize) -> usize {
    match std::env::var("POLYTOPE_WORKER_CONCURRENCY") {
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed >= 1 => parsed,
            _ => {
                tracing::warn!(value = %value, "ignoring invalid POLYTOPE_WORKER_CONCURRENCY");
                cli_value
            }
        },
        Err(std::env::VarError::NotPresent) => cli_value,
        Err(err) => {
            tracing::warn!(error = %err, "ignoring invalid POLYTOPE_WORKER_CONCURRENCY");
            cli_value
        }
    }
}

fn resolved_gj_inflight() -> usize {
    const DEFAULT: usize = 4;
    match std::env::var("POLYTOPE_CHUNKS_GJ_INFLIGHT") {
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed >= 1 => parsed,
            _ => {
                warn!(value = %value, "ignoring invalid POLYTOPE_CHUNKS_GJ_INFLIGHT");
                DEFAULT
            }
        },
        Err(std::env::VarError::NotPresent) => DEFAULT,
        Err(error) => {
            warn!(%error, "ignoring invalid POLYTOPE_CHUNKS_GJ_INFLIGHT");
            DEFAULT
        }
    }
}

fn native_extract_enabled() -> bool {
    std::env::var("POLYTOPE_CHUNKS_RUST_EXTRACT")
        .map(|value| value != "0")
        .unwrap_or(true)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    polytope_observability::init_tracing("polytope-worker-polytope-fe");
    let cli = Cli::parse();
    let worker_concurrency = resolved_worker_concurrency(cli.worker_concurrency);
    info!(
        worker_concurrency,
        poll_timeout_ms = cli.poll_timeout_ms,
        "resolved worker settings"
    );

    let config = WorkerConfigFile::load(&cli.config_path).unwrap_or_else(|err| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", config_path = %cli.config_path, error = %err, "failed to load config");
        std::process::exit(1);
    });
    tracing::info!("event.name" = "startup.config.loaded", outcome = "success", config_path = %cli.config_path, "config loaded");

    debug!(python_path = %cli.python_path, config_path = %cli.config_path, "initializing python interpreter");

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| -> PyResult<()> {
        let sys = py.import("sys")?;
        let path = sys.getattr("path")?;
        path.call_method1("insert", (0i32, &cli.python_path))?;

        let py_path: Vec<String> = path.extract()?;
        debug!(sys_path = ?py_path, "python sys.path configured");

        let wrapper = py.import("run_polytope_worker")?;
        debug!("run_polytope_worker module imported");

        wrapper.call_method1("_get_datasource", (&cli.config_path,))?;
        debug!("polytope datasource initialized");
        Ok(())
    })?;

    let extractor = if native_extract_enabled() {
        let gj_inflight = resolved_gj_inflight();
        let extractor = GribJumpExtractor::native(gj_inflight).map_err(std::io::Error::other)?;
        let warm_plan = Python::with_gil(|py| -> PyResult<WarmPlan> {
            let extract = py.import("extract")?;
            let plan = extract.call_method0("rust_warm_plan")?;
            let json_module = py.import("json")?;
            let plan_json: String = json_module.call_method1("dumps", (&plan,))?.extract()?;
            serde_json::from_str(&plan_json).map_err(|error| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid Rust warm plan: {error}"))
            })
        });
        match warm_plan {
            Ok(plan) => match extractor.warm(&plan.paths, &plan.grid_hash) {
                Ok(()) => info!(
                    endpoints = plan.paths.len(),
                    handles = gj_inflight,
                    "native GribJump handles warmed"
                ),
                Err(error) => {
                    warn!(error = %error, handles = gj_inflight, "native GribJump warm-up failed; first job will retry")
                }
            },
            Err(error) => {
                warn!(error = %error, "native GribJump warm-location lookup failed; first job will connect lazily")
            }
        }
        Some(Arc::new(extractor))
    } else {
        info!("native GribJump extraction disabled; using Python fallback");
        None
    };

    info!(broker_url = %cli.broker_url, "connecting to broker");

    run_worker_loop(
        WorkerConfig {
            broker_url: cli.broker_url,
            poll_timeout_ms: cli.poll_timeout_ms,
            heartbeat_interval: std::time::Duration::from_secs_f64(cli.heartbeat_secs),
            retry_backoff: std::time::Duration::from_secs(1),
            management_port: config.management_port,
            worker_concurrency,
        },
        config.delivery,
        PolytopeProcessor {
            config_path: cli.config_path,
            extractor,
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // These tests replace one process-global Python module and sys.path.
    static PYTHON_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn temp_dir() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "polytope-worker-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        path
    }

    #[tokio::test]
    async fn pyo3_round_trip() {
        let _python_guard = PYTHON_TEST_LOCK.lock().await;
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("run_polytope_worker.py"),
            r#"
import json

def process(payload_json):
    payload = json.loads(payload_json)
    # Fail if metadata is not present
    assert "metadata" in payload, "metadata field missing from PyO3 payload"
    # Fail if metadata value is not passed through
    assert payload["metadata"].get("test_key") == "test_value", "metadata content not preserved"
    # The job id is forwarded so the extract path can tag its profile line
    assert payload.get("job_id") == "job-1", "job_id missing from PyO3 payload"
    output = json.dumps({"echo": payload["request"], "metadata": payload["metadata"]}).encode("utf-8")
    status = {
        "ok": True,
        "timings": {"total_ms": 0},
        "logs": [{"level": "INFO", "logger": "root", "message": "hello from test"}],
        "error": None
    }
    return (output, json.dumps(status))
"#,
        )
        .unwrap();

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            // Clear any previously cached module
            let modules = sys.getattr("modules").unwrap();
            let _ = modules.call_method1("pop", ("run_polytope_worker",));
            let path = sys.getattr("path").unwrap();
            path.call_method1("insert", (0i32, dir.display().to_string()))
                .unwrap();
        });

        let processor = PolytopeProcessor {
            config_path: "/tmp/unused.yaml".into(),
            extractor: None,
        };

        let result = processor
            .process(WorkItem {
                job_id: "job-1".into(),
                request: json!({"class": "od"}),
                user: json!({}),
                metadata: json!({"test_key": "test_value"}),
                callback_url: None,
            })
            .await;

        std::fs::remove_dir_all(&dir).ok();

        match result {
            ProcessResult::Success { content_type, .. } => {
                assert_eq!(content_type, "application/prs.coverage+json");
            }
            ProcessResult::Reject { reason } => panic!("expected success, got reject: {reason}"),
            ProcessResult::Error { message } => panic!("expected success, got error: {message}"),
        }
    }

    #[tokio::test]
    async fn pyo3_error_handling() {
        let _python_guard = PYTHON_TEST_LOCK.lock().await;
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        // Use a different temp directory to ensure no module conflicts
        std::fs::write(
            dir.join("run_polytope_worker.py"),
            r#"
import json

def process(payload_json):
    status = {
        "ok": False,
        "timings": {},
        "logs": [{"level": "ERROR", "logger": "root", "message": "test error log"}],
        "error": {"message": "simulated failure"}
    }
    return (b"", json.dumps(status))
"#,
        )
        .unwrap();

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let modules = sys.getattr("modules").unwrap();
            let _ = modules.call_method1("pop", ("run_polytope_worker",));
            // Remove old path entries
            let path = sys.getattr("path").unwrap();
            let path_list: Vec<String> = path.extract().unwrap();
            for p in path_list.iter().rev() {
                if p.contains("polytope-worker-test") {
                    let _ = path.call_method1("remove", (p,));
                }
            }
            path.call_method1("insert", (0i32, dir.display().to_string()))
                .unwrap();
        });

        let processor = PolytopeProcessor {
            config_path: "/tmp/unused.yaml".into(),
            extractor: None,
        };

        let result = processor
            .process(WorkItem {
                job_id: "job-2".into(),
                request: json!({"class": "od"}),
                user: json!({}),
                metadata: json!({}),
                callback_url: None,
            })
            .await;

        std::fs::remove_dir_all(&dir).ok();

        match result {
            ProcessResult::Error { message } => {
                assert_eq!(message, "simulated failure");
            }
            ProcessResult::Success { .. } => panic!("expected error, got success"),
            ProcessResult::Reject { reason } => panic!("expected error, got reject: {reason}"),
        }
    }
    #[tokio::test]
    async fn pyo3_content_type_passthrough() {
        let _python_guard = PYTHON_TEST_LOCK.lock().await;
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("run_polytope_worker.py"),
            r#"
import json

def process(payload_json):
    status = {
        "ok": True,
        "timings": {},
        "logs": [],
        "error": None,
        "content_type": "application/octet-stream",
    }
    return (b"\x28\xb5\x2f\xfd", json.dumps(status))
"#,
        )
        .unwrap();

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let modules = sys.getattr("modules").unwrap();
            let _ = modules.call_method1("pop", ("run_polytope_worker",));
            let path = sys.getattr("path").unwrap();
            let path_list: Vec<String> = path.extract().unwrap();
            for p in path_list.iter().rev() {
                if p.contains("polytope-worker-test") {
                    let _ = path.call_method1("remove", (p,));
                }
            }
            path.call_method1("insert", (0i32, dir.display().to_string()))
                .unwrap();
        });

        let processor = PolytopeProcessor {
            config_path: "/tmp/unused.yaml".into(),
            extractor: None,
        };

        let result = processor
            .process(WorkItem {
                job_id: "job-3".into(),
                request: json!({"class": "d1", "extract": {"ranges": [[0, 1]]}}),
                user: json!({}),
                metadata: json!({}),
                callback_url: None,
            })
            .await;

        std::fs::remove_dir_all(&dir).ok();

        match result {
            ProcessResult::Success { content_type, .. } => {
                assert_eq!(content_type, "application/octet-stream");
            }
            ProcessResult::Reject { reason } => panic!("expected success, got reject: {reason}"),
            ProcessResult::Error { message } => panic!("expected success, got error: {message}"),
        }
    }
}
