use async_trait::async_trait;
use clap::Parser;
use polytope_worker_common::config::{DEFAULT_CONFIG_PATH, WorkerConfigFile};
use polytope_worker_common::{ProcessResult, Processor, WorkItem, WorkerConfig, run_worker_loop};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use serde_json::json;
use tracing::{debug, error, info};

struct PolytopeProcessor {
    config_path: String,
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
            Python::with_gil(|py| -> PyResult<(Vec<u8>, String)> {
                let wrapper = py.import("run_polytope_worker")?;
                let result = wrapper.call_method1("process", (&payload_str,))?;
                let tuple = result.downcast::<PyTuple>().map_err(|e| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "expected (bytes, str) from process(), got: {e}"
                    ))
                })?;
                let item0 = tuple.get_item(0)?;
                let py_bytes = item0.downcast::<PyBytes>().map_err(|e| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "expected bytes at index 0, got: {e}"
                    ))
                })?;
                let timings: String = tuple.get_item(1)?.extract()?;
                Ok((py_bytes.as_bytes().to_vec(), timings))
            })
        })
        .await;

        match result {
            Ok(Ok((bytes, timings))) => {
                let len = bytes.len() as u64;
                info!(job_id = %job_id, bytes = len, timings = %timings, "request completed");
                let stream =
                    futures::stream::once(futures::future::ready(
                        Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(bytes)),
                    ));
                ProcessResult::success("application/prs.coverage+json", Box::new(stream))
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
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("run_polytope_worker.py"),
            r#"
import json

def process(payload_json):
    payload = json.loads(payload_json)
    output = json.dumps({"echo": payload["request"]}).encode("utf-8")
    return (output, '{"total_ms": 0}')
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
        };

        let result = processor
            .process(WorkItem {
                job_id: "job-1".into(),
                request: json!({"class": "od"}),
                user: json!({}),
                metadata: json!({}),
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
}
