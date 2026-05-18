use crate::k8s::NodePortManager;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use mars_client::{Error as MarsError, MarsClient};
use polytope_worker_common::config::{DEFAULT_CONFIG_PATH, WorkerConfigFile};
use polytope_worker_common::{ProcessResult, Processor, WorkItem, WorkerConfig, run_worker_loop};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

mod convert;
mod k8s;
mod port_cleanup;

struct MarsProcessor {
    /// Port the C++ MARS client binds for DHS callbacks. We forcibly close any
    /// leaked listener on this port between retrieves; see `port_cleanup`.
    local_port: u16,
    env_lock: std::sync::Arc<std::sync::Mutex<()>>,
}

#[async_trait]
impl Processor for MarsProcessor {
    async fn process(&self, work: WorkItem) -> ProcessResult {
        let request_map = match convert::json_to_request(&work.request) {
            Ok(m) => m,
            Err(msg) => return ProcessResult::error(msg),
        };

        let mars_email = work.user["attributes"]["ecmwf-email"]
            .as_str()
            .unwrap_or("no-email")
            .to_owned();
        let mars_token = work.user["attributes"]["ecmwf-apikey"]
            .as_str()
            .unwrap_or("no-api-key")
            .to_owned();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
        let local_port = self.local_port;
        let env_lock = self.env_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _env_guard = env_lock.lock().expect("MARS environment lock poisoned");
            // SAFETY: this mutex serializes all per-request mutation of process environment
            // variables used by the MARS client.
            unsafe {
                std::env::set_var("MARS_USER_EMAIL", &mars_email);
                std::env::set_var("MARS_USER_TOKEN", &mars_token);
            }

            let mut client = match MarsClient::new() {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.blocking_send(Err(std::io::Error::other(e)));
                    return;
                }
            };
            let mut stream = match client.retrieve(request_map) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.blocking_send(Err(std::io::Error::other(e)));
                    return;
                }
            };
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                match stream.read_bytes(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if tx
                            .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                            .is_err()
                        {
                            warn!("client disconnected, aborting mars stream");
                            stream.close();
                            return;
                        }
                    }
                    Err(MarsError::Invalidated { offset }) => {
                        warn!(offset, "mars stream invalidated — unrecoverable");
                        let _ = tx.blocking_send(Err(std::io::Error::other(format!(
                            "stream invalidated at byte offset {offset}"
                        ))));
                        break;
                    }
                    Err(e) => {
                        warn!("mars stream error: {e}");
                        let _ = tx.blocking_send(Err(std::io::Error::other(e)));
                        break;
                    }
                }
            }
            stream.close();

            // mars-client-cpp leaks the DHS callback listener (and, on the
            // "Data not found" path, the accepted CLOSE_WAIT data socket);
            // force-close any fd in our process still bound to `local_port`,
            // otherwise the next retrieve fails with `Address already in
            // use`. Remove this once the C++ lifecycle is fixed upstream.
            // Tracked: https://jira.ecmwf.int/projects/MARSC/issues/MARSC-468
            match port_cleanup::close_leaked_listeners(local_port) {
                Ok(0) => {}
                Ok(n) => tracing::info!(
                    closed = n,
                    port = local_port,
                    "reclaimed leaked MARS DHS callback listener(s)"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    port = local_port,
                    "failed to scan /proc for leaked MARS DHS callback listeners"
                ),
            }
        });

        let stream = ReceiverStream::new(rx);
        ProcessResult::success("application/x-grib", Box::new(stream))
    }
}

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:9001")]
    broker_url: String,
    #[arg(long, default_value_t = 30000)]
    poll_timeout_ms: u64,
    #[arg(long, default_value_t = 10.0)]
    heartbeat_secs: f64,
    #[arg(long, default_value_t = 8100)]
    mars_dhs_local_port: u16,
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
                warn!(value = %value, "ignoring invalid POLYTOPE_WORKER_CONCURRENCY");
                cli_value
            }
        },
        Err(std::env::VarError::NotPresent) => cli_value,
        Err(err) => {
            warn!(error = %err, "ignoring invalid POLYTOPE_WORKER_CONCURRENCY");
            cli_value
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    polytope_observability::init_tracing("polytope-worker-mars");
    mars_client::log_bridge::init();

    let cli = Cli::parse();
    let worker_concurrency = resolved_worker_concurrency(cli.worker_concurrency);
    info!(worker_concurrency, "resolved worker concurrency");

    let config = WorkerConfigFile::load(&cli.config_path).unwrap_or_else(|err| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", config_path = %cli.config_path, error = %err, "failed to load config");
        std::process::exit(1);
    });
    tracing::info!("event.name" = "startup.config.loaded", outcome = "success", config_path = %cli.config_path, "config loaded");

    let manager = NodePortManager::new(cli.mars_dhs_local_port).await?;
    // SAFETY: set once at startup before run_worker_loop spawns any processing threads;
    // these env vars are never mutated afterwards.
    unsafe {
        std::env::set_var("MARS_DHS_LOCALPORT", manager.local_port().to_string());
        std::env::set_var("MARS_DHS_CALLBACK_HOST", manager.node_name());
        std::env::set_var("MARS_DHS_CALLBACK_PORT", manager.node_port().to_string());
    }
    tracing::debug!(
        node_port = manager.node_port(),
        "NodePort service created, MARS DHS callback configured"
    );

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
        MarsProcessor {
            local_port: manager.local_port(),
            env_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
        },
    )
    .await?;

    if let Err(e) = manager.cleanup().await {
        tracing::warn!(error = %e, "Failed to cleanup NodePort service on shutdown");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn process_returns_error_for_invalid_request() {
        let processor = MarsProcessor {
            local_port: 8100,
            env_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
        };
        let result = processor
            .process(WorkItem {
                job_id: "job-1".into(),
                request: json!({}),
                user: json!({}),
                metadata: json!({}),
            })
            .await;
        assert!(matches!(result, ProcessResult::Error { .. }));
    }
}
