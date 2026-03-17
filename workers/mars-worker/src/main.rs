use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use mars_client::{Error as MarsError, MarsClient};
use polytope_worker_common::{run_worker_loop, Completion, Processor, WorkItem, WorkerConfig};
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

mod convert;

struct MarsProcessor;

#[async_trait]
impl Processor for MarsProcessor {
    async fn process(&self, work: WorkItem) -> Completion {
        let request_map = match convert::json_to_request(&work.request) {
            Ok(m) => m,
            Err(msg) => return Completion::error(msg),
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
        tokio::task::spawn_blocking(move || {
            // SAFETY: run_worker_loop processes one request at a time — no concurrent set_var.
            unsafe {
                std::env::set_var("MARS_USER_EMAIL", &mars_email);
                std::env::set_var("MARS_USER_TOKEN", &mars_token);
                if let Ok(node) = std::env::var("K8S_NODE_NAME") {
                    std::env::set_var("MARS_DHS_CALLBACK_HOST", &node);
                    std::env::set_var("MARS_DHS_LOCALHOST", &node);
                }
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
        });

        Completion::complete(
            "application/x-grib",
            None,
            reqwest::Body::wrap_stream(ReceiverStream::new(rx)),
        )
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    run_worker_loop(
        WorkerConfig {
            broker_url: cli.broker_url,
            poll_timeout_ms: cli.poll_timeout_ms,
            heartbeat_interval: std::time::Duration::from_secs_f64(cli.heartbeat_secs),
            retry_backoff: std::time::Duration::from_secs(1),
        },
        MarsProcessor,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn process_returns_error_for_invalid_request() {
        let result = MarsProcessor
            .process(WorkItem {
                job_id: "job-1".into(),
                request: json!({}),
                user: json!({}),
                metadata: json!({}),
            })
            .await;
        assert!(matches!(result, Completion::Error { .. }));
    }
}
