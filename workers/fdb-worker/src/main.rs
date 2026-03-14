use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use polytope_worker_common::{run_worker_loop, Completion, Processor, WorkItem, WorkerConfig};
use rsfdb::{request::Request, FDB};
use tokio_stream::wrappers::ReceiverStream;

struct FdbProcessor {
    fdb_config_path: Option<String>,
}

#[async_trait]
impl Processor for FdbProcessor {
    async fn process(&self, work: WorkItem) -> Completion {
        let fdb_config = match &self.fdb_config_path {
            Some(path) => match std::fs::read_to_string(path) {
                Ok(config) => Some(config),
                Err(err) => return Completion::error(format!("failed to read FDB config: {err}")),
            },
            None => None,
        };

        let request = work.request;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
        tokio::task::spawn_blocking(move || {
            let fdb = match FDB::new(fdb_config.as_deref()) {
                Ok(fdb) => fdb,
                Err(err) => {
                    let _ = tx.blocking_send(Err(std::io::Error::other(err)));
                    return;
                }
            };
            let request = match Request::from_json(request) {
                Ok(request) => request,
                Err(err) => {
                    let _ = tx.blocking_send(Err(std::io::Error::other(err)));
                    return;
                }
            };
            let mut reader = match fdb.retrieve(&request) {
                Ok(reader) => reader,
                Err(err) => {
                    let _ = tx.blocking_send(Err(std::io::Error::other(err)));
                    return;
                }
            };
            let mut buffer = vec![0u8; 64 * 1024];
            loop {
                match std::io::Read::read(&mut reader, &mut buffer) {
                    Ok(0) => break,
                    Ok(size) => {
                        if tx
                            .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..size])))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.blocking_send(Err(err));
                        break;
                    }
                }
            }
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
    #[arg(long)]
    fdb_config_path: Option<String>,
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
        FdbProcessor {
            fdb_config_path: cli.fdb_config_path,
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn fdb_worker_surfaces_config_errors() {
        let result = FdbProcessor {
            fdb_config_path: Some("/definitely/missing/fdb.yaml".into()),
        }
        .process(WorkItem {
            job_id: "job-1".into(),
            request: json!({"class": "od"}),
            user: json!({}),
            metadata: json!({}),
        })
        .await;

        match result {
            Completion::Error { message } => assert!(message.contains("failed to read FDB config")),
            other => panic!("expected error completion, got {other:?}"),
        }
    }
}
