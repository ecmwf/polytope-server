use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use polytope_worker_common::{
    run_worker_loop, ProcessResult, Processor, WorkItem, WorkerConfig,
};
use polytope_worker_common::config::WorkerConfigFile;
use rsfdb::{request::Request, FDB};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

const DEFAULT_CONFIG_PATH: &str = "/etc/polytope-worker/config.yaml";

struct FdbProcessor {
    fdb_config: String,
}

#[async_trait]
impl Processor for FdbProcessor {
    async fn process(&self, work: WorkItem) -> ProcessResult {
        let fdb_config = self.fdb_config.clone();

        let request = work.request;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
        tokio::task::spawn_blocking(move || {
            let fdb = match FDB::new(Some(&fdb_config)) {
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
            let mut buffer = vec![0u8; 256 * 1024];
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
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    let config = WorkerConfigFile::load(&cli.config_path)
        .unwrap_or_else(|err| panic!("failed to load config at {}: {err}", cli.config_path));

    let fdb_section = config
        .section("fdb")
        .expect("config missing 'fdb' section");
    let fdb_config = serde_yml::to_string(fdb_section)
        .expect("failed to serialize fdb config section");

    info!(path = cli.config_path, fdb_config = fdb_config.as_str(), "loaded config");

    run_worker_loop(
        WorkerConfig {
            broker_url: cli.broker_url,
            poll_timeout_ms: cli.poll_timeout_ms,
            heartbeat_interval: std::time::Duration::from_secs_f64(cli.heartbeat_secs),
            retry_backoff: std::time::Duration::from_secs(1),
            management_port: config.management_port,
        },
        config.delivery,
        FdbProcessor { fdb_config },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn missing_config_panics() {
        let result = std::panic::catch_unwind(|| {
            polytope_worker_common::config::WorkerConfigFile::load("/definitely/missing/config.yaml")
                .unwrap_or_else(|err| panic!("failed to load config: {err}"));
        });
        assert!(result.is_err());
    }
}
