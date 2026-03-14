use async_trait::async_trait;
use clap::Parser;
use polytope_worker_common::{run_worker_loop, Completion, Processor, WorkItem, WorkerConfig};

struct MarsProcessor;

#[async_trait]
impl Processor for MarsProcessor {
    async fn process(&self, _work: WorkItem) -> Completion {
        Completion::error("mars-worker is not implemented yet")
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
    async fn mars_worker_is_explicit_stub() {
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
