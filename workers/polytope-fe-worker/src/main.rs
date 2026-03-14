use async_trait::async_trait;
use clap::Parser;
use polytope_worker_common::{run_worker_loop, Completion, Processor, WorkItem, WorkerConfig};
use serde_json::json;
use tokio::process::Command;
use tokio_util::io::ReaderStream;

struct PolytopeProcessor {
    python: String,
    wrapper: String,
    config_path: String,
}

#[async_trait]
impl Processor for PolytopeProcessor {
    async fn process(&self, work: WorkItem) -> Completion {
        let payload = json!({
            "request": work.request,
            "user": work.user,
            "metadata": work.metadata,
            "config_path": self.config_path,
        });

        let child = Command::new(&self.python)
            .arg(&self.wrapper)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                let input = serde_json::to_vec(&payload).unwrap();
                if let Some(mut stdin) = child.stdin.take() {
                    tokio::spawn(async move {
                        use tokio::io::AsyncWriteExt;
                        let _ = stdin.write_all(&input).await;
                    });
                }
                Ok(child)
            });

        let mut child = match child {
            Ok(child) => child,
            Err(err) => {
                return Completion::error(format!("failed to spawn polytope wrapper: {err}"))
            }
        };

        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => return Completion::error("polytope wrapper did not expose stdout"),
        };

        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let _ = tokio::io::AsyncReadExt::read_to_end(
                    &mut tokio::io::BufReader::new(stderr),
                    &mut Vec::new(),
                )
                .await;
            });
        }

        tokio::spawn(async move {
            let _ = child.wait().await;
        });

        Completion::complete(
            "application/prs.coverage+json",
            None,
            reqwest::Body::wrap_stream(ReaderStream::new(stdout)),
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
    #[arg(long, default_value = "python3")]
    python: String,
    #[arg(long, default_value = "workers/polytope-fe-worker/run_polytope_worker.py")]
    wrapper: String,
    #[arg(long)]
    config_path: String,
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
        PolytopeProcessor {
            python: cli.python,
            wrapper: cli.wrapper,
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

    fn temp_path(name: &str, ext: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "polytope-worker-{name}-{}-{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            ext,
        ));
        path
    }

    #[tokio::test]
    async fn polytope_worker_uses_python_wrapper_contract() {
        let script = temp_path("wrapper", "py");
        std::fs::write(
            &script,
            r#"#!/usr/bin/env python3
import json, sys
payload = json.load(sys.stdin)
sys.stdout.write(json.dumps({"echo": payload["request"]}))
"#,
        )
        .unwrap();

        let result = PolytopeProcessor {
            python: "python3".into(),
            wrapper: script.display().to_string(),
            config_path: "/tmp/unused.yaml".into(),
        }
        .process(WorkItem {
            job_id: "job-1".into(),
            request: json!({"class": "od"}),
            user: json!({}),
            metadata: json!({}),
        })
        .await;

        std::fs::remove_file(script).ok();

        match result {
            Completion::Complete {
                content_type,
                content_length,
                ..
            } => {
                assert_eq!(content_type, "application/prs.coverage+json");
                assert_eq!(content_length, None);
            }
            other => panic!("expected streaming completion, got {other:?}"),
        }
    }
}
