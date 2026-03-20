use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use polytope_worker_common::config::WorkerConfigFile;
use polytope_worker_common::{run_worker_loop, ProcessResult, Processor, WorkItem, WorkerConfig};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

const DEFAULT_CONFIG_PATH: &str = "/etc/polytope-worker/config.yaml";

struct CommandProcessor {
    command: String,
    content_type: String,
}

#[async_trait]
impl Processor for CommandProcessor {
    async fn process(&self, work: WorkItem) -> ProcessResult {
        let request_json = match serde_json::to_string(&work.request) {
            Ok(j) => j,
            Err(err) => return ProcessResult::error(format!("failed to serialize request: {err}")),
        };

        let mut child = match tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .env("JOB_ID", &work.job_id)
            .env("REQUEST_JSON", &request_json)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(err) => return ProcessResult::error(format!("failed to spawn command: {err}")),
        };

        if let Some(mut stdin) = child.stdin.take() {
            let json = request_json.clone();
            tokio::spawn(async move {
                let _ = stdin.write_all(json.as_bytes()).await;
                let _ = stdin.shutdown().await;
            });
        }

        let mut stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => return ProcessResult::error("failed to capture stdout".to_string()),
        };

        let stderr_handle = child.stderr.take().map(|mut stderr| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut buf).await;
                String::from_utf8_lossy(&buf).to_string()
            })
        });

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if tx.send(Ok(Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                }
            }

            match child.wait().await {
                Ok(status) if !status.success() => {
                    let stderr_output = if let Some(handle) = stderr_handle {
                        handle.await.unwrap_or_default()
                    } else {
                        String::new()
                    };
                    warn!(
                        exit_code = status.code(),
                        stderr = stderr_output.as_str(),
                        "command exited with non-zero status"
                    );
                }
                Err(err) => {
                    warn!(error = %err, "failed to wait for command");
                }
                _ => {}
            }
        });

        let stream = ReceiverStream::new(rx);
        ProcessResult::success(&self.content_type, Box::new(stream))
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
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let config = WorkerConfigFile::load(&cli.config_path)
        .unwrap_or_else(|err| panic!("failed to load config at {}: {err}", cli.config_path));

    let test_section = config
        .section("test")
        .expect("config missing 'test' section");

    let command = test_section["command"]
        .as_str()
        .expect("config missing 'test.command'")
        .to_string();

    let content_type = test_section
        .get("content_type")
        .and_then(|v| v.as_str())
        .unwrap_or("application/octet-stream")
        .to_string();

    info!(
        path = cli.config_path,
        command = command.as_str(),
        content_type = content_type.as_str(),
        "loaded config"
    );

    run_worker_loop(
        WorkerConfig {
            broker_url: cli.broker_url,
            poll_timeout_ms: cli.poll_timeout_ms,
            heartbeat_interval: std::time::Duration::from_secs_f64(cli.heartbeat_secs),
            retry_backoff: std::time::Duration::from_secs(1),
        },
        config.delivery,
        CommandProcessor {
            command,
            content_type,
        },
    )
    .await?;
    Ok(())
}
