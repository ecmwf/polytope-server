use async_trait::async_trait;
use clap::Parser;
use polytope_worker_common::config::WorkerConfigFile;
use polytope_worker_common::{run_worker_loop, ProcessResult, Processor, WorkItem, WorkerConfig};
use serde::Deserialize;
use tracing::info;

const DEFAULT_CONFIG_PATH: &str = "/etc/polytope-worker/config.yaml";

#[derive(Debug, Deserialize)]
struct TestConfig {
    behaviour: Behaviour,
    #[serde(default = "default_content_type")]
    content_type: String,
}

fn default_content_type() -> String {
    "application/json".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Behaviour {
    Reject,
    Wait {
        duration_ms: u64,
    },
    Error,
    Echo,
    Dummy {
        #[serde(default = "default_dummy_count")]
        count: u64,
    },
}

fn default_dummy_count() -> u64 {
    10
}

struct BehaviourProcessor {
    config: TestConfig,
}

fn json_success(content_type: &str, payload: Vec<u8>) -> ProcessResult {
    let body = bytes::Bytes::from(payload);
    let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(body)));
    ProcessResult::success(content_type, Box::new(stream))
}

#[async_trait]
impl Processor for BehaviourProcessor {
    async fn process(&self, work: WorkItem) -> ProcessResult {
        match &self.config.behaviour {
            Behaviour::Reject => ProcessResult::reject("rejected by test worker"),

            Behaviour::Wait { duration_ms } => {
                tokio::time::sleep(std::time::Duration::from_millis(*duration_ms)).await;
                json_success(&self.config.content_type, b"{}".to_vec())
            }

            Behaviour::Error => ProcessResult::error("test error"),

            Behaviour::Echo => {
                let payload = serde_json::to_vec(&work.request).unwrap_or_default();
                json_success(&self.config.content_type, payload)
            }

            Behaviour::Dummy { count } => {
                let data: Vec<u64> = (1..=*count).collect();
                let payload = serde_json::to_vec(&data).unwrap_or_default();
                json_success(&self.config.content_type, payload)
            }
        }
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
        .cloned()
        .expect("config missing 'test' section");

    let test_config: TestConfig = serde_yml::from_value(test_section)
        .expect("failed to parse 'test' config section");

    info!(
        path = cli.config_path,
        behaviour = ?test_config.behaviour,
        content_type = test_config.content_type.as_str(),
        "loaded config"
    );

    run_worker_loop(
        WorkerConfig {
            broker_url: cli.broker_url,
            poll_timeout_ms: cli.poll_timeout_ms,
            heartbeat_interval: std::time::Duration::from_secs_f64(cli.heartbeat_secs),
            retry_backoff: std::time::Duration::from_secs(1),
            management_port: config.management_port,
        },
        config.delivery,
        BehaviourProcessor {
            config: test_config,
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    fn dummy_work() -> WorkItem {
        WorkItem {
            job_id: "test-1".into(),
            request: serde_json::json!({"collection": "era5", "level": 500}),
            user: serde_json::json!({}),
            metadata: serde_json::json!({}),
        }
    }

    fn processor(behaviour: Behaviour) -> BehaviourProcessor {
        BehaviourProcessor {
            config: TestConfig {
                behaviour,
                content_type: "application/json".to_string(),
            },
        }
    }

    async fn collect_success_body(result: ProcessResult) -> (String, Vec<u8>) {
        match result {
            ProcessResult::Success { content_type, body } => {
                let bytes = body
                    .try_fold(Vec::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk);
                        Ok(acc)
                    })
                    .await
                    .unwrap();
                (content_type, bytes)
            }
            other => panic!("expected Success, got {:?}", variant_name(&other)),
        }
    }

    fn variant_name(r: &ProcessResult) -> &'static str {
        match r {
            ProcessResult::Success { .. } => "Success",
            ProcessResult::Reject { .. } => "Reject",
            ProcessResult::Error { .. } => "Error",
        }
    }

    #[tokio::test]
    async fn reject_returns_reject() {
        let result = processor(Behaviour::Reject).process(dummy_work()).await;
        match result {
            ProcessResult::Reject { reason } => assert_eq!(reason, "rejected by test worker"),
            other => panic!("expected Reject, got {}", variant_name(&other)),
        }
    }

    #[tokio::test]
    async fn error_returns_test_error() {
        let result = processor(Behaviour::Error).process(dummy_work()).await;
        match result {
            ProcessResult::Error { message } => assert_eq!(message, "test error"),
            other => panic!("expected Error, got {}", variant_name(&other)),
        }
    }

    #[tokio::test]
    async fn echo_returns_request_json() {
        let work = dummy_work();
        let expected = serde_json::to_vec(&work.request).unwrap();
        let result = processor(Behaviour::Echo).process(work).await;
        let (content_type, body) = collect_success_body(result).await;
        assert_eq!(content_type, "application/json");
        assert_eq!(body, expected);
    }

    #[tokio::test]
    async fn dummy_returns_sequential_array() {
        let result = processor(Behaviour::Dummy { count: 5 })
            .process(dummy_work())
            .await;
        let (content_type, body) = collect_success_body(result).await;
        assert_eq!(content_type, "application/json");
        let parsed: Vec<u64> = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn dummy_default_count_is_10() {
        let config: TestConfig =
            serde_yml::from_str("behaviour:\n  type: dummy\n").unwrap();
        let result = BehaviourProcessor { config }.process(dummy_work()).await;
        let (_, body) = collect_success_body(result).await;
        let parsed: Vec<u64> = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed, (1..=10).collect::<Vec<u64>>());
    }

    #[tokio::test]
    async fn wait_returns_success_after_delay() {
        let start = std::time::Instant::now();
        let result = processor(Behaviour::Wait { duration_ms: 50 })
            .process(dummy_work())
            .await;
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() >= 40);
        let (_, body) = collect_success_body(result).await;
        assert_eq!(body, b"{}");
    }

    #[tokio::test]
    async fn config_content_type_is_honoured() {
        let p = BehaviourProcessor {
            config: TestConfig {
                behaviour: Behaviour::Echo,
                content_type: "application/octet-stream".to_string(),
            },
        };
        let result = p.process(dummy_work()).await;
        let (content_type, _) = collect_success_body(result).await;
        assert_eq!(content_type, "application/octet-stream");
    }
}
