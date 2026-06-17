use clap::Parser;
use polytope_worker_common::config::{DEFAULT_CONFIG_PATH, WorkerConfigFile};
use polytope_worker_common::{WorkerConfig, run_worker_loop};
use test_worker::*;
use tracing::{debug, info, warn};

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:9001")]
    broker_url: String,
    #[arg(long, default_value_t = polytope_worker_common::DEFAULT_POLL_TIMEOUT_MS)]
    poll_timeout_ms: u64,
    #[arg(long, default_value_t = 10.0)]
    heartbeat_secs: f64,
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
    polytope_observability::init_tracing("polytope-worker-test");
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

    let test_section = config
        .section("test")
        .cloned()
        .expect("config missing 'test' section");

    let test_config: TestConfig =
        serde_yml::from_value(test_section).expect("failed to parse 'test' config section");

    debug!(path = cli.config_path, behaviour = ?test_config.behaviour, content_type = test_config.content_type.as_str(), "loaded test config section");

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
    use polytope_worker_common::{ProcessResult, Processor, WorkItem};

    fn dummy_work() -> WorkItem {
        WorkItem {
            job_id: "test-1".into(),
            request: serde_json::json!({"collection": "era5", "level": 500}),
            user: serde_json::json!({}),
            metadata: serde_json::json!({}),
        }
    }

    fn stress_work(delay_ms: u64, response_bytes: u64) -> WorkItem {
        WorkItem {
            job_id: "stress-1".into(),
            request: serde_json::json!({
                "stress": {
                    "delay_ms": delay_ms,
                    "response_bytes": response_bytes,
                }
            }),
            user: serde_json::json!({}),
            metadata: serde_json::json!({}),
        }
    }

    fn stress_work_with_chunk(delay_ms: u64, response_bytes: u64, chunk_bytes: u64) -> WorkItem {
        WorkItem {
            job_id: "stress-1".into(),
            request: serde_json::json!({
                "stress": {
                    "delay_ms": delay_ms,
                    "response_bytes": response_bytes,
                    "chunk_bytes": chunk_bytes,
                }
            }),
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

    /// Collect all stream chunks into a single flat byte vec.
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

    /// Collect all stream chunks, preserving individual chunk boundaries.
    async fn collect_success_chunks(result: ProcessResult) -> (String, Vec<Vec<u8>>) {
        match result {
            ProcessResult::Success { content_type, body } => {
                let chunks = body.try_collect::<Vec<_>>().await.unwrap();
                let chunks: Vec<Vec<u8>> = chunks.into_iter().map(|b| b.to_vec()).collect();
                (content_type, chunks)
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
        let config: TestConfig = serde_yml::from_str("behaviour:\n  type: dummy\n").unwrap();
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
    async fn stress_uses_request_delay_and_response_size() {
        let start = std::time::Instant::now();
        let result = processor(Behaviour::Stress {
            default_delay_ms: 0,
            default_response_bytes: 10,
            default_chunk_bytes: default_stress_chunk_bytes(),
            max_delay_ms: 1000,
            max_response_bytes: 1000,
            max_chunk_bytes: default_stress_max_chunk_bytes(),
        })
        .process(stress_work(50, 123))
        .await;
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() >= 40);
        let (content_type, body) = collect_success_body(result).await;
        assert_eq!(content_type, "application/json");
        assert_eq!(body.len(), 123);
        assert_eq!(&body[..4], b"xxxx");
    }

    #[tokio::test]
    async fn stress_caps_request_values() {
        let result = processor(Behaviour::Stress {
            default_delay_ms: 0,
            default_response_bytes: 10,
            default_chunk_bytes: default_stress_chunk_bytes(),
            max_delay_ms: 0,
            max_response_bytes: 32,
            max_chunk_bytes: default_stress_max_chunk_bytes(),
        })
        .process(stress_work(500, 500))
        .await;
        let (_, body) = collect_success_body(result).await;
        assert_eq!(body.len(), 32);
    }

    #[tokio::test]
    async fn stress_defaults_when_request_has_no_stress_block() {
        let result = processor(Behaviour::Stress {
            default_delay_ms: 0,
            default_response_bytes: 17,
            default_chunk_bytes: default_stress_chunk_bytes(),
            max_delay_ms: 0,
            max_response_bytes: 100,
            max_chunk_bytes: default_stress_max_chunk_bytes(),
        })
        .process(dummy_work())
        .await;
        let (_, body) = collect_success_body(result).await;
        assert_eq!(body.len(), 17);
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

    // --- chunk-streaming tests ---

    #[tokio::test]
    async fn stress_emits_multiple_chunks_when_chunk_bytes_smaller_than_response() {
        // 1000 bytes total, 256 bytes per chunk → ceil(1000/256) = 4 chunks
        let result = processor(Behaviour::Stress {
            default_delay_ms: 0,
            default_response_bytes: 1000,
            default_chunk_bytes: default_stress_chunk_bytes(),
            max_delay_ms: 0,
            max_response_bytes: 10_000,
            max_chunk_bytes: default_stress_max_chunk_bytes(),
        })
        .process(stress_work_with_chunk(0, 1000, 256))
        .await;

        let (content_type, chunks) = collect_success_chunks(result).await;
        assert_eq!(content_type, "application/json");
        assert!(
            chunks.len() >= 4,
            "expected ≥ 4 chunks, got {}",
            chunks.len()
        );

        // Reassemble and verify total length and deterministic bytes.
        let body: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(body.len(), 1000);
        assert_eq!(&body[..4], b"xxxx");
    }

    #[tokio::test]
    async fn stress_chunk_bytes_capped_by_max() {
        // default_chunk_bytes=1024, max_chunk_bytes=512
        // request asks for chunk_bytes=10000 → capped to 512
        // response_bytes=2048, chunk_bytes effective=512 → exactly 4 chunks of 512
        let result = processor(Behaviour::Stress {
            default_delay_ms: 0,
            default_response_bytes: 2048,
            default_chunk_bytes: 1024,
            max_delay_ms: 0,
            max_response_bytes: 10_000,
            max_chunk_bytes: 512,
        })
        .process(stress_work_with_chunk(0, 2048, 10_000))
        .await;

        let (_, chunks) = collect_success_chunks(result).await;
        assert_eq!(chunks.len(), 4, "expected 4 chunks, got {}", chunks.len());
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                chunk.len(),
                512,
                "chunk {} has wrong length: {}",
                i,
                chunk.len()
            );
        }

        let body: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(body.len(), 2048);
    }

    #[tokio::test]
    async fn stress_defaults_chunk_bytes_when_request_omits_it() {
        // default_chunk_bytes=256; response_bytes=1000, no chunk_bytes in request
        // → uses default 256, so ≥ 4 chunks
        let result = processor(Behaviour::Stress {
            default_delay_ms: 0,
            default_response_bytes: 1000,
            default_chunk_bytes: 256,
            max_delay_ms: 0,
            max_response_bytes: 10_000,
            max_chunk_bytes: default_stress_max_chunk_bytes(),
        })
        .process(stress_work(0, 1000))
        .await;

        let (_, chunks) = collect_success_chunks(result).await;
        assert!(
            chunks.len() >= 4,
            "expected ≥ 4 chunks with default chunk_bytes=256, got {}",
            chunks.len()
        );

        let body: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(body.len(), 1000);
    }
}
