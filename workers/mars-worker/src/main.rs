// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::k8s::NodePortManager;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use mars_client::{Error as MarsError, MarsClient};
use polytope_worker_common::config::{DEFAULT_CONFIG_PATH, WorkerConfigFile};
use polytope_worker_common::{
    ProcessResult, Processor, SourceError, WorkItem, WorkerConfig, run_worker_loop,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

mod convert;
mod k8s;
mod mars_logs;
mod port_cleanup;

const DEFAULT_STREAM_QUEUE_BYTE_LIMIT: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarsErrorDisposition {
    Recoverable,
    RestartWorker,
}

#[derive(Debug, PartialEq, Eq)]
struct ClassifiedMarsError {
    user_message: String,
    disposition: MarsErrorDisposition,
}

impl ClassifiedMarsError {
    fn recoverable(user_message: String) -> Self {
        Self {
            user_message,
            disposition: MarsErrorDisposition::Recoverable,
        }
    }

    fn unrecoverable(user_message: String) -> Self {
        Self {
            user_message,
            disposition: MarsErrorDisposition::RestartWorker,
        }
    }
}

fn classify_mars_error(raw: &str) -> ClassifiedMarsError {
    let lower = raw.to_lowercase();
    if lower.contains("data not yet available") || lower.contains("scheduled for after") {
        let message = if let Some(release_time) = extract_release_time(raw) {
            format!("Data not released yet. Release time is {release_time}.")
        } else {
            "Data not released yet. Please try again later.".to_string()
        };
        ClassifiedMarsError::recoverable(message)
    } else if lower.contains("croppedrepresentation") {
        ClassifiedMarsError::recoverable(format!(
            "The requested post-processing is not supported for this data. Details: {raw}"
        ))
    } else if lower.contains("restricted_access") || lower.contains("not authorised") {
        ClassifiedMarsError::recoverable(format!(
            "You do not have access to some of the requested data. Details: {raw}"
        ))
    } else if lower.contains("mars_expected_fields")
        || lower.contains("data not found")
        || lower.contains("no data found")
    {
        ClassifiedMarsError::recoverable(format!(
            "Some of the requested data is not available. Details: {raw}"
        ))
    } else if lower.contains("syntax error") || lower.contains("invalid value") {
        ClassifiedMarsError::recoverable(format!("Your request is invalid. Details: {raw}"))
    } else if lower.contains("mars_cache_corruption")
        || lower.contains("uncatched")
        || lower.contains("uncaught")
        || lower.contains("signal 1")
        || lower.contains("assertion failed")
    {
        ClassifiedMarsError::unrecoverable(format!(
            "The data retrieval system hit an internal error. Details: {raw}"
        ))
    } else {
        ClassifiedMarsError::unrecoverable(format!(
            "Your request could not be completed. Details: {raw}"
        ))
    }
}

fn record_mars_source_error(source_error: &SourceError, raw: &str) {
    let classified = classify_mars_error(raw);
    match classified.disposition {
        MarsErrorDisposition::Recoverable => source_error.set_once(classified.user_message),
        MarsErrorDisposition::RestartWorker => {
            source_error.set_unrecoverable_once(classified.user_message)
        }
    }
}

fn invalidated_user_message() -> String {
    "The data stream was interrupted before completing. Please retry.".to_string()
}

fn extract_release_time(raw: &str) -> Option<String> {
    let lower = raw.to_lowercase();
    let idx = lower.find("scheduled for after")?;
    let start = idx + "scheduled for after".len();
    let tail = raw[start..].trim_start_matches([' ', ':']);
    let end = tail
        .find(|c: char| c == ',' || c == '.' || c == '\n' || c == '\r')
        .unwrap_or(tail.len());
    let value = tail[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

struct MarsProcessor {
    /// Port the C++ MARS client binds for DHS callbacks. We forcibly close any
    /// leaked listener on this port between retrieves; see `port_cleanup`.
    local_port: u16,
    env_lock: std::sync::Arc<std::sync::Mutex<()>>,
    mars_logs: mars_logs::MarsLogBridge,
    stream_queue_byte_limit: usize,
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
        let source_error = SourceError::new();
        let source_error_for_task = source_error.clone();
        let local_port = self.local_port;
        let env_lock = self.env_lock.clone();
        let mars_logs = self.mars_logs.clone();
        let request_id = work.job_id.clone();
        let stream_queue_byte_limit = self.stream_queue_byte_limit;
        tokio::task::spawn_blocking(move || {
            let _env_guard = env_lock.lock().expect("MARS environment lock poisoned");
            let _log_scope = mars_logs.begin_request(request_id);
            // SAFETY: this mutex serializes all per-request mutation of process environment
            // variables used by the MARS client.
            unsafe {
                std::env::set_var("MARS_USER_EMAIL", &mars_email);
                std::env::set_var("MARS_USER_TOKEN", &mars_token);
            }

            let mut client = match MarsClient::new(stream_queue_byte_limit) {
                Ok(c) => c,
                Err(e) => {
                    let raw = e.to_string();
                    record_mars_source_error(&source_error_for_task, &raw);
                    let _ = tx.blocking_send(Err(std::io::Error::other(raw)));
                    return;
                }
            };
            let mut stream = match client.retrieve(request_map) {
                Ok(s) => s,
                Err(e) => {
                    let raw = e.to_string();
                    record_mars_source_error(&source_error_for_task, &raw);
                    let _ = tx.blocking_send(Err(std::io::Error::other(raw)));
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
                        let raw = format!("stream invalidated at byte offset {offset}");
                        source_error_for_task.set_unrecoverable_once(invalidated_user_message());
                        let _ = tx.blocking_send(Err(std::io::Error::other(raw)));
                        break;
                    }
                    Err(e) => {
                        warn!("mars stream error: {e}");
                        let raw = e.to_string();
                        record_mars_source_error(&source_error_for_task, &raw);
                        let _ = tx.blocking_send(Err(std::io::Error::other(raw)));
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
        ProcessResult::success_with_source_error(
            "application/x-grib",
            Box::new(stream),
            source_error,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_data_not_released_as_recoverable() {
        let classified = classify_mars_error(
            "mars - ERROR - Data not yet available. Scheduled for after 11:45:00, (11:45:00)",
        );
        assert_eq!(classified.disposition, MarsErrorDisposition::Recoverable);
        assert_eq!(
            classified.user_message,
            "Data not released yet. Release time is 11:45:00."
        );

        let classified = classify_mars_error("Data not yet available");
        assert_eq!(classified.disposition, MarsErrorDisposition::Recoverable);
        assert_eq!(
            classified.user_message,
            "Data not released yet. Please try again later."
        );
    }

    #[test]
    fn classifies_known_request_errors_as_recoverable() {
        for raw in [
            "mars-client error: Representation::croppedRepresentation() not implemented for HEALPixNested[name=H128]",
            "MARS_RESTRICTED_ACCESS_TO_DATA",
            "MARS_EXPECTED_FIELDS Expected 2, got 1",
            "Data not found",
            "syntax error near param",
            "invalid value for date",
        ] {
            assert_eq!(
                classify_mars_error(raw).disposition,
                MarsErrorDisposition::Recoverable,
                "expected recoverable classification for {raw}"
            );
        }
    }

    #[test]
    fn classifies_internal_and_unknown_errors_for_restart() {
        for raw in [
            "MARS_CACHE_CORRUPTION",
            "uncaught exception",
            "signal 11",
            "assertion failed",
            "std::future_error: Future already retrieved",
            "Unexpected message received (Blob(300))",
            "something else",
        ] {
            assert_eq!(
                classify_mars_error(raw).disposition,
                MarsErrorDisposition::RestartWorker,
                "expected restart classification for {raw}"
            );
        }
    }

    #[test]
    fn invalidated_message_matches_mapping() {
        assert_eq!(
            invalidated_user_message(),
            "The data stream was interrupted before completing. Please retry."
        );
    }
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        _ => Err("value must be a positive integer".to_string()),
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
    #[arg(long, default_value_t = 8100)]
    mars_dhs_local_port: u16,
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config_path: String,
    #[arg(long, default_value_t = 1)]
    worker_concurrency: usize,
    #[arg(
        long,
        env = "MARS_STREAM_QUEUE_BYTE_LIMIT",
        default_value_t = DEFAULT_STREAM_QUEUE_BYTE_LIMIT,
        value_parser = parse_positive_usize
    )]
    stream_queue_byte_limit: usize,
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
    let mars_logs = mars_logs::init();

    let cli = Cli::parse();
    let worker_concurrency = resolved_worker_concurrency(cli.worker_concurrency);
    info!(
        worker_concurrency,
        stream_queue_byte_limit = cli.stream_queue_byte_limit,
        poll_timeout_ms = cli.poll_timeout_ms,
        "resolved worker settings"
    );

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
            mars_logs,
            stream_queue_byte_limit: cli.stream_queue_byte_limit,
        },
    )
    .await?;

    if let Err(e) = manager.cleanup().await {
        tracing::warn!(error = %e, "Failed to cleanup NodePort service on shutdown");
    }

    Ok(())
}

#[cfg(test)]
mod processor_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn process_returns_error_for_invalid_request() {
        let processor = MarsProcessor {
            local_port: 8100,
            env_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            mars_logs: mars_logs::test_instance(),
            stream_queue_byte_limit: DEFAULT_STREAM_QUEUE_BYTE_LIMIT,
        };
        let result = processor
            .process(WorkItem {
                job_id: "job-1".into(),
                request: json!({}),
                user: json!({}),
                metadata: json!({}),
                callback_url: None,
            })
            .await;
        assert!(matches!(result, ProcessResult::Error { .. }));
    }

    #[test]
    fn stream_queue_byte_limit_must_be_positive() {
        assert_eq!(parse_positive_usize("33554432"), Ok(33_554_432));
        assert!(parse_positive_usize("0").is_err());
        assert!(parse_positive_usize("not-a-number").is_err());
    }
}
