// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::callback_relay::{CallbackRelay, RelayController};
use crate::k8s::NodePortManager;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use mars_client::{Error as MarsError, MarsClient};
use polytope_worker_common::config::{DEFAULT_CONFIG_PATH, WorkerConfigFile};
use polytope_worker_common::{
    ProcessResult, Processor, SourceError, WorkItem, WorkerConfig, run_worker_loop,
};
use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

mod callback_relay;
mod convert;
mod k8s;
mod mars_logs;

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
    // Note: Most of these are based on potentially out of date confluence pages.
    // Empirically observed errors are marked with a comment
    let lower = raw.to_lowercase();
    // Empirically observed; retrying within the worker may also be worthwhile.
    if lower.contains("connection reset by peer") || lower.contains("socket read failed") {
        ClassifiedMarsError::recoverable(
            "The data retrieval connection was interrupted. Please try again.".to_string(),
        )
    } else if lower.contains("data not yet available") || lower.contains("scheduled for after") {
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
        // Empirically observed equivalent of MARS_EXPECTED_FIELDS:
        // "0 message retrieved out of N expected" (no fields found) and
        // "N messages retrieved out of M expected" (partial retrieval).
        // Both are data-availability errors — no need to restart the worker.
        || lower.contains("retrieved out of")
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
    let end = tail.find([',', '.', '\n', '\r']).unwrap_or(tail.len());
    let value = tail[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn mars_credentials(metadata: &Value, user: &Value) -> Result<(String, String), String> {
    if let Some(credentials) = metadata
        .pointer("/mars_credentials")
        .and_then(Value::as_object)
    {
        return credential_pair(credentials, "email", "token");
    }

    let attributes = ["/auth/attributes", "/attributes"]
        .into_iter()
        .find_map(|pointer| user.pointer(pointer).and_then(Value::as_object))
        .ok_or_else(|| "job metadata is missing MARS credentials".to_string())?;
    credential_pair(attributes, "ecmwf-email", "ecmwf-apikey")
}

fn credential_pair(
    values: &serde_json::Map<String, Value>,
    email_key: &str,
    token_key: &str,
) -> Result<(String, String), String> {
    let email = values
        .get(email_key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "MARS credentials are missing the email".to_string())?;
    let token = values
        .get(token_key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "MARS credentials are missing the token".to_string())?;

    Ok((email.to_owned(), token.to_owned()))
}

/// Returns freed glibc arena pages to the OS when a MARS retrieval finishes.
///
/// The FDB5 remote client allocates a large number of medium-sized buffers
/// (roughly 1-4 MiB each) while streaming GRIB data from the remote FDB store.
/// They are freed by the C++ layer as each field is consumed, but glibc's
/// dynamic mmap threshold ratchets upward (up to `DEFAULT_MMAP_THRESHOLD_MAX`,
/// 32 MiB on 64-bit) as those buffers are released, so subsequent allocations
/// of that size come from the heap rather than mmap. Heap pages stay in
/// glibc's arenas after `free()`, and small live FDB objects scattered above
/// them block top-of-heap trimming, so the pod's RSS baseline rises to roughly
/// the peak in-flight size of the largest job it has processed. A second large
/// job then runs on top of that baseline and OOMKills the pod (exit code 137).
///
/// `malloc_trim(0)` walks all glibc arenas and releases free pages back to the
/// OS via `madvise(MADV_DONTNEED)`. Measured at 40-80 ms when it is the only
/// mitigation, and under 0.4 ms alongside `MALLOC_MMAP_THRESHOLD_=131072`
/// (which is set in the worker pod env and is the primary fix).
///
/// This is a drop guard rather than a straight-line call so that it also runs
/// on the early-return paths - in particular client disconnect part way
/// through a large retrieval, which is exactly when a lot has been allocated.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
struct MallocTrimOnDrop;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
impl Drop for MallocTrimOnDrop {
    fn drop(&mut self) {
        // SAFETY: malloc_trim only inspects glibc's own arenas and releases
        // pages that are already on its free lists. It never invalidates live
        // allocations, and it is safe to call from any thread.
        unsafe {
            libc::malloc_trim(0);
        }
    }
}

/// No-op on musl and non-Linux targets, which do not provide `malloc_trim`.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
struct MallocTrimOnDrop;

struct MarsProcessor {
    /// Serializes relay generations, MARS environment mutation, and the full
    /// blocking C++ retrieval while broker workers queue safely behind it.
    env_lock: std::sync::Arc<std::sync::Mutex<()>>,
    relay: RelayController,
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

        let (mars_email, mars_token) = match mars_credentials(&work.metadata, &work.user) {
            Ok(credentials) => credentials,
            Err(message) => return ProcessResult::error(message),
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
        let source_error = SourceError::new();
        let source_error_for_task = source_error.clone();
        let relay = self.relay.clone();
        let env_lock = self.env_lock.clone();
        let mars_logs = self.mars_logs.clone();
        let request_id = work.job_id.clone();
        let stream_queue_byte_limit = self.stream_queue_byte_limit;
        tokio::task::spawn_blocking(move || {
            // Broker concurrency may be greater than one. This process-wide
            // lock permits exactly one active relay generation and MARS call.
            let _env_guard = env_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if tx.is_closed() {
                tracing::debug!("discarding abandoned MARS job before retrieval starts");
                return;
            }
            // Declared before the relay generation, MARS client and stream so
            // that it drops *after* all of them (locals drop in reverse
            // declaration order). By that point the C++ layer has freed every
            // GRIB buffer, so the arenas hold only free pages and the trim can
            // return them immediately. It still runs while `_env_guard` is
            // held, so the trim is serialized with respect to other MARS jobs.
            let _malloc_trim = MallocTrimOnDrop;
            let _log_scope = mars_logs.begin_request(request_id);
            let generation = match relay.start_generation() {
                Ok(generation) => generation,
                Err(e) => {
                    let raw = format!("failed to start MARS callback relay generation: {e}");
                    source_error_for_task.set_unrecoverable_once(raw.clone());
                    let _ = tx.blocking_send(Err(std::io::Error::other(raw)));
                    return;
                }
            };
            // SAFETY: this mutex serializes every MARS call and mutation of the
            // process environment variable consumed by the callback listener.
            unsafe {
                std::env::set_var("MARS_DHS_LOCALPORT", generation.target_port().to_string());
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
            let mut stream = match client.retrieve(request_map, &mars_email, &mars_token) {
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
            // `_malloc_trim` (declared above) returns the freed arena pages to
            // the OS once this scope unwinds.
            //
            // End the relay generation before releasing env_lock. Early-return
            // paths preserve the same order through reverse declaration drops.
            drop(generation);
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
            // Empirically observed in production: TCP teardown mid-transfer.
            "[ERROR] Socket read failed (TCPClient[port=0]) (Connection reset by peer)",
            "Connection reset by peer",
            // Empirically observed in production: same family as mars_expected_fields.
            "[ERROR] Exception: UserError: 0 message retrieved out of 48 expected",
            "UserError: 0 message retrieved out of 1 expected",
            // Empirically observed: partial retrieval — some fields returned but
            // fewer than expected. Not an internal error; no restart needed.
            "UserError: 144 messages retrieved out of 192 expected",
            "[ERROR] Exception: UserError: 97 messages retrieved out of 388 expected",
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

    #[test]
    fn extracts_mars_credentials_from_job_metadata() {
        let metadata = serde_json::json!({
            "mars_credentials": {
                "email": "user@example.test",
                "token": "secret-token"
            }
        });

        assert_eq!(
            mars_credentials(&metadata, &serde_json::json!({})),
            Ok(("user@example.test".to_string(), "secret-token".to_string()))
        );
    }

    #[test]
    fn accepts_legacy_credentials_from_user_context() {
        let user = serde_json::json!({
            "auth": {
                "attributes": {
                    "ecmwf-email": "user@example.test",
                    "ecmwf-apikey": "secret-token"
                }
            }
        });

        assert_eq!(
            mars_credentials(&serde_json::json!({}), &user),
            Ok(("user@example.test".to_string(), "secret-token".to_string()))
        );
    }

    #[test]
    fn rejects_missing_or_empty_mars_credentials() {
        assert!(mars_credentials(&serde_json::json!({}), &serde_json::json!({})).is_err());
        assert!(
            mars_credentials(
                &serde_json::json!({
                    "mars_credentials": {
                        "email": "user@example.test",
                        "token": ""
                    }
                }),
                &serde_json::json!({})
            )
            .is_err()
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
    #[arg(long, env = "MARS_CALLBACK_RELAY_PORT", default_value_t = 18100)]
    mars_callback_relay_port: u16,
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

    // The relay must accept callbacks before its NodePort Service is created.
    let relay = CallbackRelay::bind(cli.mars_callback_relay_port).await?;
    let manager = match NodePortManager::new(relay.listen_port()).await {
        Ok(manager) => manager,
        Err(error) => {
            relay.shutdown().await;
            return Err(error);
        }
    };
    // SAFETY: set once at startup before run_worker_loop spawns processing
    // threads; per-request variables are set separately under env_lock.
    unsafe {
        std::env::set_var("MARS_DHS_CALLBACK_HOST", manager.node_name());
        std::env::set_var("MARS_DHS_CALLBACK_PORT", manager.node_port().to_string());
    }
    tracing::debug!(
        node_port = manager.node_port(),
        relay_port = manager.relay_port(),
        "NodePort service created, MARS DHS callback relay configured"
    );

    let worker_result = run_worker_loop(
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
            env_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            relay: relay.controller(),
            mars_logs,
            stream_queue_byte_limit: cli.stream_queue_byte_limit,
        },
    )
    .await;

    relay.shutdown().await;

    if let Err(e) = manager.cleanup().await {
        tracing::warn!(error = %e, "Failed to cleanup NodePort service on shutdown");
    }

    worker_result?;

    Ok(())
}

#[cfg(test)]
mod processor_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn process_returns_error_for_invalid_request() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let processor = MarsProcessor {
            env_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            relay: relay.controller(),
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
        relay.shutdown().await;
    }

    #[test]
    fn stream_queue_byte_limit_must_be_positive() {
        assert_eq!(parse_positive_usize("33554432"), Ok(33_554_432));
        assert!(parse_positive_usize("0").is_err());
        assert!(parse_positive_usize("not-a-number").is_err());
    }
}
