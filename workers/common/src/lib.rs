use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{SignalKind, signal};

pub mod config;
pub mod delivery;
pub mod delivery_config;
pub mod encoding;
pub mod management;

use crate::delivery::{DeliveryContext, ResultDelivery, make_delivery};
use crate::delivery_config::{Codec, DeliveryConfig};
use crate::encoding::encode_stream;

fn codec_from_accept_encoding(accept_encoding: Option<&str>) -> Codec {
    let enc = accept_encoding.unwrap_or("");
    if enc.contains("zstd") {
        Codec::Zstd
    } else if enc.contains("gzip") {
        Codec::Gzip
    } else {
        Codec::Identity
    }
}

pub type RawStream = Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin>;

pub enum ProcessResult {
    Success {
        content_type: String,
        body: RawStream,
    },
    Reject {
        reason: String,
    },
    Error {
        message: String,
    },
}

impl ProcessResult {
    pub fn success(content_type: impl Into<String>, body: RawStream) -> Self {
        Self::Success {
            content_type: content_type.into(),
            body,
        }
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub job_id: String,
    pub request: serde_json::Value,
    pub user: serde_json::Value,
    pub metadata: serde_json::Value,
}

#[derive(Debug)]
pub enum Completion {
    Complete {
        content_type: String,
        content_encoding: Option<String>,
        content_length: Option<u64>,
        body: reqwest::Body,
    },
    Redirect {
        location: String,
        message: String,
    },
    Reject {
        reason: String,
    },
    Error {
        message: String,
    },
}

impl Completion {
    pub fn complete(
        content_type: impl Into<String>,
        content_encoding: Option<String>,
        content_length: Option<u64>,
        body: reqwest::Body,
    ) -> Self {
        Self::Complete {
            content_type: content_type.into(),
            content_encoding,
            content_length,
            body,
        }
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum CompletionRequest {
    Redirect { location: String, message: String },
    Reject { reason: String },
    Error { message: String },
}

pub const DEFAULT_POLL_TIMEOUT_MS: u64 = 3000;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub broker_url: String,
    pub poll_timeout_ms: u64,
    pub heartbeat_interval: Duration,
    pub retry_backoff: Duration,
    pub management_port: u16,
    /// Number of independent in-flight jobs processed by this worker pod. Must be at least 1.
    pub worker_concurrency: usize,
}

fn enduser_fields(user: &serde_json::Value) -> (Option<&str>, Option<&str>) {
    (
        user.get("auth")
            .and_then(|auth| auth.get("username"))
            .and_then(|value| value.as_str()),
        user.get("auth")
            .and_then(|auth| auth.get("realm"))
            .and_then(|value| value.as_str()),
    )
}

impl WorkerConfig {
    pub fn work_url(&self) -> String {
        format!(
            "{}/work?timeout_ms={}",
            self.broker_url.trim_end_matches('/'),
            self.poll_timeout_ms
        )
    }

    pub fn heartbeat_url(&self, job_id: &str) -> String {
        format!(
            "{}/heartbeat/{job_id}",
            self.broker_url.trim_end_matches('/')
        )
    }

    pub fn complete_data_url(&self, job_id: &str) -> String {
        format!(
            "{}/complete/data/{job_id}",
            self.broker_url.trim_end_matches('/')
        )
    }

    pub fn complete_reject_url(&self, job_id: &str) -> String {
        format!(
            "{}/complete/reject/{job_id}",
            self.broker_url.trim_end_matches('/')
        )
    }

    pub fn complete_error_url(&self, job_id: &str) -> String {
        format!(
            "{}/complete/error/{job_id}",
            self.broker_url.trim_end_matches('/')
        )
    }

    pub fn complete_redirect_url(&self, job_id: &str) -> String {
        format!(
            "{}/complete/redirect/{job_id}",
            self.broker_url.trim_end_matches('/')
        )
    }
}

#[async_trait]
pub trait Processor: Send + Sync {
    async fn process(&self, work: WorkItem) -> ProcessResult;
}

async fn worker_task<P: Processor + 'static>(
    config: WorkerConfig,
    worker_index: usize,
    delivery: std::sync::Arc<dyn ResultDelivery>,
    processor: std::sync::Arc<P>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), reqwest::Error> {
    let mut poll_cycle: u64 = 0;
    let mut idle_anchor = Instant::now();
    loop {
        if *shutdown.borrow() {
            break;
        }

        poll_cycle = poll_cycle.saturating_add(1);
        tracing::debug!(
            "event.name" = "worker.broker.poll_cycle.started",
            worker_index,
            poll_cycle,
            broker_url = %config.broker_url,
            poll_timeout_ms = config.poll_timeout_ms,
            "worker broker poll cycle started"
        );
        let client = reqwest::Client::builder().build()?;

        let poll_started = Instant::now();
        let response = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = client.get(config.work_url()).send() => {
                match result {
                    Ok(response) => response,
                    Err(err) => {
                        tracing::warn!("event.name" = "worker.broker.poll.failed", outcome = "error", broker_url = %config.broker_url, error = %err, "worker poll failed");
                        tokio::time::sleep(config.retry_backoff).await;
                        continue;
                    }
                }
            }
        };

        if response.status() == StatusCode::NO_CONTENT {
            tracing::debug!(
                "event.name" = "worker.broker.poll.empty",
                worker_index,
                poll_cycle,
                broker_url = %config.broker_url,
                wait_ms = poll_started.elapsed().as_millis() as u64,
                "worker poll returned no work"
            );
            continue;
        }
        if !response.status().is_success() {
            tracing::warn!("event.name" = "worker.broker.poll.failed", outcome = "error", broker_url = %config.broker_url, status = %response.status(), "worker poll returned unexpected status");
            tokio::time::sleep(config.retry_backoff).await;
            continue;
        }

        let work: WorkItem = match response.json().await {
            Ok(work) => work,
            Err(err) => {
                tracing::warn!("event.name" = "worker.broker.poll.failed", outcome = "error", error = %err, "worker failed to decode work item");
                tokio::time::sleep(config.retry_backoff).await;
                continue;
            }
        };

        let poll_wait_ms = poll_started.elapsed().as_millis() as u64;
        let idle_ms = idle_anchor.elapsed().as_millis() as u64;
        let (enduser_id, enduser_realm) = enduser_fields(&work.user);
        if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
            tracing::info!("event.name" = "worker.job.started", outcome = "success", job.id = %work.job_id, poll_wait_ms, idle_ms, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "job started");
        } else {
            tracing::info!("event.name" = "worker.job.started", outcome = "success", job.id = %work.job_id, poll_wait_ms, idle_ms, "job started");
        }

        let stop = std::sync::Arc::new(tokio::sync::Notify::new());
        let stop_heartbeat = stop.clone();
        let heartbeat_client = client.clone();
        let heartbeat_cfg = config.clone();
        let job_id = work.job_id.clone();
        let heartbeat_enduser_id = enduser_id.map(str::to_string);
        let heartbeat_enduser_realm = enduser_realm.map(str::to_string);
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_heartbeat.notified() => break,
                    _ = tokio::time::sleep(heartbeat_cfg.heartbeat_interval) => {
                        match heartbeat_client.post(heartbeat_cfg.heartbeat_url(&job_id)).send().await {
                            Ok(resp) if resp.status() == StatusCode::OK => {}
                            Ok(resp) if resp.status() == StatusCode::NOT_FOUND => break,
                            Ok(resp) => {
                                if let (Some(enduser_id), Some(enduser_realm)) = (heartbeat_enduser_id.as_deref(), heartbeat_enduser_realm.as_deref()) {
                                    tracing::warn!("event.name" = "worker.heartbeat.failed", outcome = "error", status=%resp.status(), job.id=%job_id, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "heartbeat returned unexpected status")
                                } else {
                                    tracing::warn!("event.name" = "worker.heartbeat.failed", outcome = "error", status=%resp.status(), job.id=%job_id, "heartbeat returned unexpected status")
                                }
                            },
                            Err(err) => {
                                if let (Some(enduser_id), Some(enduser_realm)) = (heartbeat_enduser_id.as_deref(), heartbeat_enduser_realm.as_deref()) {
                                    tracing::warn!("event.name" = "worker.heartbeat.failed", outcome = "error", error=%err, job.id=%job_id, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "heartbeat request failed")
                                } else {
                                    tracing::warn!("event.name" = "worker.heartbeat.failed", outcome = "error", error=%err, job.id=%job_id, "heartbeat request failed")
                                }
                            },
                        }
                    }
                }
            }
        });

        let process_started = Instant::now();
        let process_result = processor.process(work.clone()).await;
        let process_ms = process_started.elapsed().as_millis() as u64;

        let mut deliver_ms: u64 = 0;
        let completion = match process_result {
            ProcessResult::Success { content_type, body } => {
                let codec = codec_from_accept_encoding(work.metadata["accept_encoding"].as_str());
                let content_encoding = codec.content_encoding_header().map(str::to_string);
                let encoded = encode_stream(body, &codec);
                let deliver_started = Instant::now();
                let completion = delivery
                    .deliver(
                        &content_type,
                        content_encoding.as_deref(),
                        encoded,
                        &work.metadata,
                        DeliveryContext {
                            job_id: &work.job_id,
                            user: &work.user,
                        },
                    )
                    .await;
                deliver_ms = deliver_started.elapsed().as_millis() as u64;
                completion
            }
            ProcessResult::Reject { reason } => {
                if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
                    tracing::warn!("event.name" = "worker.job.rejected", outcome = "rejected", job.id = %work.job_id, reason = %reason, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "job rejected");
                } else {
                    tracing::warn!("event.name" = "worker.job.rejected", outcome = "rejected", job.id = %work.job_id, reason = %reason, "job rejected");
                }
                Completion::Reject { reason }
            }
            ProcessResult::Error { message } => {
                if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
                    tracing::error!("event.name" = "worker.job.failed", outcome = "error", job.id = %work.job_id, error = %message, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "job failed");
                } else {
                    tracing::error!("event.name" = "worker.job.failed", outcome = "error", job.id = %work.job_id, error = %message, "job failed");
                }
                Completion::Error { message }
            }
        };

        stop.notify_one();
        let _ = heartbeat.await;

        let complete_started = Instant::now();
        let (response, outcome) = match completion {
            Completion::Complete {
                content_type,
                content_encoding,
                content_length,
                body,
            } => {
                let mut request = client
                    .post(config.complete_data_url(&work.job_id))
                    .header(reqwest::header::CONTENT_TYPE, content_type)
                    .body(body);
                if let Some(content_length) = content_length {
                    request = request.header(reqwest::header::CONTENT_LENGTH, content_length);
                }
                if let Some(encoding) = content_encoding {
                    request = request.header(reqwest::header::CONTENT_ENCODING, encoding);
                }
                (request.send().await?, "data")
            }
            Completion::Reject { reason } => {
                let payload = CompletionRequest::Reject { reason };
                let resp = client
                    .post(config.complete_reject_url(&work.job_id))
                    .json(&payload)
                    .send()
                    .await?;
                (resp, "reject")
            }
            Completion::Error { message } => {
                let payload = CompletionRequest::Error { message };
                let resp = client
                    .post(config.complete_error_url(&work.job_id))
                    .json(&payload)
                    .send()
                    .await?;
                (resp, "error")
            }
            Completion::Redirect { location, message } => {
                let payload = CompletionRequest::Redirect { location, message };
                let resp = client
                    .post(config.complete_redirect_url(&work.job_id))
                    .json(&payload)
                    .send()
                    .await?;
                (resp, "redirect")
            }
        };
        let complete_ms = complete_started.elapsed().as_millis() as u64;
        if matches!(outcome, "data" | "redirect") {
            if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
                tracing::info!("event.name" = "worker.job.completed", outcome = "success", job.id = %work.job_id, poll_wait_ms, idle_ms, process_ms, deliver_ms, complete_ms, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "job completed");
            } else {
                tracing::info!("event.name" = "worker.job.completed", outcome = "success", job.id = %work.job_id, poll_wait_ms, idle_ms, process_ms, deliver_ms, complete_ms, "job completed");
            }
        }
        if response.status() != StatusCode::OK && response.status() != StatusCode::NOT_FOUND {
            if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
                tracing::error!("event.name" = "worker.job.failed", outcome = "error", status=%response.status(), job.id=%work.job_id, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, "worker completion returned unexpected status");
            } else {
                tracing::error!("event.name" = "worker.job.failed", outcome = "error", status=%response.status(), job.id=%work.job_id, "worker completion returned unexpected status");
            }
        }
        idle_anchor = Instant::now();
    }

    Ok(())
}

pub async fn run_worker_loop<P: Processor + 'static>(
    config: WorkerConfig,
    delivery_config: DeliveryConfig,
    processor: P,
) -> Result<(), reqwest::Error> {
    assert!(
        config.worker_concurrency >= 1,
        "worker_concurrency must be at least 1"
    );

    let management_app = management::router();
    let management_listener = tokio::net::TcpListener::bind(("0.0.0.0", config.management_port))
        .await
        .unwrap_or_else(|err| {
            panic!(
                "failed to bind management server on port {}: {err}",
                config.management_port
            )
        });
    let management_port = management_listener.local_addr().unwrap().port();
    tracing::info!(
        "event.name" = "startup.server.listening",
        outcome = "success",
        port = management_port,
        "management server listening"
    );
    tokio::spawn(async move {
        axum::serve(management_listener, management_app)
            .await
            .unwrap();
    });

    let delivery: std::sync::Arc<dyn ResultDelivery> =
        std::sync::Arc::from(make_delivery(&delivery_config).await);
    let processor = std::sync::Arc::new(processor);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut tasks = Vec::with_capacity(config.worker_concurrency);
    for worker_index in 0..config.worker_concurrency {
        let task_config = config.clone();
        let task_delivery = delivery.clone();
        let task_processor = processor.clone();
        let task_shutdown = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move {
            tracing::debug!(worker_index, "worker task started");
            let result = worker_task(
                task_config,
                worker_index,
                task_delivery,
                task_processor,
                task_shutdown,
            )
            .await;
            tracing::debug!(worker_index, "worker task stopped");
            result
        }));
    }

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("event.name" = "startup.shutdown.received", outcome = "success", "received shutdown signal");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("event.name" = "startup.shutdown.received", outcome = "success", "received shutdown signal");
        }
    }

    let _ = shutdown_tx.send(true);
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(err) => panic!("worker task failed: {err}"),
        }
    }
    tracing::info!(
        "event.name" = "startup.shutdown.complete",
        outcome = "success",
        "graceful shutdown complete"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Bytes,
        extract::{ConnectInfo, Path, State},
        http::StatusCode,
        routing::{get, post, put},
    };
    use futures::TryStreamExt;
    use std::{
        net::SocketAddr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[test]
    fn enduser_fields_extracts_username_and_realm_when_present() {
        let user = serde_json::json!({"auth": {"username": "alice", "realm": "ecmwf"}});
        assert_eq!(enduser_fields(&user), (Some("alice"), Some("ecmwf")));
    }

    #[test]
    fn enduser_fields_omits_missing_user() {
        assert_eq!(enduser_fields(&serde_json::json!({})), (None, None));
    }

    #[derive(Default)]
    struct MockState {
        delivered: Mutex<bool>,
        completions: Mutex<Vec<Vec<u8>>>,
    }

    async fn work(State(state): State<Arc<MockState>>) -> Result<axum::Json<WorkItem>, StatusCode> {
        let mut delivered = state.delivered.lock().unwrap();
        if *delivered {
            Err(StatusCode::NO_CONTENT)
        } else {
            *delivered = true;
            Ok(axum::Json(WorkItem {
                job_id: "job-1".into(),
                request: serde_json::json!({"foo": "bar"}),
                user: serde_json::json!({}),
                metadata: serde_json::json!({}),
            }))
        }
    }

    async fn heartbeat(Path(_job_id): Path<String>) -> StatusCode {
        StatusCode::OK
    }

    async fn complete(
        Path(_job_id): Path<String>,
        State(state): State<Arc<MockState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        let payload = body
            .into_data_stream()
            .try_fold(Vec::new(), |mut acc, chunk: Bytes| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .unwrap();
        state.completions.lock().unwrap().push(payload);
        StatusCode::OK
    }

    struct BrokerState {
        delivered: Mutex<bool>,
        completions: Mutex<Vec<(String, Vec<u8>)>>,
        work_metadata: Mutex<serde_json::Value>,
    }

    impl Default for BrokerState {
        fn default() -> Self {
            Self {
                delivered: Mutex::new(false),
                completions: Mutex::new(Vec::new()),
                work_metadata: Mutex::new(serde_json::json!({})),
            }
        }
    }

    struct BobsState {
        write_base_url: String,
        calls: Mutex<Vec<String>>,
        writes: Mutex<Vec<Vec<u8>>>,
    }

    async fn broker_work(
        State(state): State<Arc<BrokerState>>,
    ) -> Result<axum::Json<WorkItem>, StatusCode> {
        let mut delivered = state.delivered.lock().unwrap();
        if *delivered {
            Err(StatusCode::NO_CONTENT)
        } else {
            *delivered = true;
            Ok(axum::Json(WorkItem {
                job_id: "job-1".into(),
                request: serde_json::json!({"foo": "bar"}),
                user: serde_json::json!({}),
                metadata: state.work_metadata.lock().unwrap().clone(),
            }))
        }
    }

    async fn broker_heartbeat(Path(_job_id): Path<String>) -> StatusCode {
        StatusCode::OK
    }

    async fn broker_complete_data(
        Path(_job_id): Path<String>,
        State(state): State<Arc<BrokerState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        broker_complete("data", state, body).await
    }

    async fn broker_complete_redirect(
        Path(_job_id): Path<String>,
        State(state): State<Arc<BrokerState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        broker_complete("redirect", state, body).await
    }

    async fn broker_complete_reject(
        Path(_job_id): Path<String>,
        State(state): State<Arc<BrokerState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        broker_complete("reject", state, body).await
    }

    async fn broker_complete_error(
        Path(_job_id): Path<String>,
        State(state): State<Arc<BrokerState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        broker_complete("error", state, body).await
    }

    async fn broker_complete(
        kind: &str,
        state: Arc<BrokerState>,
        body: axum::body::Body,
    ) -> StatusCode {
        let payload = body
            .into_data_stream()
            .try_fold(Vec::new(), |mut acc, chunk: Bytes| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .unwrap();
        state
            .completions
            .lock()
            .unwrap()
            .push((kind.to_string(), payload));
        StatusCode::OK
    }

    async fn bobs_create(
        State(state): State<Arc<BobsState>>,
    ) -> (StatusCode, axum::Json<serde_json::Value>) {
        state.calls.lock().unwrap().push("create".to_string());
        (
            StatusCode::CREATED,
            axum::Json(serde_json::json!({
                "key": "redirect-key",
                "read_url": "https://polytope.example.com/download-0/redirect-key",
                "write_url": state.write_base_url.clone(),
            })),
        )
    }

    async fn bobs_write(
        Path((_key, _offset)): Path<(String, u64)>,
        State(state): State<Arc<BobsState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        let payload = body
            .into_data_stream()
            .try_fold(Vec::new(), |mut acc, chunk: Bytes| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .unwrap();
        state.calls.lock().unwrap().push("write".to_string());
        state.writes.lock().unwrap().push(payload);
        StatusCode::OK
    }

    async fn bobs_complete(
        Path(_key): Path<String>,
        State(state): State<Arc<BobsState>>,
    ) -> StatusCode {
        state.calls.lock().unwrap().push("complete".to_string());
        StatusCode::OK
    }

    #[derive(Default)]
    struct ConnectionState {
        work_calls: AtomicUsize,
        work_peers: Mutex<Vec<SocketAddr>>,
        complete_peers: Mutex<Vec<SocketAddr>>,
    }

    async fn connection_work(
        ConnectInfo(peer): ConnectInfo<SocketAddr>,
        State(state): State<Arc<ConnectionState>>,
    ) -> Result<axum::Json<WorkItem>, StatusCode> {
        state.work_peers.lock().unwrap().push(peer);
        let call = state.work_calls.fetch_add(1, Ordering::SeqCst);
        if call == 1 {
            Ok(axum::Json(WorkItem {
                job_id: "job-connection".into(),
                request: serde_json::json!({"foo": "bar"}),
                user: serde_json::json!({}),
                metadata: serde_json::json!({}),
            }))
        } else {
            Err(StatusCode::NO_CONTENT)
        }
    }

    async fn connection_complete(
        ConnectInfo(peer): ConnectInfo<SocketAddr>,
        State(state): State<Arc<ConnectionState>>,
    ) -> StatusCode {
        state.complete_peers.lock().unwrap().push(peer);
        StatusCode::OK
    }

    struct StubProcessor;

    #[async_trait]
    impl Processor for StubProcessor {
        async fn process(&self, _work: WorkItem) -> ProcessResult {
            let stream =
                futures::stream::once(futures::future::ready(Ok::<bytes::Bytes, std::io::Error>(
                    bytes::Bytes::from(vec![1, 2, 3]),
                )));
            ProcessResult::success("application/octet-stream", Box::new(stream))
        }
    }

    struct DirectStreamProcessor;

    #[async_trait]
    impl Processor for DirectStreamProcessor {
        async fn process(&self, _work: WorkItem) -> ProcessResult {
            let stream = futures::stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(
                bytes::Bytes::from(vec![1u8, 2, 3]),
            )]);
            ProcessResult::success("application/octet-stream", Box::new(stream))
        }
    }

    struct RejectProcessor;

    #[async_trait]
    impl Processor for RejectProcessor {
        async fn process(&self, _work: WorkItem) -> ProcessResult {
            ProcessResult::reject("bad request")
        }
    }

    struct ErrorProcessor;

    #[async_trait]
    impl Processor for ErrorProcessor {
        async fn process(&self, _work: WorkItem) -> ProcessResult {
            ProcessResult::error("internal error")
        }
    }

    #[tokio::test]
    async fn empty_poll_rotates_connection_and_job_callbacks_stay_sticky() {
        let state = Arc::new(ConnectionState::default());
        let app = Router::new()
            .route("/work", get(connection_work))
            .route("/heartbeat/{job_id}", post(broker_heartbeat))
            .route("/complete/data/{job_id}", post(connection_complete))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let config = WorkerConfig {
            broker_url: format!("http://{addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_secs(60),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 1,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,
                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            StubProcessor,
        ));

        for _ in 0..50 {
            if !state.complete_peers.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        let work_peers = state.work_peers.lock().unwrap().clone();
        let complete_peers = state.complete_peers.lock().unwrap().clone();
        assert!(
            work_peers.len() >= 2,
            "expected an empty poll and then a job poll"
        );
        assert_eq!(complete_peers.len(), 1, "expected one completion callback");
        assert_ne!(
            work_peers[0], work_peers[1],
            "empty poll and next poll should use different TCP connections"
        );
        assert_eq!(
            work_peers[1], complete_peers[0],
            "job completion must reuse the job poll connection for in-job stickiness"
        );
    }

    #[tokio::test]
    async fn worker_loop_posts_streaming_completion() {
        let state = Arc::new(MockState::default());
        let app = Router::new()
            .route("/work", get(work))
            .route("/heartbeat/{job_id}", post(heartbeat))
            .route("/complete/data/{job_id}", post(complete))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let config = WorkerConfig {
            broker_url: format!("http://{addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 1,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,

                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            StubProcessor,
        ));
        for _ in 0..20 {
            if !state.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        let completions = state.completions.lock().unwrap();
        assert_eq!(completions[0], vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn worker_with_bobs_delivery_posts_redirect_completion() {
        let bobs_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bobs_addr = bobs_listener.local_addr().unwrap();
        let bobs_url = format!("http://{bobs_addr}");
        // make_delivery strips the scheme and appends "/api/v1", so the worker's
        // api_base (for /create) and write_base (for /write and /complete) are
        // both rooted at "{bobs_url}/api/v1".  Mirror that in the mock.
        let bobs_api_base = format!("{bobs_url}/api/v1");

        let bobs_state = Arc::new(BobsState {
            write_base_url: bobs_api_base.clone(),
            calls: Mutex::new(vec![]),
            writes: Mutex::new(vec![]),
        });
        let bobs_app = Router::new()
            .route("/api/v1/create", put(bobs_create))
            .route("/api/v1/write/{key}/{offset}", post(bobs_write))
            .route("/api/v1/complete/{key}", post(bobs_complete))
            .with_state(bobs_state.clone());
        tokio::spawn(async move { axum::serve(bobs_listener, bobs_app).await.unwrap() });

        let broker_state = Arc::new(BrokerState::default());
        let broker_app = Router::new()
            .route("/work", get(broker_work))
            .route("/heartbeat/{job_id}", post(broker_heartbeat))
            .route("/complete/data/{job_id}", post(broker_complete_data))
            .route(
                "/complete/redirect/{job_id}",
                post(broker_complete_redirect),
            )
            .route("/complete/reject/{job_id}", post(broker_complete_reject))
            .route("/complete/error/{job_id}", post(broker_complete_error))
            .with_state(broker_state.clone());
        let broker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_addr = broker_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(broker_listener, broker_app).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let config = WorkerConfig {
            broker_url: format!("http://{broker_addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 1,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Bobs,
                bobs_url: Some(bobs_url.clone()),

                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            DirectStreamProcessor,
        ));
        for _ in 0..40 {
            if !broker_state.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        let completions = broker_state.completions.lock().unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, "redirect");
        let body: serde_json::Value = serde_json::from_slice(&completions[0].1).unwrap();
        assert_eq!(
            body["location"].as_str().unwrap(),
            "https://polytope.example.com/download-0/redirect-key"
        );
        assert_eq!(
            body["message"].as_str().unwrap(),
            "result available for download"
        );

        drop(completions);
        let calls = bobs_state.calls.lock().unwrap();
        assert_eq!(&*calls, &["create", "write", "complete"]);
        drop(calls);
        let writes = bobs_state.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0], vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn worker_with_direct_delivery_posts_data_completion() {
        let broker_state = Arc::new(BrokerState::default());
        let broker_app = Router::new()
            .route("/work", get(broker_work))
            .route("/heartbeat/{job_id}", post(broker_heartbeat))
            .route("/complete/data/{job_id}", post(broker_complete_data))
            .route(
                "/complete/redirect/{job_id}",
                post(broker_complete_redirect),
            )
            .route("/complete/reject/{job_id}", post(broker_complete_reject))
            .route("/complete/error/{job_id}", post(broker_complete_error))
            .with_state(broker_state.clone());
        let broker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_addr = broker_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(broker_listener, broker_app).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let config = WorkerConfig {
            broker_url: format!("http://{broker_addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 1,
        };

        *broker_state.work_metadata.lock().unwrap() =
            serde_json::json!({"accept_encoding": "zstd"});

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,

                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            StubProcessor,
        ));
        for _ in 0..40 {
            if !broker_state.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        let completions = broker_state.completions.lock().unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, "data");
        assert!(!completions[0].1.is_empty());
    }

    #[tokio::test]
    async fn worker_reject_skips_delivery() {
        let broker_state = Arc::new(BrokerState::default());
        let broker_app = Router::new()
            .route("/work", get(broker_work))
            .route("/heartbeat/{job_id}", post(broker_heartbeat))
            .route("/complete/data/{job_id}", post(broker_complete_data))
            .route(
                "/complete/redirect/{job_id}",
                post(broker_complete_redirect),
            )
            .route("/complete/reject/{job_id}", post(broker_complete_reject))
            .route("/complete/error/{job_id}", post(broker_complete_error))
            .with_state(broker_state.clone());
        let broker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_addr = broker_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(broker_listener, broker_app).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let config = WorkerConfig {
            broker_url: format!("http://{broker_addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 1,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,

                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            RejectProcessor,
        ));
        for _ in 0..40 {
            if !broker_state.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        let completions = broker_state.completions.lock().unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, "reject");
        assert!(!completions.iter().any(|(kind, _)| kind == "data"));
    }

    #[tokio::test]
    async fn worker_error_skips_delivery() {
        let broker_state = Arc::new(BrokerState::default());
        let broker_app = Router::new()
            .route("/work", get(broker_work))
            .route("/heartbeat/{job_id}", post(broker_heartbeat))
            .route("/complete/data/{job_id}", post(broker_complete_data))
            .route(
                "/complete/redirect/{job_id}",
                post(broker_complete_redirect),
            )
            .route("/complete/reject/{job_id}", post(broker_complete_reject))
            .route("/complete/error/{job_id}", post(broker_complete_error))
            .with_state(broker_state.clone());
        let broker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_addr = broker_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(broker_listener, broker_app).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let config = WorkerConfig {
            broker_url: format!("http://{broker_addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 1,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,

                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            ErrorProcessor,
        ));
        for _ in 0..40 {
            if !broker_state.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        let completions = broker_state.completions.lock().unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, "error");
        assert!(!completions.iter().any(|(kind, _)| kind == "data"));
    }

    struct ConcurrentBrokerState {
        polls: AtomicUsize,
        completions: Mutex<Vec<String>>,
    }

    async fn concurrent_work(
        State(state): State<Arc<ConcurrentBrokerState>>,
    ) -> Result<axum::Json<WorkItem>, StatusCode> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let poll = state.polls.fetch_add(1, Ordering::SeqCst);
        if poll < 2 {
            Ok(axum::Json(WorkItem {
                job_id: format!("job-{}", poll + 1),
                request: serde_json::json!({"index": poll}),
                user: serde_json::json!({}),
                metadata: serde_json::json!({}),
            }))
        } else {
            Err(StatusCode::NO_CONTENT)
        }
    }

    async fn concurrent_complete(
        Path(job_id): Path<String>,
        State(state): State<Arc<ConcurrentBrokerState>>,
    ) -> StatusCode {
        state.completions.lock().unwrap().push(job_id);
        StatusCode::OK
    }

    struct ConcurrentProcessor {
        concurrent_in_process: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        reached_two: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Processor for ConcurrentProcessor {
        async fn process(&self, _work: WorkItem) -> ProcessResult {
            let current = self.concurrent_in_process.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            if current >= 2 {
                self.reached_two.notify_waiters();
            }

            self.release.notified().await;
            self.concurrent_in_process.fetch_sub(1, Ordering::SeqCst);

            let stream =
                futures::stream::once(futures::future::ready(Ok::<bytes::Bytes, std::io::Error>(
                    bytes::Bytes::from_static(b"ok"),
                )));
            ProcessResult::success("application/octet-stream", Box::new(stream))
        }
    }

    #[tokio::test]
    async fn worker_loop_processes_jobs_concurrently_when_worker_concurrency_is_two() {
        let state = Arc::new(ConcurrentBrokerState {
            polls: AtomicUsize::new(0),
            completions: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route("/work", get(concurrent_work))
            .route("/heartbeat/{job_id}", post(heartbeat))
            .route("/complete/data/{job_id}", post(concurrent_complete))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let concurrent_in_process = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let reached_two = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());

        let config = WorkerConfig {
            broker_url: format!("http://{addr}"),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 2,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,
                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            ConcurrentProcessor {
                concurrent_in_process,
                peak: peak.clone(),
                reached_two: reached_two.clone(),
                release: release.clone(),
            },
        ));

        tokio::time::timeout(Duration::from_secs(2), reached_two.notified())
            .await
            .unwrap();
        release.notify_waiters();

        for _ in 0..40 {
            if state.completions.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        run.abort();

        assert!(peak.load(Ordering::SeqCst) >= 2);
        let mut completions = state.completions.lock().unwrap().clone();
        completions.sort();
        assert_eq!(completions, vec!["job-1".to_string(), "job-2".to_string()]);
    }

    #[tokio::test]
    async fn worker_loop_validates_nonzero_worker_concurrency() {
        let config = WorkerConfig {
            broker_url: "http://127.0.0.1:1".to_string(),
            poll_timeout_ms: 10,
            heartbeat_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            management_port: 0,
            worker_concurrency: 0,
        };

        let run = tokio::spawn(run_worker_loop(
            config,
            delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,
                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_presigned_url_expiry_secs: None,
                s3_public_url: None,
                s3_key_prefix: String::new(),
            },
            StubProcessor,
        ));

        let err = run.await.unwrap_err();
        assert!(err.is_panic());
        let message = if let Some(message) = err.try_into_panic().unwrap().downcast_ref::<&str>() {
            (*message).to_string()
        } else {
            "".to_string()
        };
        assert!(message.contains("worker_concurrency"));
    }
}
