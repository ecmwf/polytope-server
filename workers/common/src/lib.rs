use std::time::Duration;

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

use crate::delivery::{ResultDelivery, make_delivery};
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

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub broker_url: String,
    pub poll_timeout_ms: u64,
    pub heartbeat_interval: Duration,
    pub retry_backoff: Duration,
    pub management_port: u16,
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

pub async fn run_worker_loop<P: Processor>(
    config: WorkerConfig,
    delivery_config: DeliveryConfig,
    processor: P,
) -> Result<(), reqwest::Error> {
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
    tracing::info!(port = management_port, "management server listening");
    tokio::spawn(async move {
        axum::serve(management_listener, management_app)
            .await
            .unwrap();
    });

    let client = reqwest::Client::builder().build()?;
    let delivery: Box<dyn ResultDelivery> = make_delivery(&delivery_config, client.clone()).await;
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    let mut shutting_down = false;

    loop {
        if shutting_down {
            tracing::info!("graceful shutdown complete");
            break;
        }

        let response = tokio::select! {
            biased;
            _ = sigterm.recv() => {
                tracing::info!("received shutdown signal, completing in-flight work then exiting");
                shutting_down = true;
                continue;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received shutdown signal, completing in-flight work then exiting");
                shutting_down = true;
                continue;
            }
            result = client.get(config.work_url()).send() => {
                match result {
                    Ok(response) => response,
                    Err(err) => {
                        tracing::warn!(broker_url = %config.broker_url, error = %err, "worker poll failed");
                        tokio::time::sleep(config.retry_backoff).await;
                        continue;
                    }
                }
            }
        };

        if response.status() == StatusCode::NO_CONTENT {
            continue;
        }
        if !response.status().is_success() {
            tracing::warn!(broker_url = %config.broker_url, status = %response.status(), "worker poll returned unexpected status");
            tokio::time::sleep(config.retry_backoff).await;
            continue;
        }

        let work: WorkItem = match response.json().await {
            Ok(work) => work,
            Err(err) => {
                tracing::warn!(error = %err, "worker failed to decode work item");
                tokio::time::sleep(config.retry_backoff).await;
                continue;
            }
        };

        tracing::info!(job_id = %work.job_id, "job started");

        let stop = std::sync::Arc::new(tokio::sync::Notify::new());
        let stop_heartbeat = stop.clone();
        let heartbeat_client = client.clone();
        let heartbeat_cfg = config.clone();
        let job_id = work.job_id.clone();
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_heartbeat.notified() => break,
                    _ = tokio::time::sleep(heartbeat_cfg.heartbeat_interval) => {
                        match heartbeat_client.post(heartbeat_cfg.heartbeat_url(&job_id)).send().await {
                            Ok(resp) if resp.status() == StatusCode::OK => {}
                            Ok(resp) if resp.status() == StatusCode::NOT_FOUND => break,
                            Ok(resp) => tracing::warn!(status=%resp.status(), job_id=%job_id, "heartbeat returned unexpected status"),
                            Err(err) => tracing::warn!(error=%err, job_id=%job_id, "heartbeat request failed"),
                        }
                    }
                }
            }
        });

        let process_result = processor.process(work.clone()).await;

        let completion = match process_result {
            ProcessResult::Success { content_type, body } => {
                let codec = codec_from_accept_encoding(work.metadata["accept_encoding"].as_str());
                let content_encoding = codec.content_encoding_header().map(str::to_string);
                let encoded = encode_stream(body, &codec);
                delivery
                    .deliver(
                        &content_type,
                        content_encoding.as_deref(),
                        encoded,
                        &work.metadata,
                    )
                    .await
            }
            ProcessResult::Reject { reason } => Completion::Reject { reason },
            ProcessResult::Error { message } => Completion::Error { message },
        };

        stop.notify_one();
        let _ = heartbeat.await;

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
        tracing::info!(job_id = %work.job_id, outcome = outcome, "job completed");
        if response.status() != StatusCode::OK && response.status() != StatusCode::NOT_FOUND {
            tracing::warn!(status=%response.status(), job_id=%work.job_id, "worker completion returned unexpected status");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Bytes,
        extract::{Path, State},
        http::StatusCode,
        routing::{get, post, put},
    };
    use futures::TryStreamExt;
    use std::sync::{Arc, Mutex};

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

    #[derive(Default)]
    struct BobsState {
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
            axum::Json(
                serde_json::json!({ "key": "redirect-key", "read_url": "https://polytope.example.com/download-0/redirect-key" }),
            ),
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
        let bobs_state = Arc::new(BobsState::default());
        let bobs_app = Router::new()
            .route("/create", put(bobs_create))
            .route("/write/{key}/{offset}", post(bobs_write))
            .route("/complete/{key}", post(bobs_complete))
            .with_state(bobs_state.clone());
        let bobs_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bobs_addr = bobs_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(bobs_listener, bobs_app).await.unwrap() });
        let bobs_url = format!("http://{bobs_addr}");

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
}
