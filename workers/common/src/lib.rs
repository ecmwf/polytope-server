use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{signal, SignalKind};

pub mod delivery_config;
pub mod delivery;
pub mod encoding;

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
    async fn process(&self, work: WorkItem) -> Completion;
}

pub async fn run_worker_loop<P: Processor>(
    config: WorkerConfig,
    processor: P,
) -> Result<(), reqwest::Error> {
    let client = reqwest::Client::builder().build()?;
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
                        tracing::warn!(error = %err, "worker poll failed");
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
            tracing::warn!(status = %response.status(), "worker poll returned unexpected status");
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

        let completion = processor.process(work.clone()).await;
        stop.notify_one();
        let _ = heartbeat.await;

        let response = match completion {
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
                request.send().await?
            }
            Completion::Reject { reason } => {
                let payload = CompletionRequest::Reject { reason };
                client
                    .post(config.complete_reject_url(&work.job_id))
                    .json(&payload)
                    .send()
                    .await?
            }
            Completion::Error { message } => {
                let payload = CompletionRequest::Error { message };
                client
                    .post(config.complete_error_url(&work.job_id))
                    .json(&payload)
                    .send()
                    .await?
            }
            Completion::Redirect { location, message } => {
                let payload = CompletionRequest::Redirect { location, message };
                client
                    .post(config.complete_redirect_url(&work.job_id))
                    .json(&payload)
                    .send()
                    .await?
            }
        };
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
        body::Bytes,
        extract::{Path, State},
        http::StatusCode,
        routing::{get, post, put},
        Router,
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

    #[derive(Default)]
    struct BrokerState {
        delivered: Mutex<bool>,
        completions: Mutex<Vec<(String, Vec<u8>)>>,
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
                metadata: serde_json::json!({}),
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
            axum::Json(serde_json::json!({ "key": "redirect-key" })),
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

    struct DeliveryProcessor {
        delivery_config: delivery_config::DeliveryConfig,
    }

    #[async_trait]
    impl Processor for DeliveryProcessor {
        async fn process(&self, _work: WorkItem) -> Completion {
            let client = reqwest::Client::new();
            let delivery = delivery::make_delivery(&self.delivery_config, client).await;
            let stream = futures::stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(
                bytes::Bytes::from(vec![1u8, 2, 3]),
            )]);
            let encoded = encoding::encode_stream(stream, &self.delivery_config.encoding);

            delivery
                .deliver(
                    "application/octet-stream",
                    self.delivery_config.encoding.content_encoding_header(),
                    encoded,
                )
                .await
        }
    }

    struct StubProcessor;

    #[async_trait]
    impl Processor for StubProcessor {
        async fn process(&self, _work: WorkItem) -> Completion {
            Completion::complete(
                "application/octet-stream",
                None,
                Some(3),
                reqwest::Body::from(vec![1, 2, 3]),
            )
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
        };

        let run = tokio::spawn(run_worker_loop(config, StubProcessor));
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
            .route("/complete/redirect/{job_id}", post(broker_complete_redirect))
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
        };

        let processor = DeliveryProcessor {
            delivery_config: delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Bobs,
                bobs_url: Some(bobs_url.clone()),
                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_key_prefix: String::new(),
                encoding: delivery_config::Codec::Identity,
                encoding_threshold_bytes: 1024,
            },
        };

        let run = tokio::spawn(run_worker_loop(config, processor));
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
            format!("{bobs_url}/read/redirect-key")
        );
        assert_eq!(body["message"].as_str().unwrap(), "result available for download");

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
            .route("/complete/redirect/{job_id}", post(broker_complete_redirect))
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
        };

        let processor = DeliveryProcessor {
            delivery_config: delivery_config::DeliveryConfig {
                delivery_type: delivery_config::DeliveryType::Direct,
                bobs_url: None,
                s3_bucket: None,
                s3_region: None,
                s3_endpoint_url: None,
                s3_force_path_style: None,
                s3_key_prefix: String::new(),
                encoding: delivery_config::Codec::Zstd,
                encoding_threshold_bytes: 1024,
            },
        };

        let run = tokio::spawn(run_worker_loop(config, processor));
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
}
