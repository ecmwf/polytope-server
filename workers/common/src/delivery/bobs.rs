// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;

use super::{DeliveryContext, ResultDelivery, enduser_fields};
use crate::{Completion, metrics};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(super) struct BobsPush {
    pub(super) api_base: String,
    pub(super) create_client: reqwest::Client,
    pub(super) body_client: reqwest::Client,
    pub(super) policy: BobsPolicy,
}

#[derive(Debug, Clone)]
pub(super) struct BobsPolicy {
    /// Total create attempts, including the first.
    pub(super) create_attempts: u32,
    pub(super) create_attempt_timeout: Duration,
    pub(super) discard_timeout: Duration,
}

impl BobsPolicy {
    const DEFAULT_CREATE_ATTEMPTS: u32 = 3;
    const DEFAULT_CREATE_TIMEOUT_SECS: u64 = 30;
    const DISCARD_TIMEOUT: Duration = Duration::from_secs(10);

    pub(super) fn from_config(config: &crate::delivery_config::DeliveryConfig) -> Self {
        let create_attempts = match config.bobs_create_attempts {
            Some(0) => {
                tracing::warn!("bobs_create_attempts must be at least 1; using 1");
                1
            }
            Some(attempts) => attempts,
            None => Self::DEFAULT_CREATE_ATTEMPTS,
        };
        Self {
            create_attempts,
            create_attempt_timeout: Duration::from_secs(
                config
                    .bobs_create_timeout_secs
                    .unwrap_or(Self::DEFAULT_CREATE_TIMEOUT_SECS),
            ),
            discard_timeout: Self::DISCARD_TIMEOUT,
        }
    }
}

struct CreateFailure {
    message: String,
    /// Metric label: the HTTP status code, or `transport`.
    reason: String,
    retryable: bool,
}

/// Statuses worth retrying against another BOBS pod. Every create opens a fresh
/// connection to the Service, so a retry is load-balanced independently of the
/// failed attempt. 503 is admission exhaustion on one pod. 409 means that pod
/// already holds a spool keyed by this job id (an earlier attempt or an earlier
/// dispatch of the job); another pod can still accept it.
fn create_status_is_retryable(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::CONFLICT
        || status == StatusCode::TOO_MANY_REQUESTS
}

#[async_trait]
impl ResultDelivery for BobsPush {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
        metadata: &serde_json::Value,
        context: DeliveryContext<'_>,
    ) -> Completion {
        let buffer_full =
            metadata.get("buffer_full_output").and_then(|v| v.as_bool()) == Some(true);
        match self
            .push(
                content_type,
                content_encoding,
                body,
                buffer_full,
                context.job_id,
                context.user,
                metadata,
            )
            .await
        {
            Ok(location) => Completion::Redirect {
                location,
                message: "result available for download".to_string(),
                content_type: Some(content_type.to_string()),
                content_length: None,
            },
            Err(e) => {
                let (enduser_id, enduser_realm) = enduser_fields(context.user);
                if let Some(source_error) = context.source_error_message() {
                    if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
                        tracing::error!("event.name" = "worker.delivery.failed", outcome = "error", request.id = %context.job_id, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, source_error = %source_error, sink_error = %e, "result delivery failed after source stream error");
                    } else {
                        tracing::error!("event.name" = "worker.delivery.failed", outcome = "error", request.id = %context.job_id, source_error = %source_error, sink_error = %e, "result delivery failed after source stream error");
                    }
                    Completion::Error {
                        message: source_error,
                    }
                } else {
                    if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
                        tracing::error!("event.name" = "worker.delivery.failed", outcome = "error", request.id = %context.job_id, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, error = %e, "result delivery failed");
                    } else {
                        tracing::error!("event.name" = "worker.delivery.failed", outcome = "error", request.id = %context.job_id, error = %e, "result delivery failed");
                    }
                    Completion::Error {
                        message: format!("delivery failed: {e}"),
                    }
                }
            }
        }
    }
}

impl BobsPush {
    #[allow(clippy::too_many_arguments)]
    async fn push(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
        write_locked: bool,
        job_id: &str,
        user: &serde_json::Value,
        metadata: &serde_json::Value,
    ) -> Result<String, BoxError> {
        let mut create_body = serde_json::json!({
            "content_type": content_type,
            "write_locked": write_locked,
        });
        if let Some(enc) = content_encoding {
            create_body["content_encoding"] = serde_json::json!(enc);
        }
        // Propagate collection label for per-collection download metrics in bobs.
        if let Some(collection) = metadata.get("collection").and_then(|v| v.as_str()) {
            create_body["labels"] = serde_json::json!({"collection": collection});
        }
        let create_json = self.create(&create_body, job_id).await?;
        let key = create_json["key"]
            .as_str()
            .ok_or("missing key in response")?
            .to_string();
        let write_base = create_json["write_url"]
            .as_str()
            .ok_or(
                "missing write_url in create response: BOBS server does not support write routing",
            )?
            .to_string();

        let uploaded = async {
            let read_url = create_json["read_url"]
                .as_str()
                .ok_or("missing read_url in response")?
                .to_string();
            self.write_and_complete(&write_base, &key, body, job_id)
                .await?;
            Ok::<_, BoxError>(read_url)
        }
        .await;
        let read_url = match uploaded {
            Ok(read_url) => read_url,
            Err(e) => {
                self.discard(&write_base, &key, job_id).await;
                return Err(e);
            }
        };

        let (enduser_id, enduser_realm) = enduser_fields(user);
        if let (Some(enduser_id), Some(enduser_realm)) = (enduser_id, enduser_realm) {
            tracing::debug!("event.name" = "worker.delivery.completed", outcome = "success", request.id = %job_id, "enduser.id" = %enduser_id, "enduser.realm" = %enduser_realm, bobs.key = %key, read_url = %read_url, "result pushed to BOBS");
        } else {
            tracing::debug!("event.name" = "worker.delivery.completed", outcome = "success", request.id = %job_id, bobs.key = %key, read_url = %read_url, "result pushed to BOBS");
        }
        Ok(read_url)
    }

    async fn create(
        &self,
        create_body: &serde_json::Value,
        job_id: &str,
    ) -> Result<serde_json::Value, BoxError> {
        let max_attempts = self.policy.create_attempts;
        let mut attempt = 1;
        loop {
            let failure = match self.create_once(create_body, job_id).await {
                Ok(json) => return Ok(json),
                Err(failure) => failure,
            };
            if !failure.retryable || attempt >= max_attempts {
                let message = if attempt > 1 {
                    format!("{} (after {attempt} attempts)", failure.message)
                } else {
                    failure.message
                };
                return Err(message.into());
            }
            tracing::warn!(
                "event.name" = "worker.delivery.create.retry",
                outcome = "error",
                request.id = %job_id,
                attempt,
                max_attempts,
                error = %failure.message,
                "BOBS create failed; retrying"
            );
            metrics::record_bobs_create_retry(&failure.reason);
            attempt += 1;
        }
    }

    async fn create_once(
        &self,
        create_body: &serde_json::Value,
        job_id: &str,
    ) -> Result<serde_json::Value, CreateFailure> {
        let response = self
            .create_client
            .put(format!("{}/create", self.api_base))
            .header("X-Polytope-Job-Id", job_id)
            .timeout(self.policy.create_attempt_timeout)
            .json(create_body)
            .send()
            .await
            .map_err(|e| CreateFailure {
                message: format!("create failed: {e}"),
                reason: "transport".to_string(),
                retryable: true,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(CreateFailure {
                message: format!("create failed: {status}"),
                reason: status.as_u16().to_string(),
                retryable: create_status_is_retryable(status),
            });
        }
        response.json().await.map_err(|e| CreateFailure {
            message: format!("create failed: invalid response: {e}"),
            reason: "invalid_response".to_string(),
            retryable: false,
        })
    }

    async fn write_and_complete(
        &self,
        write_base: &str,
        key: &str,
        body: reqwest::Body,
        job_id: &str,
    ) -> Result<(), BoxError> {
        // Stream the entire response body to BOBS as a single chunked HTTP/2
        // request at offset 0. BOBS appends page-by-page as the body arrives;
        // the worker no longer pays one round-trip per producer chunk.
        let write_resp = self
            .body_client
            .post(format!("{write_base}/write/{key}/0"))
            .header("X-Polytope-Job-Id", job_id)
            .body(body)
            .send()
            .await?;
        if !write_resp.status().is_success() {
            return Err(format!("write failed: {}", write_resp.status()).into());
        }

        let complete_resp = self
            .body_client
            .post(format!("{write_base}/complete/{key}"))
            .header("X-Polytope-Job-Id", job_id)
            .send()
            .await?;
        if !complete_resp.status().is_success() {
            return Err(format!("complete failed: {}", complete_resp.status()).into());
        }
        Ok(())
    }

    /// Best-effort delete of a spool whose upload failed after create, so it does
    /// not hold a BOBS admission slot and disk until the writer-inactivity
    /// timeout. A spool left in `Completing` by a failed complete is never
    /// reaped by BOBS on its own. Awaited rather than spawned so it still runs
    /// when the worker restarts right after the failure.
    async fn discard(&self, write_base: &str, key: &str, job_id: &str) {
        let result = self
            .body_client
            .delete(format!("{write_base}/delete/{key}"))
            .header("X-Polytope-Job-Id", job_id)
            .timeout(self.policy.discard_timeout)
            .send()
            .await;
        match result {
            // 404: BOBS already removed it (oversize rejection, writer timeout).
            Ok(resp) if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND => {
                metrics::record_bobs_discard("success");
                tracing::debug!("event.name" = "worker.delivery.discarded", outcome = "success", request.id = %job_id, bobs.key = %key, status = %resp.status(), "discarded partial BOBS spool after failed delivery");
            }
            Ok(resp) => {
                metrics::record_bobs_discard("error");
                tracing::warn!("event.name" = "worker.delivery.discard.failed", outcome = "error", request.id = %job_id, bobs.key = %key, status = %resp.status(), "failed to discard partial BOBS spool; BOBS cleanup will reclaim it");
            }
            Err(e) => {
                metrics::record_bobs_discard("error");
                tracing::warn!("event.name" = "worker.delivery.discard.failed", outcome = "error", request.id = %job_id, bobs.key = %key, error = %e, "failed to discard partial BOBS spool; BOBS cleanup will reclaim it");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{Path, State},
        http::{HeaderMap, StatusCode},
        routing::{post, put},
    };
    use std::sync::{Arc, Mutex};

    fn test_policy() -> BobsPolicy {
        BobsPolicy {
            create_attempts: 3,
            create_attempt_timeout: Duration::from_secs(5),
            discard_timeout: Duration::from_secs(5),
        }
    }

    struct BobsState {
        base_url: String,
        created_keys: Mutex<Vec<String>>,
        written_data: Mutex<Vec<u8>>,
        completed: Mutex<bool>,
        job_headers: Mutex<Vec<String>>,
    }

    fn record_job_header(target: &Mutex<Vec<String>>, headers: &HeaderMap) {
        let value = headers
            .get("x-polytope-job-id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        target.lock().unwrap().push(value);
    }

    async fn mock_create(
        headers: HeaderMap,
        State(state): State<Arc<BobsState>>,
    ) -> (StatusCode, axum::Json<serde_json::Value>) {
        let key = "test-key-123".to_string();
        let read_url = format!("http://public.example.com/download-0/{key}");
        let write_url = state.base_url.clone();
        record_job_header(&state.job_headers, &headers);
        state.created_keys.lock().unwrap().push(key.clone());
        (
            StatusCode::CREATED,
            axum::Json(
                serde_json::json!({ "key": key, "read_url": read_url, "write_url": write_url }),
            ),
        )
    }

    async fn mock_write(
        Path((_key, _offset)): Path<(String, u64)>,
        State(state): State<Arc<BobsState>>,
        headers: HeaderMap,
        body: axum::body::Body,
    ) -> StatusCode {
        record_job_header(&state.job_headers, &headers);
        let data: Vec<u8> = axum::body::to_bytes(body, usize::MAX)
            .await
            .unwrap()
            .to_vec();
        state.written_data.lock().unwrap().extend(data);
        StatusCode::OK
    }

    async fn mock_complete(
        Path(_key): Path<String>,
        headers: HeaderMap,
        State(state): State<Arc<BobsState>>,
    ) -> StatusCode {
        record_job_header(&state.job_headers, &headers);
        *state.completed.lock().unwrap() = true;
        StatusCode::OK
    }

    #[tokio::test]
    async fn bobs_push_delivers_and_returns_redirect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bobs_url = format!("http://{addr}");

        let state = Arc::new(BobsState {
            base_url: bobs_url.clone(),
            created_keys: Mutex::new(vec![]),
            written_data: Mutex::new(vec![]),
            completed: Mutex::new(false),
            job_headers: Mutex::new(vec![]),
        });
        let app = Router::new()
            .route("/create", put(mock_create))
            .route("/write/{key}/{offset}", post(mock_write))
            .route("/complete/{key}", post(mock_complete))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let h2_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .expect("build h2 client");
        let push = BobsPush {
            api_base: bobs_url.clone(),
            create_client: h2_client.clone(),
            body_client: h2_client,
            policy: test_policy(),
        };
        let data = b"hello bobs".to_vec();
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(data.clone()),
                &serde_json::json!({}),
                DeliveryContext {
                    job_id: "job-1",
                    user: &serde_json::json!({}),
                    source_error: None,
                },
            )
            .await;

        match result {
            Completion::Redirect {
                location, message, ..
            } => {
                assert_eq!(
                    location,
                    "http://public.example.com/download-0/test-key-123"
                );
                assert_eq!(message, "result available for download");
            }
            other => panic!("expected Redirect, got {other:?}"),
        }
        assert!(*state.completed.lock().unwrap());
        assert_eq!(*state.written_data.lock().unwrap(), data);
        assert_eq!(
            *state.job_headers.lock().unwrap(),
            vec!["job-1", "job-1", "job-1"]
        );
    }

    struct StreamingBobsState {
        base_url: String,
        created_keys: Mutex<Vec<String>>,
        writes: Mutex<Vec<(u64, Vec<u8>)>>,
        completed: Mutex<bool>,
    }

    async fn streaming_create(
        State(state): State<Arc<StreamingBobsState>>,
    ) -> (StatusCode, axum::Json<serde_json::Value>) {
        let key = "stream-key".to_string();
        let read_url = format!("http://public.example.com/download-0/{key}");
        let write_url = state.base_url.clone();
        state.created_keys.lock().unwrap().push(key.clone());
        (
            StatusCode::CREATED,
            axum::Json(
                serde_json::json!({ "key": key, "read_url": read_url, "write_url": write_url }),
            ),
        )
    }

    async fn streaming_write(
        Path((_key, offset)): Path<(String, u64)>,
        State(state): State<Arc<StreamingBobsState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        let data: Vec<u8> = axum::body::to_bytes(body, usize::MAX)
            .await
            .unwrap()
            .to_vec();
        state.writes.lock().unwrap().push((offset, data));
        StatusCode::OK
    }

    async fn streaming_complete(
        Path(_key): Path<String>,
        State(state): State<Arc<StreamingBobsState>>,
    ) -> StatusCode {
        *state.completed.lock().unwrap() = true;
        StatusCode::OK
    }

    #[tokio::test]
    async fn bobs_push_streams_whole_body_as_one_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bobs_url = format!("http://{addr}");

        let state = Arc::new(StreamingBobsState {
            base_url: bobs_url.clone(),
            created_keys: Mutex::new(vec![]),
            writes: Mutex::new(vec![]),
            completed: Mutex::new(false),
        });
        let app = Router::new()
            .route("/create", put(streaming_create))
            .route("/write/{key}/{offset}", post(streaming_write))
            .route("/complete/{key}", post(streaming_complete))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let h2_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .expect("build h2 client");
        let push = BobsPush {
            api_base: bobs_url.clone(),
            create_client: h2_client.clone(),
            body_client: h2_client,
            policy: test_policy(),
        };

        // Build a body from a multi-chunk producer stream. The delivery layer
        // must coalesce these into one streamed HTTP request to BOBS, not
        // emit one POST per producer chunk.
        let stream = futures::stream::iter(vec![
            Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"abc")),
            Ok(bytes::Bytes::from_static(b"defg")),
        ]);
        let body = reqwest::Body::wrap_stream(stream);

        let result = push
            .deliver(
                "application/octet-stream",
                None,
                body,
                &serde_json::json!({}),
                DeliveryContext {
                    job_id: "job-1",
                    user: &serde_json::json!({}),
                    source_error: None,
                },
            )
            .await;

        match result {
            Completion::Redirect { location, .. } => {
                assert_eq!(location, "http://public.example.com/download-0/stream-key");
            }
            other => panic!("expected Redirect, got {other:?}"),
        }
        assert!(*state.completed.lock().unwrap());

        let writes = state.writes.lock().unwrap();
        assert_eq!(
            writes.len(),
            1,
            "expected exactly one streamed write covering the whole body"
        );
        assert_eq!(writes[0].0, 0, "write must start at offset 0");
        assert_eq!(writes[0].1, b"abcdefg");
    }

    #[tokio::test]
    async fn bobs_push_prefers_source_error_when_stream_fails_mid_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bobs_url = format!("http://{addr}");

        let state = Arc::new(StreamingBobsState {
            base_url: bobs_url.clone(),
            created_keys: Mutex::new(vec![]),
            writes: Mutex::new(vec![]),
            completed: Mutex::new(false),
        });
        let app = Router::new()
            .route("/create", put(streaming_create))
            .route("/write/{key}/{offset}", post(streaming_write))
            .route("/complete/{key}", post(streaming_complete))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let h2_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .expect("build h2 client");
        let push = BobsPush {
            api_base: bobs_url.clone(),
            create_client: h2_client.clone(),
            body_client: h2_client,
            policy: test_policy(),
        };

        let source_error = crate::SourceError::new();
        source_error.set_once(
            "The requested post-processing is not supported for this data. Details: mars-client error: Serious bug: Representation::croppedRepresentation() not implemented for HEALPixNested[name=H128]",
        );
        let stream = futures::stream::iter(vec![
            Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"abc")),
            Err(std::io::Error::other(
                "mars-client error: Serious bug: Representation::croppedRepresentation() not implemented for HEALPixNested[name=H128]",
            )),
        ]);
        let body = reqwest::Body::wrap_stream(stream);

        let result = push
            .deliver(
                "application/octet-stream",
                None,
                body,
                &serde_json::json!({}),
                DeliveryContext {
                    job_id: "job-1",
                    user: &serde_json::json!({}),
                    source_error: Some(source_error),
                },
            )
            .await;

        match result {
            Completion::Error { message } => {
                assert!(
                    message.contains("not supported for this data"),
                    "mapped source message should win, got: {message}"
                );
                assert!(message.contains("croppedRepresentation"));
                assert!(!message.starts_with("delivery failed:"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
        assert!(
            !*state.completed.lock().unwrap(),
            "BOBS object must not be completed after a source stream error"
        );
    }

    #[tokio::test]
    async fn bobs_push_returns_error_when_unreachable() {
        let h2_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .expect("build h2 client");
        let push = BobsPush {
            api_base: "http://127.0.0.1:1".to_string(),
            create_client: h2_client.clone(),
            body_client: h2_client,
            policy: test_policy(),
        };
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(vec![]),
                &serde_json::json!({}),
                DeliveryContext {
                    job_id: "job-1",
                    user: &serde_json::json!({}),
                    source_error: None,
                },
            )
            .await;

        match result {
            Completion::Error { message } => {
                assert!(message.starts_with("delivery failed:"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bobs_push_errors_on_missing_write_url() {
        // Mock create handler that omits write_url.
        async fn create_no_write_url() -> (StatusCode, axum::Json<serde_json::Value>) {
            (
                StatusCode::CREATED,
                axum::Json(serde_json::json!({
                    "key": "k",
                    "read_url": "http://x/k",
                })),
            )
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/create", put(create_no_write_url));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let h2_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .expect("build h2 client");
        let push = BobsPush {
            api_base: format!("http://{addr}"),
            create_client: h2_client.clone(),
            body_client: h2_client,
            policy: test_policy(),
        };
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(vec![]),
                &serde_json::json!({}),
                DeliveryContext {
                    job_id: "job-1",
                    user: &serde_json::json!({}),
                    source_error: None,
                },
            )
            .await;

        match result {
            Completion::Error { message } => {
                assert!(
                    message.contains("missing write_url"),
                    "expected 'missing write_url' in error message, got: {message}"
                );
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    /// BOBS mock whose create/complete/delete responses are scripted per test.
    struct ScriptedBobs {
        base_url: String,
        /// Statuses returned by successive create calls; once exhausted, create succeeds.
        create_failures: Mutex<std::collections::VecDeque<StatusCode>>,
        complete_status: StatusCode,
        delete_status: StatusCode,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedBobs {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn count(&self, call: &str) -> usize {
            self.calls().iter().filter(|c| c.as_str() == call).count()
        }
    }

    async fn scripted_create(
        State(state): State<Arc<ScriptedBobs>>,
    ) -> (StatusCode, axum::Json<serde_json::Value>) {
        state.calls.lock().unwrap().push("create".to_string());
        if let Some(status) = state.create_failures.lock().unwrap().pop_front() {
            return (status, axum::Json(serde_json::json!({"error": "scripted"})));
        }
        (
            StatusCode::CREATED,
            axum::Json(serde_json::json!({
                "key": "job-1",
                "read_url": "http://public.example.com/download-0/job-1",
                "write_url": state.base_url.clone(),
            })),
        )
    }

    async fn scripted_write(
        Path((_key, _offset)): Path<(String, u64)>,
        State(state): State<Arc<ScriptedBobs>>,
        body: axum::body::Body,
    ) -> StatusCode {
        state.calls.lock().unwrap().push("write".to_string());
        match axum::body::to_bytes(body, usize::MAX).await {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::BAD_REQUEST,
        }
    }

    async fn scripted_complete(
        Path(_key): Path<String>,
        State(state): State<Arc<ScriptedBobs>>,
    ) -> StatusCode {
        state.calls.lock().unwrap().push("complete".to_string());
        state.complete_status
    }

    async fn scripted_delete(
        Path(key): Path<String>,
        State(state): State<Arc<ScriptedBobs>>,
    ) -> StatusCode {
        state.calls.lock().unwrap().push(format!("delete:{key}"));
        state.delete_status
    }

    async fn start_scripted_bobs(
        create_failures: Vec<StatusCode>,
        complete_status: StatusCode,
        delete_status: StatusCode,
    ) -> (Arc<ScriptedBobs>, BobsPush) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(ScriptedBobs {
            base_url: base_url.clone(),
            create_failures: Mutex::new(create_failures.into()),
            complete_status,
            delete_status,
            calls: Mutex::new(vec![]),
        });
        let app = Router::new()
            .route("/create", put(scripted_create))
            .route("/write/{key}/{offset}", post(scripted_write))
            .route("/complete/{key}", post(scripted_complete))
            .route("/delete/{key}", axum::routing::delete(scripted_delete))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let h2_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .expect("build h2 client");
        let push = BobsPush {
            api_base: base_url,
            create_client: h2_client.clone(),
            body_client: h2_client,
            policy: test_policy(),
        };
        (state, push)
    }

    async fn deliver_bytes(push: &BobsPush, body: reqwest::Body) -> Completion {
        push.deliver(
            "application/octet-stream",
            None,
            body,
            &serde_json::json!({}),
            DeliveryContext {
                job_id: "job-1",
                user: &serde_json::json!({}),
                source_error: None,
            },
        )
        .await
    }

    #[tokio::test]
    async fn create_is_retried_on_503_and_conflict_then_succeeds() {
        let (state, push) = start_scripted_bobs(
            vec![StatusCode::SERVICE_UNAVAILABLE, StatusCode::CONFLICT],
            StatusCode::OK,
            StatusCode::OK,
        )
        .await;

        let result = deliver_bytes(&push, reqwest::Body::from(b"data".to_vec())).await;

        assert!(
            matches!(result, Completion::Redirect { .. }),
            "got {result:?}"
        );
        assert_eq!(
            state.calls(),
            vec!["create", "create", "create", "write", "complete"]
        );
    }

    #[tokio::test]
    async fn create_gives_up_after_max_attempts_keeping_status_in_message() {
        let (state, push) = start_scripted_bobs(
            vec![StatusCode::SERVICE_UNAVAILABLE; 5],
            StatusCode::OK,
            StatusCode::OK,
        )
        .await;

        let result = deliver_bytes(&push, reqwest::Body::from(b"data".to_vec())).await;

        match result {
            Completion::Error { message } => assert_eq!(
                message,
                "delivery failed: create failed: 503 Service Unavailable (after 3 attempts)"
            ),
            other => panic!("expected Error, got {other:?}"),
        }
        assert_eq!(state.calls(), vec!["create", "create", "create"]);
    }

    #[tokio::test]
    async fn create_client_errors_are_not_retried() {
        let (state, push) = start_scripted_bobs(
            vec![StatusCode::BAD_REQUEST],
            StatusCode::OK,
            StatusCode::OK,
        )
        .await;

        let result = deliver_bytes(&push, reqwest::Body::from(b"data".to_vec())).await;

        match result {
            Completion::Error { message } => {
                assert_eq!(message, "delivery failed: create failed: 400 Bad Request")
            }
            other => panic!("expected Error, got {other:?}"),
        }
        assert_eq!(state.calls(), vec!["create"]);
    }

    #[tokio::test]
    async fn spool_is_discarded_when_source_fails_mid_stream() {
        let (state, push) = start_scripted_bobs(vec![], StatusCode::OK, StatusCode::OK).await;
        let stream = futures::stream::iter(vec![
            Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"abc")),
            Err(std::io::Error::other("source died")),
        ]);

        let result = deliver_bytes(&push, reqwest::Body::wrap_stream(stream)).await;

        assert!(matches!(result, Completion::Error { .. }), "got {result:?}");
        assert_eq!(state.count("complete"), 0);
        assert_eq!(state.count("delete:job-1"), 1);
    }

    #[tokio::test]
    async fn spool_is_discarded_when_complete_fails() {
        let (state, push) =
            start_scripted_bobs(vec![], StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK).await;

        let result = deliver_bytes(&push, reqwest::Body::from(b"data".to_vec())).await;

        match result {
            Completion::Error { message } => {
                assert!(message.contains("complete failed: 500"), "got {message}")
            }
            other => panic!("expected Error, got {other:?}"),
        }
        assert_eq!(
            state.calls(),
            vec!["create", "write", "complete", "delete:job-1"]
        );
    }

    #[tokio::test]
    async fn successful_push_does_not_discard() {
        let (state, push) = start_scripted_bobs(vec![], StatusCode::OK, StatusCode::OK).await;

        let result = deliver_bytes(&push, reqwest::Body::from(b"data".to_vec())).await;

        assert!(
            matches!(result, Completion::Redirect { .. }),
            "got {result:?}"
        );
        assert_eq!(state.calls(), vec!["create", "write", "complete"]);
    }

    #[tokio::test]
    async fn failed_discard_keeps_the_original_error() {
        let (state, push) = start_scripted_bobs(
            vec![],
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;

        let result = deliver_bytes(&push, reqwest::Body::from(b"data".to_vec())).await;

        match result {
            Completion::Error { message } => {
                assert!(message.contains("complete failed: 500"), "got {message}")
            }
            other => panic!("expected Error, got {other:?}"),
        }
        assert_eq!(state.count("delete:job-1"), 1);
    }

    #[test]
    fn policy_reads_config_with_defaults() {
        let mut config = crate::delivery_config::DeliveryConfig {
            delivery_type: crate::delivery_config::DeliveryType::Bobs,
            bobs_url: Some("http://bobs:3000".to_string()),
            bobs_create_attempts: None,
            bobs_create_timeout_secs: None,
            s3_bucket: None,
            s3_region: None,
            s3_endpoint_url: None,
            s3_force_path_style: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_presigned_url_expiry_secs: None,
            s3_public_url: None,
            s3_key_prefix: String::new(),
        };
        let policy = BobsPolicy::from_config(&config);
        assert_eq!(policy.create_attempts, 3);
        assert_eq!(policy.create_attempt_timeout, Duration::from_secs(30));

        config.bobs_create_attempts = Some(7);
        config.bobs_create_timeout_secs = Some(9);
        let policy = BobsPolicy::from_config(&config);
        assert_eq!(policy.create_attempts, 7);
        assert_eq!(policy.create_attempt_timeout, Duration::from_secs(9));

        config.bobs_create_attempts = Some(0);
        assert_eq!(BobsPolicy::from_config(&config).create_attempts, 1);
    }

    #[test]
    fn create_retry_classification() {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::CONFLICT,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert!(create_status_is_retryable(status), "{status}");
        }
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::NOT_FOUND,
            StatusCode::PAYLOAD_TOO_LARGE,
        ] {
            assert!(!create_status_is_retryable(status), "{status}");
        }
    }
}
