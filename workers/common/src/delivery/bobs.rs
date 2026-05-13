use async_trait::async_trait;
use futures::TryStreamExt;
use http_body_util::BodyDataStream;

use super::ResultDelivery;
use crate::Completion;

pub(super) struct BobsPush {
    pub(super) api_base: String,
    pub(super) create_client: reqwest::Client,
    pub(super) body_client: reqwest::Client,
}

#[async_trait]
impl ResultDelivery for BobsPush {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
        metadata: &serde_json::Value,
    ) -> Completion {
        let buffer_full =
            metadata.get("buffer_full_output").and_then(|v| v.as_bool()) == Some(true);
        match self
            .push(content_type, content_encoding, body, buffer_full)
            .await
        {
            Ok(location) => Completion::Redirect {
                location,
                message: "result available for download".to_string(),
            },
            Err(e) => Completion::Error {
                message: format!("delivery failed: {e}"),
            },
        }
    }
}

impl BobsPush {
    async fn push(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
        write_locked: bool,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut create_body = serde_json::json!({
            "content_type": content_type,
            "write_locked": write_locked,
        });
        if let Some(enc) = content_encoding {
            create_body["content_encoding"] = serde_json::json!(enc);
        }
        let create_resp = self
            .create_client
            .put(format!("{}/create", self.api_base))
            .json(&create_body)
            .send()
            .await?;
        if !create_resp.status().is_success() {
            return Err(format!("create failed: {}", create_resp.status()).into());
        }
        let create_json: serde_json::Value = create_resp.json().await?;
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

        let mut stream = BodyDataStream::new(body);
        let mut offset: u64 = 0;
        while let Some(chunk) =
            stream
                .try_next()
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("body stream error: {e}").into()
                })?
        {
            let chunk_len = chunk.len() as u64;
            let write_resp = self
                .body_client
                .post(format!("{}/write/{}/{}", write_base, key, offset))
                .body(chunk)
                .send()
                .await?;
            if !write_resp.status().is_success() {
                return Err(format!("write failed: {}", write_resp.status()).into());
            }
            offset += chunk_len;
        }

        let complete_resp = self
            .body_client
            .post(format!("{}/complete/{}", write_base, key))
            .send()
            .await?;
        if !complete_resp.status().is_success() {
            return Err(format!("complete failed: {}", complete_resp.status()).into());
        }

        let read_url = create_json["read_url"]
            .as_str()
            .ok_or("missing read_url in response")?
            .to_string();
        tracing::info!(key = %key, read_url = %read_url, "result pushed to BOBS");
        Ok(read_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{Path, State},
        http::StatusCode,
        routing::{post, put},
    };
    use std::sync::{Arc, Mutex};

    struct BobsState {
        base_url: String,
        created_keys: Mutex<Vec<String>>,
        written_data: Mutex<Vec<u8>>,
        completed: Mutex<bool>,
    }

    async fn mock_create(
        State(state): State<Arc<BobsState>>,
    ) -> (StatusCode, axum::Json<serde_json::Value>) {
        let key = "test-key-123".to_string();
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

    async fn mock_write(
        Path((_key, _offset)): Path<(String, u64)>,
        State(state): State<Arc<BobsState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        let data: Vec<u8> = axum::body::to_bytes(body, usize::MAX)
            .await
            .unwrap()
            .to_vec();
        state.written_data.lock().unwrap().extend(data);
        StatusCode::OK
    }

    async fn mock_complete(
        Path(_key): Path<String>,
        State(state): State<Arc<BobsState>>,
    ) -> StatusCode {
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
        };
        let data = b"hello bobs".to_vec();
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(data.clone()),
                &serde_json::json!({}),
            )
            .await;

        match result {
            Completion::Redirect { location, message } => {
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
    async fn bobs_push_streams_chunks_at_correct_offsets() {
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
        };

        // Build a body from a multi-chunk stream (3 bytes, then 4 bytes).
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
        assert_eq!(writes.len(), 2, "expected 2 streamed writes");
        assert_eq!(writes[0].0, 0, "first chunk should be at offset 0");
        assert_eq!(writes[0].1, b"abc");
        assert_eq!(writes[1].0, 3, "second chunk should be at offset 3");
        assert_eq!(writes[1].1, b"defg");
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
        };
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(vec![]),
                &serde_json::json!({}),
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
        };
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(vec![]),
                &serde_json::json!({}),
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
}
