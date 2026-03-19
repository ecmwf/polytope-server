use async_trait::async_trait;

use super::ResultDelivery;
use crate::Completion;

pub(super) struct BobsPush {
    pub(super) bobs_url: String,
    pub(super) client: reqwest::Client,
}

#[async_trait]
impl ResultDelivery for BobsPush {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
    ) -> Completion {
        match self.push(content_type, content_encoding, body).await {
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
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut create_body = serde_json::json!({ "content_type": content_type });
        if let Some(enc) = content_encoding {
            create_body["content_encoding"] = serde_json::json!(enc);
        }
        let create_resp = self
            .client
            .put(format!("{}/create", self.bobs_url))
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

        let write_resp = self
            .client
            .post(format!("{}/write/{}/0", self.bobs_url, key))
            .body(body)
            .send()
            .await?;
        if !write_resp.status().is_success() {
            return Err(format!("write failed: {}", write_resp.status()).into());
        }

        let complete_resp = self
            .client
            .post(format!("{}/complete/{}", self.bobs_url, key))
            .send()
            .await?;
        if !complete_resp.status().is_success() {
            return Err(format!("complete failed: {}", complete_resp.status()).into());
        }

        Ok(format!("{}/read/{}", self.bobs_url, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Path, State},
        http::StatusCode,
        routing::{post, put},
        Router,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct BobsState {
        created_keys: Mutex<Vec<String>>,
        written_data: Mutex<Vec<u8>>,
        completed: Mutex<bool>,
    }

    async fn mock_create(
        State(state): State<Arc<BobsState>>,
    ) -> (StatusCode, axum::Json<serde_json::Value>) {
        let key = "test-key-123".to_string();
        state.created_keys.lock().unwrap().push(key.clone());
        (StatusCode::CREATED, axum::Json(serde_json::json!({ "key": key })))
    }

    async fn mock_write(
        Path((_key, _offset)): Path<(String, u64)>,
        State(state): State<Arc<BobsState>>,
        body: axum::body::Body,
    ) -> StatusCode {
        let data: Vec<u8> = axum::body::to_bytes(body, usize::MAX).await.unwrap().to_vec();
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
        let state = Arc::new(BobsState::default());
        let app = Router::new()
            .route("/create", put(mock_create))
            .route("/write/{key}/{offset}", post(mock_write))
            .route("/complete/{key}", post(mock_complete))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let bobs_url = format!("http://{addr}");
        let push = BobsPush {
            bobs_url: bobs_url.clone(),
            client: reqwest::Client::new(),
        };
        let data = b"hello bobs".to_vec();
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(data.clone()),
            )
            .await;

        match result {
            Completion::Redirect { location, message } => {
                assert_eq!(location, format!("{bobs_url}/read/test-key-123"));
                assert_eq!(message, "result available for download");
            }
            other => panic!("expected Redirect, got {other:?}"),
        }
        assert!(*state.completed.lock().unwrap());
        assert_eq!(*state.written_data.lock().unwrap(), data);
    }

    #[tokio::test]
    async fn bobs_push_returns_error_when_unreachable() {
        let push = BobsPush {
            bobs_url: "http://127.0.0.1:1".to_string(),
            client: reqwest::Client::new(),
        };
        let result = push
            .deliver(
                "application/octet-stream",
                None,
                reqwest::Body::from(vec![]),
            )
            .await;

        match result {
            Completion::Error { message } => {
                assert!(message.starts_with("delivery failed:"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
