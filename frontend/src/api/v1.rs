use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bits::{Job, JobResult, PollOutcome};
use bytes::BytesMut;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
pub struct SubmitBody {
    pub verb: String,
    pub request: Value,
}

#[derive(Serialize)]
struct Accepted {
    status: &'static str,
    id: String,
    message: &'static str,
}

#[derive(Serialize)]
struct Queued {
    status: &'static str,
    id: String,
    message: &'static str,
}

pub async fn test() -> &'static str {
    "Polytope server is alive"
}

pub async fn list_collections() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("Deprecation", "true")],
        Json(json!({"message": ["all"]})),
    )
}

pub async fn list_requests() -> impl IntoResponse {
    Json(json!({"message": []}))
}

pub async fn submit_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(_collection): Path<String>,
    Json(body): Json<SubmitBody>,
) -> impl IntoResponse {
    let request = json!({
        "verb": body.verb,
        "request": body.request,
    });

    let mut job = Job::new(request);
    if let Some(ip) = super::client_ip(&headers) {
        job.user = json!({"client_ip": ip});
    }
    let handle = state.bits.submit(job);
    let location = format!("/api/v1/requests/{}", handle.id);
    (
        StatusCode::ACCEPTED,
        [(header::LOCATION, location)],
        Json(Accepted {
            status: "queued",
            id: handle.id,
            message: "Request accepted",
        }),
    )
}

pub async fn get_request(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => (
            StatusCode::ACCEPTED,
            Json(Queued {
                status: "queued",
                id,
                message: "Request is being processed",
            }),
        )
            .into_response(),
        PollOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "request not found"})),
        )
            .into_response(),
        PollOutcome::JobLost => (
            StatusCode::GONE,
            Json(json!({"error": "request state expired or was lost"})),
        )
            .into_response(),
        PollOutcome::Ready(result) => match result {
            JobResult::Success {
                content_type,
                size,
                stream,
            } => {
                if size >= 0 {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, content_type)
                        .header(header::CONTENT_LENGTH, size)
                        .body(Body::from_stream(stream))
                        .unwrap()
                } else {
                    let mut buf = BytesMut::new();
                    tokio::pin!(stream);
                    while let Some(chunk) = stream.try_next().await.unwrap_or(None) {
                        buf.extend_from_slice(&chunk);
                    }
                    let body = buf.freeze();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, content_type)
                        .header(header::CONTENT_LENGTH, body.len())
                        .body(Body::from(body))
                        .unwrap()
                }
            }
            JobResult::Redirect { location, message } => Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location)
                .body(Body::from(message))
                .unwrap(),
            JobResult::Error { message } => (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "failed", "message": message})),
            )
                .into_response(),
            JobResult::Failed { reason } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"status": "failed", "message": reason})),
            )
                .into_response(),
            JobResult::Cancelled => {
                (StatusCode::OK, Json(json!({"status": "cancelled"}))).into_response()
            }
            JobResult::ClientGone => (
                StatusCode::GONE,
                Json(json!({"error": "request abandoned: client disconnected"})),
            )
                .into_response(),
        },
    }
}

pub async fn delete_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.bits.cancel(&id);
    (
        StatusCode::OK,
        Json(json!({"status": "cancelled", "id": id})),
    )
}

pub async fn downloads_deprecated() -> impl IntoResponse {
    (
        StatusCode::GONE,
        [("Deprecation", "true")],
        Json(
            json!({"error": "downloads endpoint is deprecated; poll /api/v1/requests/:id instead"}),
        ),
    )
}
