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
use serde_json::{json, Value};

use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn health() -> &'static str {
    "Polytope server is alive"
}

pub async fn submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let mut job = Job::new(body);
    if let Some(ip) = super::client_ip(&headers) {
        job.user = json!({"client_ip": ip});
    }
    // Propagate Accept-Encoding so workers can choose an encoding codec
    if let Some(enc) = headers.get(axum::http::header::ACCEPT_ENCODING).and_then(|v| v.to_str().ok()) {
        job.metadata["accept_encoding"] = serde_json::json!(enc);
    }
    let proxy_proto_addr = headers
        .get("x-proxy-protocol-addr")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<not set>");
    tracing::info!(
        "client IP candidates: x-forwarded-for={:?}, x-real-ip={:?}, x-proxy-protocol-addr={:?}",
        headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()),
        headers.get("x-real-ip").and_then(|v| v.to_str().ok()),
        proxy_proto_addr,
    );
    let id = state.bits.submit(job).id;

    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, format!("/api/v2/requests/{}", id))
            .body(Body::empty())
            .unwrap(),
        PollOutcome::NotFound => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
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
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, content_type);
                if size >= 0 {
                    builder = builder.header(header::CONTENT_LENGTH, size);
                }
                builder.body(Body::from_stream(stream)).unwrap()
            }
            JobResult::Redirect { location, message } => Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location)
                .body(Body::from(message))
                .unwrap(),
            JobResult::Error { message } => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
            JobResult::Failed { reason } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": reason})),
            )
                .into_response(),
            JobResult::ClientGone => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "client disconnected before data could be delivered"})),
            )
                .into_response(),
            JobResult::Cancelled => {
                (StatusCode::OK, Json(json!({"status": "cancelled"}))).into_response()
            }
        },
    }
}

pub async fn poll(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, format!("/api/v2/requests/{}", id))
            .body(Body::empty())
            .unwrap(),
        PollOutcome::NotFound => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
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
                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, content_type);
                if size >= 0 {
                    builder = builder.header(header::CONTENT_LENGTH, size);
                }
                builder.body(Body::from_stream(stream)).unwrap()
            }
            JobResult::Redirect { location, message } => Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location)
                .body(Body::from(message))
                .unwrap(),
            JobResult::Error { message } => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
            JobResult::Failed { reason } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": reason})),
            )
                .into_response(),
            JobResult::ClientGone => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "client disconnected before data could be delivered"})),
            )
                .into_response(),
            JobResult::Cancelled => {
                (StatusCode::OK, Json(json!({"status": "cancelled"}))).into_response()
            }
        },
    }
}

pub async fn cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.bits.cancel(&id);
    (
        StatusCode::OK,
        Json(json!({"id": id, "status": "cancelled"})),
    )
}
