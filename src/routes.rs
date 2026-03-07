use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bits::{Job, JobResult, PollOutcome};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Request/response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SubmitBody {
    pub verb: String,
    pub request: Value,
}

#[derive(Serialize)]
struct Accepted {
    status: &'static str,
    id: String,
}

#[derive(Serialize)]
struct Queued {
    status: &'static str,
    id: String,
}

// ---------------------------------------------------------------------------
// GET /api/v1/test
// ---------------------------------------------------------------------------

pub async fn test() -> &'static str {
    "Polytope server is alive"
}

// ---------------------------------------------------------------------------
// GET /api/v1/collections  (deprecated — always returns ["all"])
// ---------------------------------------------------------------------------

pub async fn list_collections() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("Deprecation", "true")],
        Json(json!(["all"])),
    )
}

// ---------------------------------------------------------------------------
// GET /api/v1/requests
// ---------------------------------------------------------------------------

pub async fn list_requests() -> impl IntoResponse {
    Json(json!([]))
}

// ---------------------------------------------------------------------------
// POST /api/v1/requests/:collection
// (collection is ignored — requests are routed by bits config)
// ---------------------------------------------------------------------------

pub async fn submit_request(
    State(state): State<Arc<AppState>>,
    Path(_collection): Path<String>,
    Json(body): Json<SubmitBody>,
) -> impl IntoResponse {
    let request = json!({
        "verb": body.verb,
        "request": body.request,
    });

    let job = Job::new(request);
    let handle = state.bits.submit(job);

    (
        StatusCode::ACCEPTED,
        Json(Accepted {
            status: "queued",
            id: handle.id,
        }),
    )
}

// ---------------------------------------------------------------------------
// GET /api/v1/requests/:id
// ---------------------------------------------------------------------------

pub async fn get_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.bits.poll(&id, POLL_TIMEOUT).await {
        PollOutcome::Pending { id } => (
            StatusCode::ACCEPTED,
            Json(Queued {
                status: "queued",
                id,
            }),
        )
            .into_response(),

        PollOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "request not found"})),
        )
            .into_response(),

        PollOutcome::Ready(result) => match result {
            JobResult::Success {
                content_type,
                stream,
                ..
            } => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from_stream(stream))
                .unwrap(),

            JobResult::Redirect { location, message } => Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location)
                .body(Body::from(message))
                .unwrap(),

            JobResult::Error { message } => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": message})),
            )
                .into_response(),

            JobResult::Failed { reason } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": reason})),
            )
                .into_response(),
        },
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/requests/:id
// ---------------------------------------------------------------------------

pub async fn delete_request(Path(id): Path<String>) -> impl IntoResponse {
    // Cancellation is not yet implemented in bits.
    tracing::warn!(id, "delete_request called but cancellation is not implemented");
    (
        StatusCode::OK,
        Json(json!({"status": "deleted", "id": id})),
    )
}

// ---------------------------------------------------------------------------
// GET /api/v1/downloads/:id  (deprecated)
// ---------------------------------------------------------------------------

pub async fn downloads_deprecated() -> impl IntoResponse {
    (
        StatusCode::GONE,
        [("Deprecation", "true")],
        Json(json!({"error": "downloads endpoint is deprecated; poll /api/v1/requests/:id instead"})),
    )
}
