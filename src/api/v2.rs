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
use serde_json::{json, Value};

use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// GET /api/v2/test
// ---------------------------------------------------------------------------

pub async fn test() -> &'static str {
    "Polytope server is alive"
}

// ---------------------------------------------------------------------------
// POST /api/v2/requests
//
// Body: any JSON object — passed directly to bits as the job request.
// ---------------------------------------------------------------------------

pub async fn submit(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let job = Job::new(body);
    let id = job.id.clone();
    state.bits.submit(job);

    // 303 immediately to the poll URL so that redirect-following clients
    // (curl -L) enter the poll loop without any extra steps.
    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, format!("/api/v2/requests/{}", id))
        .body(Body::empty())
        .unwrap()
}

// ---------------------------------------------------------------------------
// GET /api/v2/requests/{id}
//
// Long-polls for up to POLL_TIMEOUT. Returns:
//   202  — still running, retry
//   200  — complete, body is the result stream
//   303  — redirect to data location
//   400  — request-level error
//   500  — system failure
// ---------------------------------------------------------------------------

pub async fn poll(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        // 303 back to self: redirect-following clients (curl -L, etc.) will
        // re-issue the GET and long-poll again, looping until 200 or an error.
        PollOutcome::Pending { id } => Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, format!("/api/v2/requests/{}", id))
            .body(Body::empty())
            .unwrap(),

        PollOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "not found"})),
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

            JobResult::ClientGone => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "client disconnected before data could be delivered"})),
            )
                .into_response(),

            JobResult::Cancelled => (
                StatusCode::OK,
                Json(json!({"status": "cancelled"})),
            )
                .into_response(),
        },
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/v2/requests/{id}
// ---------------------------------------------------------------------------

pub async fn cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.bits.cancel(&id);
    (StatusCode::OK, Json(json!({"id": id, "status": "cancelled"})))
}
