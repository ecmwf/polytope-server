use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use bits::{Job, JobResult, PollOutcome, SubmitOutcome};
use bytes::BytesMut;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::{AuthUser, MockRolesAudit};
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

pub async fn list_collections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut collections: Vec<String> = state.collections.keys().cloned().collect();
    collections.sort();
    tracing::info!(
        "event.name" = "api.collection.list",
        outcome = "success",
        collection_count = collections.len() as u64,
        api.version = "v1",
        "listed collections"
    );
    (StatusCode::OK, Json(json!({"collections": collections})))
}

pub async fn list_requests() -> impl IntoResponse {
    Json(json!({"message": []}))
}

pub async fn submit_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    auth_user: Option<Extension<AuthUser>>,
    mock_audit: Option<Extension<MockRolesAudit>>,
    mock_time_extensions: super::MockTimeSubmissionExtensions,
    Path(collection): Path<String>,
    Json(body): Json<SubmitBody>,
) -> Response {
    let route_handle = match state.collections.get(&collection) {
        Some(handle) => handle.clone(),
        None => {
            tracing::warn!("event.name" = "api.job.rejected", outcome = "rejected", collection = %collection, reason = "unknown_collection", "job rejected");
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("unknown collection '{collection}'")})),
            )
                .into_response();
        }
    };

    let mut request = json!({ "request": body.request });
    if let Err(msg) = super::flatten_request(&mut request) {
        tracing::warn!("event.name" = "api.job.rejected", outcome = "rejected", reason = "invalid_request", error = %msg, "job rejected");
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response();
    }

    let mut job = Job::new(request);
    super::set_job_user_context(
        &mut job,
        &headers,
        auth_user.as_ref().map(|Extension(user)| user),
        mock_audit.as_ref().map(|Extension(audit)| audit),
        &state.admin_bypass_roles,
    );
    // v1 clients require Content-Length, so delivery must buffer the full
    // output before making it available for download.
    job.metadata_mut()["buffer_full_output"] = json!(true);
    job.metadata_mut()["collection"] = json!(&collection);
    super::set_job_mock_time_metadata(&mut job, mock_time_extensions.mock_time.as_ref());

    let submitted_request = job.request.clone();
    let enqueue_started = Instant::now();
    let handle = match route_handle.submit(job) {
        SubmitOutcome::Accepted(handle) => handle,
        SubmitOutcome::Overloaded => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "broker at capacity"})),
            )
                .into_response();
        }
    };
    let enqueue_ms = enqueue_started.elapsed().as_millis() as u64;
    super::audit_mock_job_submission(
        mock_audit.as_ref().map(|Extension(audit)| audit),
        &handle.id,
    );
    super::audit_mock_time_job_submission(
        mock_time_extensions.mock_time_audit.as_ref(),
        &handle.id,
    );

    if let Some(Extension(user)) = auth_user.as_ref() {
        tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %handle.id, enqueue_ms, "enduser.id" = %user.username, "enduser.realm" = %user.realm, polytope.request = %polytope_observability::request(&submitted_request), "job submitted");
    } else {
        tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %handle.id, enqueue_ms, polytope.request = %polytope_observability::request(&submitted_request), "job submitted");
    }
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
        .into_response()
}

pub async fn get_request(
    State(state): State<Arc<AppState>>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
) -> Response {
    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => {
            if let Some(Extension(user)) = auth_user.as_ref() {
                tracing::debug!("event.name" = "api.job.poll.pending", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job poll pending");
            } else {
                tracing::debug!("event.name" = "api.job.poll.pending", outcome = "success", request.id = %id, "job poll pending");
            }
            (
                StatusCode::ACCEPTED,
                Json(Queued {
                    status: "queued",
                    id,
                    message: "Request is being processed",
                }),
            )
                .into_response()
        }
        PollOutcome::NotFound => {
            if let Some(Extension(user)) = auth_user.as_ref() {
                tracing::debug!("event.name" = "api.job.poll.failed", outcome = "error", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, reason = "not_found", "job poll failed");
            } else {
                tracing::debug!("event.name" = "api.job.poll.failed", outcome = "error", request.id = %id, reason = "not_found", "job poll failed");
            }
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "request not found"})),
            )
                .into_response()
        }
        PollOutcome::JobLost => {
            if let Some(Extension(user)) = auth_user.as_ref() {
                tracing::warn!("event.name" = "api.job.poll.failed", outcome = "error", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, reason = "job_lost", "job poll failed");
            } else {
                tracing::warn!("event.name" = "api.job.poll.failed", outcome = "error", request.id = %id, reason = "job_lost", "job poll failed");
            }
            (
                StatusCode::GONE,
                Json(json!({"error": "request state expired or was lost"})),
            )
                .into_response()
        }
        PollOutcome::Ready(result) => match result {
            JobResult::Success {
                content_type,
                size,
                stream,
            } => {
                if let Some(Extension(user)) = auth_user.as_ref() {
                    tracing::info!("event.name" = "api.job.poll.completed", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, content_length = size, "job poll completed");
                } else {
                    tracing::info!("event.name" = "api.job.poll.completed", outcome = "success", request.id = %id, content_length = size, "job poll completed");
                }
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
            JobResult::Redirect { location, message } => {
                if let Some(Extension(user)) = auth_user.as_ref() {
                    tracing::info!("event.name" = "api.job.poll.completed", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job poll completed");
                } else {
                    tracing::info!("event.name" = "api.job.poll.completed", outcome = "success", request.id = %id, "job poll completed");
                }
                Response::builder()
                    .status(StatusCode::SEE_OTHER)
                    .header(header::LOCATION, location)
                    .body(Body::from(message))
                    .unwrap()
            }
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
            JobResult::Overloaded { reason } => super::overloaded_response(json!({
                "status": "failed",
                "message": reason,
                "retryable": true,
            })),
            JobResult::Cancelled => {
                if let Some(Extension(user)) = auth_user.as_ref() {
                    tracing::info!("event.name" = "api.job.poll.cancelled", outcome = "cancelled", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job poll cancelled");
                } else {
                    tracing::info!("event.name" = "api.job.poll.cancelled", outcome = "cancelled", request.id = %id, "job poll cancelled");
                }
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
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.bits.cancel(&id);
    if let Some(Extension(user)) = auth_user.as_ref() {
        tracing::info!("event.name" = "api.job.cancelled", outcome = "cancelled", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job cancelled");
    } else {
        tracing::info!("event.name" = "api.job.cancelled", outcome = "cancelled", request.id = %id, "job cancelled");
    }
    // The polytope-client SDK reads `messages["message"]` from the JSON
    // body of this endpoint and KeyErrors if it isn't present. The legacy
    // polytope-server included a `message` field, so keep one here for
    // backwards compatibility with the published SDK.
    (
        StatusCode::OK,
        Json(json!({
            "status": "cancelled",
            "id": id,
            "message": "Request revoked",
        })),
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
