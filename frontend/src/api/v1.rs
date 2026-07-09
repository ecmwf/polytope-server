// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use bits::{ActiveJobSnapshot, Job, JobResult, PollOutcome, SubmitOutcome};
use bytes::BytesMut;
use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::auth::{AuthUser, MockRolesAudit};
use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// `Retry-After` seconds advertised on 202 (queued/pending) responses, matching
/// the legacy Python frontend so clients keep the same poll cadence.
const RETRY_AFTER_SECS: &str = "5";

#[derive(Deserialize)]
pub struct SubmitBody {
    pub verb: String,
    pub request: Value,
}

fn request_queued_body() -> Value {
    json!({"message": "Request queued", "status": "queued"})
}

pub async fn test() -> impl IntoResponse {
    Json(json!({"message": "Polytope server is alive"}))
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
    (StatusCode::OK, Json(json!({"message": collections})))
}

fn snapshot_collection(job: &ActiveJobSnapshot) -> Option<&str> {
    job.metadata.get("collection").and_then(Value::as_str)
}

fn snapshot_matches_user(job: &ActiveJobSnapshot, auth_user: Option<&AuthUser>) -> bool {
    let Some(auth_user) = auth_user else {
        // Anonymous v1 listing used to be empty; do not expose the shared
        // anonymous broker namespace through this compatibility endpoint.
        return false;
    };

    let Some(auth) = job.user.get("auth") else {
        return false;
    };

    auth.get("username").and_then(Value::as_str) == Some(auth_user.username.as_str())
        && auth.get("realm").and_then(Value::as_str) == Some(auth_user.realm.as_str())
}

fn legacy_request_record(job: &ActiveJobSnapshot) -> Value {
    let timestamp = job.created_at.timestamp_millis() as f64 / 1000.0;
    let collection = snapshot_collection(job).unwrap_or_default();
    let user = job.user.get("auth").cloned().unwrap_or(Value::Null);
    let mut status_history = Map::new();
    status_history.insert(job.status.to_string(), json!(timestamp));

    json!({
        "id": job.id,
        "timestamp": timestamp,
        "last_modified": timestamp,
        "user": user,
        "verb": "retrieve",
        "url": job.location.clone().unwrap_or_default(),
        "md5": Value::Null,
        "collection": collection,
        "status": job.status,
        "user_message": job.user_message.clone().unwrap_or_default(),
        "user_request": job.original_request.to_string(),
        "coerced_request": job.request,
        "content_length": job.content_length,
        "content_type": job
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        "status_history": Value::Object(status_history),
        "datasource": "",
    })
}

fn active_request_records(
    state: &AppState,
    auth_user: Option<&AuthUser>,
    collection: Option<&str>,
) -> Vec<Value> {
    let mut jobs: Vec<_> = state
        .bits
        .active_jobs()
        .into_iter()
        .filter(|job| snapshot_matches_user(job, auth_user))
        .filter(|job| collection.is_none_or(|name| snapshot_collection(job) == Some(name)))
        .collect();

    jobs.sort_by_key(|job| job.created_at);
    jobs.iter().map(legacy_request_record).collect()
}

pub async fn list_requests(
    State(state): State<Arc<AppState>>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    let records =
        active_request_records(&state, auth_user.as_ref().map(|Extension(user)| user), None);
    (StatusCode::OK, Json(json!({"message": records}))).into_response()
}

pub async fn user_info(
    State(state): State<Arc<AppState>>,
    auth_user: Option<Extension<AuthUser>>,
) -> impl IntoResponse {
    let live_requests =
        active_request_records(&state, auth_user.as_ref().map(|Extension(user)| user), None).len();
    Json(json!({"live requests": live_requests.to_string()}))
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

    // v1 only ever supported `retrieve` in practice (archive was never fleshed
    // out). Reject anything else with the legacy wording for parity.
    if body.verb != "retrieve" {
        tracing::warn!("event.name" = "api.job.rejected", outcome = "rejected", collection = %collection, verb = %body.verb, reason = "unsupported_verb", "job rejected");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Transfer type {} not supported", body.verb)})),
        )
            .into_response();
    }

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
    let location = format!("./{}", handle.id);
    (
        StatusCode::ACCEPTED,
        [
            (header::LOCATION, location),
            (header::RETRY_AFTER, RETRY_AFTER_SECS.to_string()),
        ],
        Json(request_queued_body()),
    )
        .into_response()
}

pub async fn get_request(
    State(state): State<Arc<AppState>>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
) -> Response {
    let auth_user_ref = auth_user.as_ref().map(|Extension(user)| user);

    if state.collections.contains_key(&id) {
        let records = active_request_records(&state, auth_user_ref, Some(&id));
        return (StatusCode::OK, Json(json!({"message": records}))).into_response();
    }

    if !super::known_active_job_allows_user(&state, &id, auth_user_ref) {
        tracing::warn!("event.name" = "api.job.poll.failed", outcome = "rejected", request.id = %id, reason = "wrong_user", "job poll rejected");
        return super::request_not_found_response();
    }

    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => {
            if let Some(Extension(user)) = auth_user.as_ref() {
                tracing::debug!("event.name" = "api.job.poll.pending", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job poll pending");
            } else {
                tracing::debug!("event.name" = "api.job.poll.pending", outcome = "success", request.id = %id, "job poll pending");
            }
            (
                StatusCode::ACCEPTED,
                [
                    (header::LOCATION, format!("./{id}")),
                    (header::RETRY_AFTER, RETRY_AFTER_SECS.to_string()),
                ],
                Json(request_queued_body()),
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
            JobResult::Redirect {
                location,
                content_type,
                content_length,
                ..
            } => {
                if let Some(Extension(user)) = auth_user.as_ref() {
                    tracing::info!("event.name" = "api.job.poll.completed", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job poll completed");
                } else {
                    tracing::info!("event.name" = "api.job.poll.completed", outcome = "success", request.id = %id, "job poll completed");
                }
                // Legacy v1 redirect body: `{contentLength, contentType, location}`
                // (message/status omitted, as the Python server did). The client
                // follows the Location header to download from BOBS/staging.
                let mut redirect_body = serde_json::Map::new();
                if let Some(content_length) = content_length {
                    redirect_body.insert("contentLength".to_string(), json!(content_length));
                }
                if let Some(content_type) = content_type {
                    redirect_body.insert("contentType".to_string(), json!(content_type));
                }
                redirect_body.insert("location".to_string(), json!(location));
                Response::builder()
                    .status(StatusCode::SEE_OTHER)
                    .header(header::LOCATION, &location)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&Value::Object(redirect_body)).unwrap_or_default(),
                    ))
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
) -> Response {
    let auth_user_ref = auth_user.as_ref().map(|Extension(user)| user);
    if !id.eq_ignore_ascii_case("all")
        && !super::known_active_job_allows_user(&state, &id, auth_user_ref)
    {
        tracing::warn!("event.name" = "api.job.cancelled", outcome = "rejected", request.id = %id, reason = "wrong_user", "job cancellation rejected");
        return super::request_not_found_response();
    }

    let revoked = if id.eq_ignore_ascii_case("all") {
        active_request_records(&state, auth_user_ref, None)
            .into_iter()
            .filter_map(|record| record.get("id").and_then(Value::as_str).map(str::to_string))
            .map(|id| usize::from(state.bits.cancel(&id)))
            .sum::<usize>()
    } else {
        usize::from(state.bits.cancel(&id))
    };
    if let Some(Extension(user)) = auth_user.as_ref() {
        tracing::info!("event.name" = "api.job.cancelled", outcome = "cancelled", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job cancelled");
    } else {
        tracing::info!("event.name" = "api.job.cancelled", outcome = "cancelled", request.id = %id, "job cancelled");
    }
    (
        StatusCode::OK,
        Json(json!({
            "message": format!("Successfully revoked {revoked} requests"),
        })),
    )
        .into_response()
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

pub async fn uploads_deprecated() -> impl IntoResponse {
    (
        StatusCode::GONE,
        [("Deprecation", "true")],
        Json(json!({"error": "uploads endpoint is not supported by this deployment"})),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    fn auth_user(username: &str, realm: &str) -> AuthUser {
        AuthUser {
            version: 1,
            username: username.to_string(),
            realm: realm.to_string(),
            roles: Vec::new(),
            attributes: HashMap::new(),
            scopes: HashMap::new(),
        }
    }

    fn snapshot() -> ActiveJobSnapshot {
        ActiveJobSnapshot {
            id: "req-1".to_string(),
            created_at: Utc::now(),
            original_request: json!({"class": "od", "param": "t"}),
            request: json!({"class": "od", "param": "t"}),
            user: json!({"auth": {"username": "alice", "realm": "ecmwf"}}),
            metadata: json!({"collection": "mars"}),
            status: "queued",
            location: None,
            content_type: None,
            content_length: None,
            user_message: None,
        }
    }

    #[test]
    fn active_request_filter_matches_effective_user() {
        let job = snapshot();
        assert!(snapshot_matches_user(
            &job,
            Some(&auth_user("alice", "ecmwf"))
        ));
        assert!(!snapshot_matches_user(
            &job,
            Some(&auth_user("bob", "ecmwf"))
        ));
        assert!(!snapshot_matches_user(
            &job,
            Some(&auth_user("alice", "desp"))
        ));
        assert!(!snapshot_matches_user(&job, None));
    }

    #[test]
    fn legacy_request_record_uses_python_v1_shape() {
        let mut job = snapshot();
        job.status = "processed";
        job.location = Some("https://example.test/result".to_string());
        job.content_type = Some("application/x-grib".to_string());
        job.content_length = Some(42);

        let record = legacy_request_record(&job);
        assert_eq!(record["id"], "req-1");
        assert_eq!(record["collection"], "mars");
        assert_eq!(record["status"], "processed");
        assert_eq!(record["verb"], "retrieve");
        assert_eq!(record["url"], "https://example.test/result");
        assert_eq!(record["content_type"], "application/x-grib");
        assert_eq!(record["content_length"], 42);
        assert!(record["status_history"].get("processed").is_some());
    }
}
