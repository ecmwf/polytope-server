// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use bits::{Job, JobResult, PollOutcome, SubmitOutcome};
use serde_json::{Value, json};

use crate::auth::{AuthUser, MockRolesAudit};
use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const PENDING_STATUS_HEADER: &str = "x-bits-pending-status";

fn local_pending_status(state: &AppState, id: &str) -> &'static str {
    state
        .bits
        .active_jobs()
        .into_iter()
        .find(|job| job.id == id)
        .map(|job| job.status)
        .unwrap_or("queued")
}

fn pending_redirect(id: &str, status: &str) -> Response {
    let location = format!("/api/v2/requests/{id}");
    let body = json!({
        "id": id,
        "location": location,
        "status": status,
    });

    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, &location)
        .header(PENDING_STATUS_HEADER, status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap_or_default()))
        .unwrap()
}

pub async fn health() -> &'static str {
    "Polytope server is alive"
}

pub async fn list_collections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut names: Vec<String> = state.collections.keys().cloned().collect();
    names.sort();
    tracing::info!(
        "event.name" = "api.collection.list",
        outcome = "success",
        collection_count = names.len() as u64,
        api.version = "v2",
        "listed collections"
    );
    (StatusCode::OK, Json(json!({"collections": names})))
}

pub async fn submit_collection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    auth_user: Option<Extension<AuthUser>>,
    mock_audit: Option<Extension<MockRolesAudit>>,
    mock_time_extensions: super::MockTimeSubmissionExtensions,
    Path(collection): Path<String>,
    Json(mut body): Json<Value>,
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

    if let Err(msg) = super::flatten_request(&mut body) {
        tracing::warn!("event.name" = "api.job.rejected", outcome = "rejected", reason = "invalid_request", error = %msg, "job rejected");
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response();
    }

    let mut job = Job::new(body);
    super::set_job_user_context(
        &mut job,
        &headers,
        auth_user.as_ref().map(|Extension(user)| user),
        mock_audit.as_ref().map(|Extension(audit)| audit),
        &state.admin_bypass_roles,
    );
    // Propagate Accept-Encoding so workers can choose an encoding codec
    if let Some(enc) = headers
        .get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        job.metadata_mut()["accept_encoding"] = serde_json::json!(enc);
    }
    job.metadata_mut()["collection"] = serde_json::json!(&collection);
    super::set_job_mock_time_metadata(&mut job, mock_time_extensions.mock_time.as_ref());
    tracing::debug!(
        x_forwarded_for_present = headers.get("x-forwarded-for").is_some(),
        x_real_ip_present = headers.get("x-real-ip").is_some(),
        x_proxy_protocol_addr_present = headers.get("x-proxy-protocol-addr").is_some(),
        "client IP candidate headers present"
    );
    let submitted_request = job.request.clone();
    let id = match route_handle.submit(job) {
        SubmitOutcome::Accepted(handle) => handle.id,
        SubmitOutcome::Overloaded => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "broker at capacity"})),
            )
                .into_response();
        }
    };
    if let Some(Extension(user)) = auth_user.as_ref() {
        tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, polytope.request = %polytope_observability::request(&submitted_request), "job submitted");
    } else {
        tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %id, polytope.request = %polytope_observability::request(&submitted_request), "job submitted");
    }
    super::audit_mock_job_submission(mock_audit.as_ref().map(|Extension(audit)| audit), &id);
    super::audit_mock_time_job_submission(mock_time_extensions.mock_time_audit.as_ref(), &id);

    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id, .. } => {
            let status = local_pending_status(&state, &id);
            pending_redirect(&id, status)
        }
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
            JobResult::Redirect {
                location,
                message,
                content_type,
                content_length,
            } => {
                let mut builder = Response::builder()
                    .status(StatusCode::SEE_OTHER)
                    .header(header::LOCATION, location);
                // Carry content metadata so a proxying broker can rebuild the v1
                // redirect body without an extra round-trip (see
                // bits::runtime::recovery::try_proxy_with_lease).
                if let Some(content_type) = content_type {
                    builder = builder.header("x-polytope-content-type", content_type);
                }
                if let Some(content_length) = content_length {
                    builder =
                        builder.header("x-polytope-content-length", content_length.to_string());
                }
                builder.body(Body::from(message)).unwrap()
            }
            JobResult::Error { message } => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
            JobResult::Failed { reason } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": reason})),
            )
                .into_response(),
            JobResult::Overloaded { reason } => {
                super::overloaded_response(json!({"error": reason, "retryable": true}))
            }
            JobResult::RateLimited { reason } => {
                super::rate_limited_response(json!({"error": reason, "retryable": true}))
            }
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

pub async fn public_poll(
    State(state): State<Arc<AppState>>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
) -> Response {
    let auth_user_ref = auth_user.as_ref().map(|Extension(user)| user);
    if !super::known_active_job_allows_user(&state, &id, auth_user_ref) {
        tracing::warn!("event.name" = "api.job.poll.failed", outcome = "rejected", request.id = %id, reason = "wrong_user", "job poll rejected");
        return super::request_not_found_response();
    }

    poll(State(state), Path(id)).await
}

pub async fn poll(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id, .. } => {
            let status = local_pending_status(&state, &id);
            pending_redirect(&id, status)
        }
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
            JobResult::Redirect {
                location,
                message,
                content_type,
                content_length,
            } => {
                let mut builder = Response::builder()
                    .status(StatusCode::SEE_OTHER)
                    .header(header::LOCATION, location);
                // Carry content metadata so a proxying broker can rebuild the v1
                // redirect body without an extra round-trip (see
                // bits::runtime::recovery::try_proxy_with_lease).
                if let Some(content_type) = content_type {
                    builder = builder.header("x-polytope-content-type", content_type);
                }
                if let Some(content_length) = content_length {
                    builder =
                        builder.header("x-polytope-content-length", content_length.to_string());
                }
                builder.body(Body::from(message)).unwrap()
            }
            JobResult::Error { message } => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
            JobResult::Failed { reason } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": reason})),
            )
                .into_response(),
            JobResult::Overloaded { reason } => {
                super::overloaded_response(json!({"error": reason, "retryable": true}))
            }
            JobResult::RateLimited { reason } => {
                super::rate_limited_response(json!({"error": reason, "retryable": true}))
            }
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

pub async fn public_cancel(
    State(state): State<Arc<AppState>>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
) -> Response {
    let auth_user_ref = auth_user.as_ref().map(|Extension(user)| user);
    if !super::known_active_job_allows_user(&state, &id, auth_user_ref) {
        tracing::warn!("event.name" = "api.job.cancelled", outcome = "rejected", request.id = %id, reason = "wrong_user", "job cancellation rejected");
        return super::request_not_found_response();
    }

    if !state.bits.cancel(&id) {
        return super::request_not_found_response();
    }

    if let Some(Extension(user)) = auth_user.as_ref() {
        tracing::info!("event.name" = "api.job.cancelled", outcome = "cancelled", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, "job cancelled");
    } else {
        tracing::info!("event.name" = "api.job.cancelled", outcome = "cancelled", request.id = %id, "job cancelled");
    }
    (
        StatusCode::OK,
        Json(json!({"id": id, "status": "cancelled"})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
    };
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::state::AppState;

    fn make_bits_with_route(route_name: &str) -> (bits::Bits, bits::RouteHandle) {
        let yaml = r#"bits:
  site: tst
  env: tst
targets:
  t:
    type: http
    url: "http://127.0.0.1:0/""#;
        let bits = bits::Bits::from_config(yaml).unwrap();
        let route_value = serde_json::json!([{"test_route": ["target::t"]}]);
        let handle = bits.add_route(route_name, &route_value).unwrap();
        (bits, handle)
    }

    fn build_v2_app(bits: bits::Bits, collections: HashMap<String, bits::RouteHandle>) -> Router {
        let state = Arc::new(AppState {
            bits,
            auth_client: None,
            collections,
            allow_anonymous: false,
            admin_bypass_roles: None,
            support: Default::default(),
            completed_redirects: std::sync::Mutex::new(std::collections::HashMap::new()),
        });
        Router::new()
            .route("/api/v2/collections", get(super::list_collections))
            .route(
                "/api/v2/{collection}/requests",
                post(super::submit_collection),
            )
            .route(
                "/api/v2/requests/{id}",
                get(super::public_poll).delete(super::public_cancel),
            )
            .with_state(state)
    }

    fn auth_user(username: &str, realm: &str) -> crate::auth::AuthUser {
        crate::auth::AuthUser {
            version: 1,
            username: username.to_string(),
            realm: realm.to_string(),
            roles: Vec::new(),
            attributes: HashMap::new(),
            scopes: HashMap::new(),
        }
    }

    fn test_bits() -> bits::Bits {
        bits::Bits::from_router_for_tests(
            bits::routing::switch::Switch::new(vec![]),
            "test".to_string(),
            "http://localhost:0".to_string(),
            std::time::Duration::from_secs(1),
            None,
            None,
            std::time::Duration::from_secs(30),
        )
    }

    #[tokio::test]
    async fn list_collections_returns_sorted_names() {
        let (bits, handle_a) = make_bits_with_route("ecmwf");
        let handle_b = bits
            .add_route("opendata", &serde_json::json!([{"r": ["target::t"]}]))
            .unwrap();
        let mut collections = HashMap::new();
        collections.insert("ecmwf".to_string(), handle_a);
        collections.insert("opendata".to_string(), handle_b);
        let app = build_v2_app(bits, collections);

        let resp = app
            .oneshot(
                Request::get("/api/v2/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let names: Vec<&str> = json["collections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(names.contains(&"ecmwf"), "ecmwf missing from {names:?}");
        assert!(
            names.contains(&"opendata"),
            "opendata missing from {names:?}"
        );
    }

    #[tokio::test]
    async fn list_collections_empty_when_none_configured() {
        let bits = bits::Bits::from_router_for_tests(
            bits::routing::switch::Switch::new(vec![]),
            "test".to_string(),
            "http://localhost:0".to_string(),
            std::time::Duration::from_secs(1),
            None,
            None,
            std::time::Duration::from_secs(30),
        );
        let app = build_v2_app(bits, HashMap::new());

        let resp = app
            .oneshot(
                Request::get("/api/v2/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["collections"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn submit_unknown_collection_returns_404() {
        let bits = bits::Bits::from_router_for_tests(
            bits::routing::switch::Switch::new(vec![]),
            "test".to_string(),
            "http://localhost:0".to_string(),
            std::time::Duration::from_secs(1),
            None,
            None,
            std::time::Duration::from_secs(30),
        );
        let app = build_v2_app(bits, HashMap::new());

        let resp = app
            .oneshot(
                Request::post("/api/v2/nonexistent/requests")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "unknown collection 'nonexistent'");
    }

    #[tokio::test]
    async fn submit_known_collection_routes_to_handle() {
        let (bits, handle) = make_bits_with_route("ecmwf");
        let mut collections = HashMap::new();
        collections.insert("ecmwf".to_string(), handle);
        let app = build_v2_app(bits, collections);

        let resp = app
            .oneshot(
                Request::post("/api/v2/ecmwf/requests")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        let error_msg = json.get("error").and_then(|e| e.as_str()).unwrap_or("");
        assert_ne!(
            error_msg, "unknown collection 'ecmwf'",
            "collection 'ecmwf' should be found; got status {status}"
        );
    }

    #[tokio::test]
    async fn cancel_rejects_active_job_owned_by_another_user() {
        let bits = test_bits();
        let mut job = bits::Job::new(serde_json::json!({"class": "od"}));
        job.user = serde_json::json!({"auth": auth_user("alice", "ecmwf")}).into();
        let id = bits
            .submit(job)
            .expect_accepted("test broker should accept the request")
            .id;
        let app = build_v2_app(bits, HashMap::new());

        let mut bob_request = Request::builder()
            .method("DELETE")
            .uri(format!("/api/v2/requests/{id}"))
            .body(Body::empty())
            .unwrap();
        bob_request
            .extensions_mut()
            .insert(auth_user("bob", "ecmwf"));
        let bob_resp = app.clone().oneshot(bob_request).await.unwrap();
        assert_eq!(bob_resp.status(), StatusCode::NOT_FOUND);

        let mut alice_request = Request::builder()
            .method("DELETE")
            .uri(format!("/api/v2/requests/{id}"))
            .body(Body::empty())
            .unwrap();
        alice_request
            .extensions_mut()
            .insert(auth_user("alice", "ecmwf"));
        let alice_resp = app.oneshot(alice_request).await.unwrap();
        assert_eq!(alice_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pending_redirect_has_json_body() {
        let resp = super::pending_redirect("request-id", "processing");

        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            "/api/v2/requests/request-id"
        );
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/json"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], "request-id");
        assert_eq!(json["location"], "/api/v2/requests/request-id");
        assert_eq!(json["status"], "processing");
    }

    #[tokio::test]
    async fn old_submit_endpoint_is_gone() {
        let bits = bits::Bits::from_router_for_tests(
            bits::routing::switch::Switch::new(vec![]),
            "test".to_string(),
            "http://localhost:0".to_string(),
            std::time::Duration::from_secs(1),
            None,
            None,
            std::time::Duration::from_secs(30),
        );
        let app = build_v2_app(bits, HashMap::new());

        let resp = app
            .oneshot(
                Request::post("/api/v2/requests")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
