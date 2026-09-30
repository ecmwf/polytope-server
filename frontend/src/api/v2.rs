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
use bits::{Job, JobResult, PollOutcome, SubmitOutcome};
use serde_json::{Value, json};

use crate::auth::{AuthUser, MockRolesAudit};
use crate::state::AppState;

const PENDING_STATUS_HEADER: &str = "x-bits-pending-status";
const MIN_POLL_REHOLD: Duration = Duration::from_millis(5);

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

    submit_and_poll(
        &state,
        Submission {
            collection: &collection,
            route_handle,
            headers: &headers,
            auth_user: auth_user.as_ref().map(|Extension(user)| user),
            mock_audit: mock_audit.as_ref().map(|Extension(audit)| audit),
            mock_time: &mock_time_extensions,
            api: None,
        },
        body,
    )
    .await
}

/// Everything needed to submit an already-validated, flat job body to a
/// collection's bits route with the v2 submission semantics.
pub(crate) struct Submission<'a> {
    pub collection: &'a str,
    pub route_handle: bits::RouteHandle,
    pub headers: &'a HeaderMap,
    pub auth_user: Option<&'a AuthUser>,
    pub mock_audit: Option<&'a MockRolesAudit>,
    pub mock_time: &'a super::MockTimeSubmissionExtensions,
    /// Optional trusted `metadata.api` tag (e.g. `"chunks"`); v2 leaves it unset.
    pub api: Option<&'static str>,
}

/// Build a job from `body`, attach user context and trusted metadata, submit
/// it to the route and inline-poll it, returning the v2 response (200 inline
/// bytes | 303 pending redirect to `/api/v2/requests/{id}` | 303 result
/// redirect | error status).
///
/// Shared by v2 `submit_collection` and `/chunks/v1/{collection}/extract` so
/// both APIs have identical submission and poll semantics.
pub(crate) async fn submit_and_poll(
    state: &Arc<AppState>,
    submission: Submission<'_>,
    body: Value,
) -> Response {
    let Submission {
        collection,
        route_handle,
        headers,
        auth_user,
        mock_audit,
        mock_time: mock_time_extensions,
        api,
    } = submission;

    let mut job = Job::new(body);
    super::set_job_user_context(
        &mut job,
        headers,
        auth_user,
        mock_audit,
        &state.admin_bypass_roles,
    );
    // Propagate Accept-Encoding so workers can choose an encoding codec
    if let Some(enc) = headers
        .get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        job.metadata_mut()["accept_encoding"] = serde_json::json!(enc);
    }
    job.metadata_mut()["collection"] = serde_json::json!(collection);
    if let Some(api) = api {
        job.metadata_mut()["api"] = serde_json::json!(api);
        if api == "chunks" {
            // A terminal poll consumes the BITS result. Keep chunk payloads in BOBS
            // so the terminal response is a repeatable redirect, rather than a
            // one-shot inline body that becomes a 404 when a client retries the GET.
            job.metadata_mut()["buffer_full_output"] = serde_json::json!(true);
        }
    }
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
    if let Some(user) = auth_user {
        tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, polytope.request = %polytope_observability::request(&submitted_request), "job submitted");
    } else {
        tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %id, polytope.request = %polytope_observability::request(&submitted_request), "job submitted");
    }
    super::audit_mock_job_submission(mock_audit, &id);
    super::audit_mock_time_job_submission(mock_time_extensions.mock_time_audit.as_ref(), &id);

    let timeout = state.v2_poll_timeout;
    let mut response = poll_job_v2(state, id.clone(), timeout).await;
    // Surface the BITS-generated request ID so the outer middleware can quote it
    // in error responses (helps correlate with logs).
    response
        .extensions_mut()
        .insert(crate::support::RequestId(id));
    response
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

    let timeout = state.v2_poll_timeout;
    poll_job_v2(&state, id, timeout).await
}

fn ready_status(result: &JobResult) -> &'static str {
    match result {
        JobResult::Success { .. } => "success",
        JobResult::Redirect { .. } => "redirect",
        JobResult::Error { .. } => "error",
        JobResult::Failed { .. } => "failed",
        JobResult::Overloaded { .. } => "overloaded",
        JobResult::RateLimited { .. } => "rate_limited",
        JobResult::ClientGone => "client_gone",
        JobResult::Cancelled => "cancelled",
    }
}

/// Poll a job and convert the outcome to a v2 HTTP response.
///
/// Shared by `submit_collection` (inline poll on submit), and `public_poll`
/// (user-facing long-poll endpoint). Non-terminal wakes are re-held until the
/// original timeout budget expires. The internal poll endpoint deliberately
/// retains its single-wake lifecycle via `poll_job_v2_once`.
async fn poll_job_v2(state: &Arc<AppState>, id: String, timeout: Duration) -> Response {
    poll_job_v2_impl(state, id, timeout, true).await
}

async fn poll_job_v2_once(state: &Arc<AppState>, id: String, timeout: Duration) -> Response {
    poll_job_v2_impl(state, id, timeout, false).await
}

async fn poll_job_v2_impl(
    state: &Arc<AppState>,
    id: String,
    timeout: Duration,
    rehold_pending: bool,
) -> Response {
    let poll_started = Instant::now();
    let deadline = poll_started + timeout;
    let outcome = loop {
        let call_started = Instant::now();
        let remaining = deadline.saturating_duration_since(call_started);
        let outcome = state.bits.poll(&id, Some(remaining)).await;

        if !rehold_pending || !matches!(outcome, PollOutcome::Pending { .. }) {
            break outcome;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break outcome;
        }

        // A remote/internal poll can report the same pending state immediately.
        // Pace that path so re-holding cannot spin while preserving the deadline.
        let call_elapsed = call_started.elapsed();
        if call_elapsed < MIN_POLL_REHOLD {
            tokio::time::sleep(remaining.min(MIN_POLL_REHOLD.saturating_sub(call_elapsed))).await;
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                break outcome;
            }
        }
    };

    match outcome {
        PollOutcome::Pending { id, .. } => {
            let status = local_pending_status(state, &id);
            tracing::info!(
                "event.name" = "api.job.poll",
                outcome = "pending",
                request.id = %id,
                job.status = status,
                wait_ms = poll_started.elapsed().as_millis() as u64,
                "job poll completed"
            );
            pending_redirect(&id, status)
        }
        PollOutcome::NotFound => {
            tracing::info!(
                "event.name" = "api.job.poll",
                outcome = "not_found",
                request.id = %id,
                wait_ms = poll_started.elapsed().as_millis() as u64,
                "job poll completed"
            );
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        PollOutcome::JobLost => {
            tracing::info!(
                "event.name" = "api.job.poll",
                outcome = "job_lost",
                request.id = %id,
                wait_ms = poll_started.elapsed().as_millis() as u64,
                "job poll completed"
            );
            (
                StatusCode::GONE,
                Json(json!({"error": "request state expired or was lost"})),
            )
                .into_response()
        }
        PollOutcome::Ready(result) => {
            tracing::info!(
                "event.name" = "api.job.poll",
                outcome = "ready",
                request.id = %id,
                job.status = ready_status(&result),
                wait_ms = poll_started.elapsed().as_millis() as u64,
                "job poll completed"
            );
            match result {
                JobResult::Success {
                    content_type,
                    size,
                    stream,
                } => {
                    let disposition = super::download::content_disposition_for(&id, &content_type);
                    let mut builder = Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, content_type)
                        .header(header::CONTENT_DISPOSITION, disposition);
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
            }
        }
    }
}

pub async fn poll(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let timeout = state.v2_poll_timeout;
    poll_job_v2_once(&state, id, timeout).await
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
    use std::time::Duration;

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
        build_v2_app_with_timeout(bits, collections, Duration::from_secs(30))
    }

    fn build_v2_app_with_timeout(
        bits: bits::Bits,
        collections: HashMap<String, bits::RouteHandle>,
        v2_poll_timeout: Duration,
    ) -> Router {
        let state = Arc::new(AppState {
            bits,
            auth_client: None,
            collections,
            allow_anonymous: false,
            admin_bypass_roles: None,
            support: Default::default(),
            completed_redirects: std::sync::Mutex::new(std::collections::HashMap::new()),
            completed_redirect_ttl: std::time::Duration::from_secs(600),
            v1_poll_timeout: Duration::from_secs(30),
            v2_poll_timeout,
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

    async fn make_remote_bits_with_route(route_name: &str) -> (bits::Bits, bits::RouteHandle, u16) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let yaml = format!(
            r#"bits:
  site: tst
  env: tst
  worker_server:
    host: "127.0.0.1"
    port: {port}
targets:
  test_pool:
    type: remote
    dispatcher:
      executor:
        type: remote_pool
        heartbeat_timeout_secs: 60
routes:
  - unused:
      - target::test_pool
"#
        );
        let bits = bits::Bits::from_config(&yaml).unwrap();
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if client
                    .get(format!(
                        "http://127.0.0.1:{port}/test_pool/work?timeout_ms=0"
                    ))
                    .send()
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("remote worker test server did not become ready");
        let route_value = serde_json::json!([{"test_route": ["target::test_pool"]}]);
        let handle = bits.add_route(route_name, &route_value).unwrap();
        (bits, handle, port)
    }

    async fn claim_remote_job(port: u16) -> String {
        let response = reqwest::Client::new()
            .get(format!(
                "http://127.0.0.1:{port}/test_pool/work?timeout_ms=5000"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let work: Value = response.json().await.unwrap();
        work["job_id"].as_str().unwrap().to_string()
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
    async fn post_accepted_async_target_returns_poll_redirect_with_assigned_id() {
        // The target now executes asynchronously after BITS accepts the job. Until
        // a worker publishes its delivery redirect, return the v2 poll URL and keep
        // the generated request ID attached for correlation.
        let (bits, handle) = make_bits_with_route("ecmwf");
        let mut collections = HashMap::new();
        collections.insert("ecmwf".to_string(), handle);
        let state = Arc::new(AppState {
            bits,
            auth_client: None,
            collections,
            allow_anonymous: false,
            admin_bypass_roles: None,
            support: Default::default(),
            completed_redirects: std::sync::Mutex::new(std::collections::HashMap::new()),
            completed_redirect_ttl: std::time::Duration::from_secs(600),
            v1_poll_timeout: Duration::from_secs(30),
            v2_poll_timeout: Duration::from_secs(30),
        });
        let app = Router::new()
            .route(
                "/api/v2/{collection}/requests",
                post(super::submit_collection),
            )
            .with_state(state.clone())
            .layer(axum::middleware::from_fn_with_state(
                state,
                crate::support::request_context_middleware,
            ));

        let resp = app
            .oneshot(
                Request::post("/api/v2/ecmwf/requests")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SEE_OTHER);

        let id = resp
            .extensions()
            .get::<crate::support::RequestId>()
            .expect("assigned request ID present on the response")
            .0
            .clone();
        assert!(!id.is_empty());

        let location = resp
            .headers()
            .get(axum::http::header::LOCATION)
            .expect("pending response has a poll location")
            .to_str()
            .unwrap();
        assert_eq!(location, format!("/api/v2/requests/{id}"));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_reholds_processing_job_until_terminal_result() {
        let (bits, handle, port) = make_remote_bits_with_route("ecmwf").await;
        let mut collections = HashMap::new();
        collections.insert("ecmwf".to_string(), handle);
        let app = build_v2_app_with_timeout(bits, collections, Duration::from_millis(500));

        let worker = tokio::spawn(async move {
            let id = claim_remote_job(port).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            let response = reqwest::Client::new()
                .post(format!(
                    "http://127.0.0.1:{port}/test_pool/complete/data/{id}"
                ))
                .header("content-type", "application/octet-stream")
                .body("terminal-bytes")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
        });

        let response = app
            .oneshot(
                Request::post("/api/v2/ecmwf/requests")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        worker.await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !response
                .headers()
                .contains_key(super::PENDING_STATUS_HEADER)
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"terminal-bytes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn processing_job_returns_pending_only_after_window_expires() {
        let (bits, handle, port) = make_remote_bits_with_route("ecmwf").await;
        let mut collections = HashMap::new();
        collections.insert("ecmwf".to_string(), handle);
        let app = build_v2_app_with_timeout(bits, collections, Duration::from_secs(5));

        let claim = tokio::spawn(async move { claim_remote_job(port).await });
        let started = std::time::Instant::now();
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v2/ecmwf/requests")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let id = claim.await.unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get(super::PENDING_STATUS_HEADER)
                .unwrap(),
            "processing"
        );
        assert!(
            elapsed >= Duration::from_millis(4_500),
            "pending response returned before its window: {elapsed:?}"
        );

        // The same re-hold policy applies to the public GET poll handler.
        let location = response.headers()[axum::http::header::LOCATION]
            .to_str()
            .unwrap()
            .to_string();
        let completion = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            reqwest::Client::new()
                .post(format!(
                    "http://127.0.0.1:{port}/test_pool/complete/data/{id}"
                ))
                .header("content-type", "application/octet-stream")
                .body("polled-terminal-bytes")
                .send()
                .await
                .unwrap()
        });
        let poll_response = app
            .oneshot(Request::get(location).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(completion.await.unwrap().status(), reqwest::StatusCode::OK);
        assert_eq!(poll_response.status(), StatusCode::OK);
        let body = poll_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(&body[..], b"polled-terminal-bytes");
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
