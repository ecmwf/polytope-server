// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Backwards-compatibility checks: the v1 API keeps parity with the legacy
//! Python polytope-server on the response shapes that clients depend on.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use polytope_server::build_app;
use polytope_server::config::ServerConfig;
use polytope_server::state::AppState;
use tower::ServiceExt;

fn app() -> axum::Router {
    // allow_anonymous lets the protected routes resolve without a live auth
    // backend, so we can exercise their response shapes directly.
    app_from_yaml(
        r#"
polytope:
  site: bol
  env: tst
bits: {}
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
  allow_anonymous: true
support:
  default_url: "https://support.ecmwf.int/"
"#,
    )
}

fn app_with_collection() -> axum::Router {
    app_from_yaml(
        r#"
polytope:
  site: bol
  env: tst
bits:
  targets:
    my_target:
      type: http
      url: "http://127.0.0.1:1/"
  collections:
    ecmwf:
      - my_route:
          - target::my_target
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
  allow_anonymous: true
support:
  default_url: "https://support.ecmwf.int/"
"#,
    )
}

/// Builds a collection backed by a TCP listener that accepts connections but
/// never sends a response, keeping jobs in "processing" state indefinitely.
/// Combined with a very short `v1_poll_timeout_ms`, this lets us exercise the
/// 202 Accepted pending path without a long wait.
fn app_with_stalled_collection() -> axum::Router {
    app_and_state_with_stalled_collection("").0
}

/// As [`app_with_stalled_collection`], but also hands back the app state so a
/// test can inspect the metadata of the job it submitted. `extra_config` is
/// appended to the YAML, for overriding sections such as `result_encoding`.
fn app_and_state_with_stalled_collection(extra_config: &str) -> (axum::Router, Arc<AppState>) {
    // Bind on an ephemeral port; accept connections but never respond.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        // Collect accepted streams into a vec so they are not dropped
        // (dropping a TcpStream sends TCP FIN, breaking the HTTP request).
        let mut open = Vec::new();
        while let Ok((stream, _)) = listener.accept() {
            open.push(stream);
        }
    });
    let cfg: ServerConfig = serde_yaml::from_str(&format!(
        r#"
polytope:
  site: bol
  env: tst
bits:
  targets:
    stall_target:
      type: http
      url: "http://{addr}/"
  collections:
    ecmwf:
      - my_route:
          - target::stall_target
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
  allow_anonymous: true
support:
  default_url: "https://support.ecmwf.int/"
server:
  v1_poll_timeout_ms: 50
{extra_config}"#
    ))
    .expect("config parses");
    build_app(cfg).expect("app builds")
}

/// Submits a v1 retrieve to the stalled collection and returns the metadata
/// the frontend attached to the queued job.
async fn submitted_job_metadata(
    extra_config: &str,
    user_agent: Option<&str>,
    accept_encoding: Option<&str>,
) -> serde_json::Value {
    let (app, state) = app_and_state_with_stalled_collection(extra_config);
    let body = serde_json::json!({"verb": "retrieve", "request": {"param": "t"}}).to_string();
    let mut request =
        Request::post("/api/v1/requests/ecmwf").header(header::CONTENT_TYPE, "application/json");
    if let Some(user_agent) = user_agent {
        request = request.header(header::USER_AGENT, user_agent);
    }
    if let Some(accept_encoding) = accept_encoding {
        request = request.header(header::ACCEPT_ENCODING, accept_encoding);
    }
    let resp = app
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let snapshot = state
        .bits
        .active_jobs()
        .into_iter()
        .next()
        .expect("the submitted job is still active against the stalled target");
    snapshot.metadata
}

fn app_from_yaml(yaml: &str) -> axum::Router {
    let cfg: ServerConfig = serde_yaml::from_str(yaml).expect("config parses");
    build_app(cfg).expect("app builds").0
}

#[tokio::test]
async fn test_endpoint_returns_json_message_like_python() {
    let resp = app()
        .oneshot(Request::get("/api/v1/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"message": "Polytope server is alive"})
    );
}

#[tokio::test]
async fn collections_are_wrapped_in_message_like_python() {
    let resp = app()
        .oneshot(
            Request::get("/api/v1/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let obj = v.as_object().expect("collections body is an object");
    // Python wraps the list under `message`, not `collections`.
    assert!(obj.contains_key("message"), "expected `message` key");
    assert!(
        !obj.contains_key("collections"),
        "must not use legacy Rust `collections` key"
    );
    assert!(obj["message"].is_array());
}

#[tokio::test]
async fn submit_pending_returns_202_accepted_matching_python() {
    // When the backend is reachable but slow, the inline poll times out and
    // the server returns 202 Accepted — matching the legacy Python frontend
    // behaviour that v1 clients depend on.
    let body = serde_json::json!({"verb": "retrieve", "request": {"param": "t"}}).to_string();
    let resp = app_with_stalled_collection()
        .oneshot(
            Request::post("/api/v1/requests/ecmwf")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Python returns {"message": "Request queued", "status": "queued"}
    assert!(
        v.get("status").is_some(),
        "202 body must carry a status field"
    );
    assert!(
        v.get("message").is_some(),
        "202 body must carry a message field"
    );
}

#[tokio::test]
async fn submit_polls_inline_and_surfaces_errors_immediately() {
    // The POST endpoint now polls inline (matching v2): failures reach the
    // client on the submit response instead of being deferred to a subsequent
    // GET poll.  The target at 127.0.0.1:1 is unreachable, so the job fails
    // quickly and the error surfaces directly — not as 202 Accepted.
    let body = serde_json::json!({"verb": "retrieve", "request": {"param": "t"}}).to_string();
    let resp = app_with_collection()
        .oneshot(
            Request::post("/api/v1/requests/ecmwf")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        resp.status(),
        StatusCode::ACCEPTED,
        "submit must not unconditionally return 202; failures must surface immediately"
    );
    // The error must surface as a failed/errored result, not a success.
    assert!(
        resp.status().is_server_error() || resp.status().is_client_error(),
        "expected an error status, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn delete_request_body_matches_python_revoke_shape() {
    let resp = app()
        .oneshot(
            Request::delete("/api/v1/requests/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"message": "Successfully revoked 0 requests"})
    );
}

#[tokio::test]
async fn delete_all_keyword_matches_python_revoke_shape() {
    let resp = app()
        .oneshot(
            Request::delete("/api/v1/requests/ALL")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"message": "Successfully revoked 0 requests"})
    );
}

#[tokio::test]
async fn uploads_endpoint_is_explicitly_gone() {
    let resp = app()
        .oneshot(
            Request::get("/api/v1/uploads/abc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::GONE);
    assert_eq!(resp.headers().get("deprecation").unwrap(), "true");
}

#[tokio::test]
async fn security_and_cache_headers_present_on_every_response() {
    let resp = app()
        .oneshot(Request::get("/api/v1/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    let h = resp.headers();
    assert_eq!(h.get(header::CACHE_CONTROL).unwrap(), "no-cache, no-store");
    assert_eq!(h.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(h.get("x-frame-options").unwrap(), "DENY");
    assert_eq!(h.get("x-xss-protection").unwrap(), "1; mode=block");
}

#[tokio::test]
async fn v1_submit_always_asks_for_a_buffered_result() {
    // v1 serves the result with a fixed Content-Length, encoded or not.
    let metadata = submitted_job_metadata("", Some("curl/8.5.0"), Some("zstd, gzip")).await;
    assert_eq!(metadata["buffer_full_output"], true);
}

#[tokio::test]
async fn v1_forwards_accept_encoding_for_clients_that_decode_it() {
    let metadata = submitted_job_metadata(
        "",
        Some("polytope-client/0.7.7 python-requests/2.34.2"),
        Some("zstd, gzip"),
    )
    .await;
    assert_eq!(metadata["accept_encoding"], "zstd, gzip");

    let metadata = submitted_job_metadata("", Some("curl/8.5.0"), Some("gzip")).await;
    assert_eq!(metadata["accept_encoding"], "gzip");

    let metadata = submitted_job_metadata("", None, Some("gzip")).await;
    assert_eq!(metadata["accept_encoding"], "gzip");
}

#[tokio::test]
async fn v1_withholds_accept_encoding_from_pre_0_7_7_polytope_clients() {
    // polytope-client below 0.7.7 counted decoded bytes against the compressed
    // Content-Length and failed every encoded download.
    for user_agent in [
        "polytope-client/0.7.6 python-requests/2.34.2",
        "python-requests/2.34.2",
    ] {
        let metadata = submitted_job_metadata("", Some(user_agent), Some("gzip, deflate")).await;
        assert!(
            metadata.get("accept_encoding").is_none(),
            "'{user_agent}' must not be offered an encoded result, got {metadata}"
        );
    }
}

#[tokio::test]
async fn v1_accept_encoding_forwarding_can_be_disabled_by_config() {
    let metadata = submitted_job_metadata(
        "result_encoding:\n  enabled: false\n",
        Some("polytope-client/0.7.7 python-requests/2.34.2"),
        Some("zstd, gzip"),
    )
    .await;
    assert!(metadata.get("accept_encoding").is_none(), "{metadata}");
}

#[tokio::test]
async fn v1_accept_encoding_threshold_is_configurable() {
    let metadata = submitted_job_metadata(
        "result_encoding:\n  min_polytope_client_version: \"0.9.0\"\n",
        Some("polytope-client/0.8.0 python-requests/2.34.2"),
        Some("zstd, gzip"),
    )
    .await;
    assert!(metadata.get("accept_encoding").is_none(), "{metadata}");

    let metadata = submitted_job_metadata(
        "result_encoding:\n  min_polytope_client_version: \"0.9.0\"\n",
        Some("polytope-client/0.9.1 python-requests/2.34.2"),
        Some("zstd, gzip"),
    )
    .await;
    assert_eq!(metadata["accept_encoding"], "zstd, gzip");
}
