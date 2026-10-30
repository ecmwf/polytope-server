// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! The request ID quoted in an error is the one carried by that call's logs,
//! and is the job's ID when the call submitted one.
//!
//! Kept in its own test binary: in the lib tests, other tests hit the same
//! callsites with no subscriber installed, and tracing's process-wide callsite
//! interest cache then made the scoped capture miss events.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use http_body_util::BodyExt;
use polytope_observability::test_helper::capturing_subscriber;
use polytope_server::state::AppState;
use serde_json::Value;
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

async fn quoted_request_id(resp: axum::response::Response) -> String {
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    let msg = v["message"].as_str().unwrap();
    let id = msg
        .split("quote your request ID ")
        .nth(1)
        .and_then(|rest| rest.split('.').next())
        .unwrap_or_else(|| panic!("no request ID quoted in: {msg}"));
    bits::request_id::decode(id).expect("quoted ID is a BITS request ID");
    id.to_string()
}

fn event(lines: &[Value], name: &str) -> Value {
    lines
        .iter()
        .find(|l| l["attributes"]["event.name"] == name)
        .unwrap_or_else(|| panic!("no {name} event in {lines:?}"))
        .clone()
}

fn rejecting_app() -> axum::Router {
    let yaml = r#"
polytope:
  site: bol
  env: tst
bits: {}
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
"#;
    polytope_server::build_app(serde_yaml::from_str(yaml).unwrap())
        .unwrap()
        .0
}

fn failing_submit_app() -> axum::Router {
    failing_submit_router(
        axum::Router::new()
            .route(
                "/api/v2/{collection}/requests",
                post(polytope_server::api::v2::submit_collection),
            )
            .route(
                "/api/v1/requests/{collection}",
                post(polytope_server::api::v1::submit_request),
            ),
    )
}

fn failing_submit_router(routes: axum::Router<Arc<AppState>>) -> axum::Router {
    let yaml = r#"bits:
  site: tst
  env: tst
targets:
  t:
    type: http
    url: "http://127.0.0.1:0/""#;
    let bits = bits::Bits::from_config(yaml).unwrap();
    let handle = bits
        .add_route("ecmwf", &serde_json::json!([{"r": ["target::t"]}]))
        .unwrap();
    let state = Arc::new(AppState {
        bits,
        auth_client: None,
        collections: HashMap::from([("ecmwf".to_string(), handle)]),
        allow_anonymous: true,
        admin_bypass_roles: None,
        support: Default::default(),
        completed_redirects: std::sync::Mutex::new(HashMap::new()),
        completed_redirect_ttl: Duration::from_secs(600),
        v1_poll_timeout: Duration::from_secs(30),
        v2_poll_timeout: Duration::from_secs(30),
    });
    routes
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state,
            polytope_server::support::request_context_middleware,
        ))
}

fn submit() -> Request<Body> {
    Request::post("/api/v2/ecmwf/requests")
        .header("Content-Type", "application/json")
        .body(Body::from("{}"))
        .unwrap()
}

fn capture() -> (
    polytope_observability::test_helper::CapturedLogs,
    tracing::subscriber::DefaultGuard,
) {
    let (layer, logs) = capturing_subscriber("polytope-frontend");
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (logs, guard)
}

#[tokio::test]
async fn auth_rejection_is_logged_with_the_quoted_id() {
    let (logs, _guard) = capture();
    let resp = rejecting_app().oneshot(submit()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let id = quoted_request_id(resp).await;
    let rejected = event(&logs.json_lines(), "api.auth.rejected");
    assert_eq!(rejected["attributes"]["request.id"], id.as_str());
    assert_eq!(rejected["attributes"]["http.request.method"], "POST");
    assert_eq!(rejected["attributes"]["url.path"], "/api/v2/ecmwf/requests");
}

#[tokio::test]
async fn failed_v2_submit_quotes_the_job_id() {
    let (logs, _guard) = capture();
    let resp = failing_submit_app().oneshot(submit()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let id = quoted_request_id(resp).await;
    let submitted = event(&logs.json_lines(), "api.job.submitted");
    assert_eq!(submitted["attributes"]["request.id"], id.as_str());
}

#[tokio::test]
async fn failed_v1_submit_quotes_the_job_id() {
    let (logs, _guard) = capture();
    let resp = failing_submit_app()
        .oneshot(
            Request::post("/api/v1/requests/ecmwf")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"verb": "retrieve", "request": {}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status().is_server_error(), "{}", resp.status());
    let id = quoted_request_id(resp).await;
    let submitted = event(&logs.json_lines(), "api.job.submitted");
    assert_eq!(submitted["attributes"]["request.id"], id.as_str());
}
