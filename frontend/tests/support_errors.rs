// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end checks that user-facing errors carry support guidance + a request
//! ID, and that the error body stays a flat string→string object (so the Python
//! `polytope-client`, which flattens every value and crashes on non-strings,
//! renders it cleanly).

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use polytope_server::build_app;
use polytope_server::config::{ServerConfig, SupportConfig};
use tower::ServiceExt;

fn app() -> axum::Router {
    let yaml = r#"
polytope:
  site: bol
  env: tst
bits: {}
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
support:
  default_url: "https://support.ecmwf.int/"
  realms:
    desp: "https://platform.destine.eu/contact/"
"#;
    let cfg: ServerConfig = serde_yaml::from_str(yaml).expect("config parses");
    build_app(cfg).expect("app builds").0
}

#[tokio::test]
async fn unauthenticated_error_is_rewritten_with_default_url_and_request_id() {
    let resp = app()
        .oneshot(
            Request::get("/api/v2/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let rid = resp
        .headers()
        .get("x-request-id")
        .expect("X-Request-Id header present on error")
        .to_str()
        .unwrap()
        .to_string();
    assert!(!rid.is_empty());

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let obj = v.as_object().expect("body is a JSON object");

    // Exactly one field, and it is a string: cannot crash the Python client.
    assert_eq!(obj.len(), 1, "error body must be a single field");
    assert!(
        obj.values().all(serde_json::Value::is_string),
        "every error body value must be a string"
    );

    let msg = obj["message"].as_str().unwrap();
    assert!(msg.starts_with("Your request was not authorised"));
    assert!(msg.contains("https://support.ecmwf.int/")); // deployment default (no realm pre-auth)
    assert!(msg.contains(&rid)); // request ID quoted back to the user
}

#[tokio::test]
async fn successful_response_still_carries_request_id_header() {
    let resp = app()
        .oneshot(Request::get("/api/v2/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-request-id").is_some());
}

#[tokio::test]
async fn inbound_request_id_is_preserved() {
    let resp = app()
        .oneshot(
            Request::get("/api/v2/collections")
                .header("X-Request-Id", "caller-supplied-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let rid = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(rid, "caller-supplied-id");
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        v["message"]
            .as_str()
            .unwrap()
            .contains("caller-supplied-id")
    );
}

#[test]
fn resolver_prefers_realm_then_falls_back_to_default() {
    let sc = SupportConfig {
        default_url: Some("https://support.ecmwf.int/".into()),
        realms: HashMap::from([(
            "desp".to_string(),
            "https://platform.destine.eu/contact/".to_string(),
        )]),
    };
    // Authenticated DESP user → DestinE, always (including 5xx, since resolution is uniform).
    assert_eq!(
        sc.resolve(Some("desp")),
        Some("https://platform.destine.eu/contact/")
    );
    // Authenticated ecmwf-realm user (unmapped) → deployment default.
    assert_eq!(
        sc.resolve(Some("ecmwf")),
        Some("https://support.ecmwf.int/")
    );
    // Pre-auth (no realm) → deployment default.
    assert_eq!(sc.resolve(None), Some("https://support.ecmwf.int/"));
}
