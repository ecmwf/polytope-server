// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end checks that user-facing errors carry support guidance and the
//! call's request ID, and that the error body stays a flat
//! string→string object (so the Python `polytope-client`, which flattens every
//! value and crashes on non-strings, renders it cleanly).

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use polytope_server::build_app;
use polytope_server::config::{ServerConfig, SupportConfig};
use tower::ServiceExt;

fn config() -> ServerConfig {
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
    serde_yaml::from_str(yaml).expect("config parses")
}

fn app() -> axum::Router {
    build_app(config()).expect("app builds").0
}

// A plausible opaque request ID (26-char Crockford base32; see docs/request-ids.md).
const RID: &str = "3k7p9q2r5s8t1v4w6x0y2z5a8b";

fn quoted_request_id(msg: &str) -> Option<&str> {
    msg.split("quote your request ID ")
        .nth(1)
        .and_then(|rest| rest.split('.').next())
}

async fn message(resp: axum::response::Response) -> String {
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    v["message"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn error_body_is_a_single_string_field() {
    let resp = app()
        .oneshot(
            Request::get("/api/v2/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

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
}

#[tokio::test]
async fn rejected_submit_quotes_a_request_id() {
    // A submit rejected before BITS sees it has no job, but still gets an ID.
    let resp = app()
        .oneshot(
            Request::post("/api/v2/ecmwf/requests")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let msg = message(resp).await;
    let quoted = quoted_request_id(&msg).expect("a request ID is quoted");
    bits::request_id::decode(quoted).expect("quoted ID is a BITS request ID");
}

#[tokio::test]
async fn v1_submit_path_segment_is_not_taken_as_the_request_id() {
    // `POST /api/v1/requests/{collection}` names a collection, not a request.
    let resp = app()
        .oneshot(
            Request::post("/api/v1/requests/ecmwf")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    let msg = message(resp).await;
    let quoted = quoted_request_id(&msg).expect("a request ID is quoted");
    assert_ne!(quoted, "ecmwf");
    bits::request_id::decode(quoted).expect("quoted ID is a BITS request ID");
}

#[tokio::test]
async fn error_on_a_request_path_quotes_the_url_derived_request_id() {
    // The ID lives in the URL path; even an unauthenticated 401 (rejected before
    // the handler) must quote it in the message.
    let resp = app()
        .oneshot(
            Request::get(format!("/api/v2/requests/{RID}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let msg = v["message"].as_str().unwrap();
    assert!(msg.contains(&format!("quote your request ID {RID}")));
}

#[tokio::test]
async fn inbound_request_id_header_is_ignored() {
    // No real client supplies this header; a caller-supplied value must never be
    // trusted or surfaced back to the user.
    let resp = app()
        .oneshot(
            Request::get("/api/v2/collections")
                .header("X-Request-Id", "caller-supplied-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        !v["message"]
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
