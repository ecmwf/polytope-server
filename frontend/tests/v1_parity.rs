//! Backwards-compatibility checks: the v1 API keeps parity with the legacy
//! Python polytope-server on the response shapes that clients depend on.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use polytope_server::build_app;
use polytope_server::config::ServerConfig;
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
async fn submit_accepted_body_matches_python_request_accepted_shape() {
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

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert_eq!(resp.headers().get(header::RETRY_AFTER).unwrap(), "5");
    assert!(
        resp.headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("./")
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"message": "Request queued", "status": "queued"})
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
