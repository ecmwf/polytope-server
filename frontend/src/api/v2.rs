use std::sync::Arc;
use std::time::Duration;

use axum::{
    Extension, Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use bits::{Job, JobResult, PollOutcome};
use serde_json::{Value, json};

use crate::auth::AuthUser;
use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn health() -> &'static str {
    "Polytope server is alive"
}

pub async fn list_collections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut names: Vec<String> = state.collections.keys().cloned().collect();
    names.sort();
    (StatusCode::OK, Json(json!({"collections": names})))
}

pub async fn submit_collection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    auth_user: Option<Extension<AuthUser>>,
    Path(collection): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let route_handle = match state.collections.get(&collection) {
        Some(handle) => handle.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("unknown collection '{collection}'")})),
            )
                .into_response();
        }
    };

    let mut job = Job::new(body);
    let mut user_context = serde_json::Map::new();
    if let Some(ip) = super::client_ip(&headers) {
        user_context.insert("client_ip".to_string(), json!(ip));
    }
    if let Some(Extension(user)) = auth_user {
        user_context.insert("auth".to_string(), serde_json::to_value(&user).unwrap());
    }
    job.user = Value::Object(user_context).into();
    // Propagate Accept-Encoding so workers can choose an encoding codec
    if let Some(enc) = headers
        .get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        job.metadata_mut()["accept_encoding"] = serde_json::json!(enc);
    }
    let proxy_proto_addr = headers
        .get("x-proxy-protocol-addr")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<not set>");
    tracing::info!(
        "client IP candidates: x-forwarded-for={:?}, x-real-ip={:?}, x-proxy-protocol-addr={:?}",
        headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()),
        headers.get("x-real-ip").and_then(|v| v.to_str().ok()),
        proxy_proto_addr,
    );
    let id = route_handle.submit(job).id;

    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, format!("/api/v2/requests/{}", id))
            .body(Body::empty())
            .unwrap(),
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
            JobResult::Redirect { location, message } => Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location)
                .body(Body::from(message))
                .unwrap(),
            JobResult::Error { message } => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
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
            JobResult::Cancelled => {
                (StatusCode::OK, Json(json!({"status": "cancelled"}))).into_response()
            }
        },
    }
}

pub async fn poll(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.bits.poll(&id, Some(POLL_TIMEOUT)).await {
        PollOutcome::Pending { id } => Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, format!("/api/v2/requests/{}", id))
            .body(Body::empty())
            .unwrap(),
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
            JobResult::Redirect { location, message } => Response::builder()
                .status(StatusCode::SEE_OTHER)
                .header(header::LOCATION, location)
                .body(Body::from(message))
                .unwrap(),
            JobResult::Error { message } => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
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
            JobResult::Cancelled => {
                (StatusCode::OK, Json(json!({"status": "cancelled"}))).into_response()
            }
        },
    }
}

pub async fn cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.bits.cancel(&id);
    (
        StatusCode::OK,
        Json(json!({"id": id, "status": "cancelled"})),
    )
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
        let yaml = r#"targets:
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
        });
        Router::new()
            .route("/api/v2/collections", get(super::list_collections))
            .route(
                "/api/v2/{collection}/requests",
                post(super::submit_collection),
            )
            .route(
                "/api/v2/requests/{id}",
                get(super::poll).delete(super::cancel),
            )
            .with_state(state)
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
