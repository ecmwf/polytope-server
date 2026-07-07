use axum::extract::State;
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use prometheus::Encoder;

pub fn router(registry: prometheus::Registry) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(registry)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn metrics(State(registry): State<prometheus::Registry>) -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let families = registry.gather();
    let mut body = Vec::new();
    encoder
        .encode(&families, &mut body)
        .expect("encoding prometheus metrics should succeed");
    (
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type().to_owned(),
        )],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_returns_ok() {
        let app: Router = router(prometheus::Registry::new());
        let request = axum::http::Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = ServiceExt::<axum::http::Request<axum::body::Body>>::oneshot(app, request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: bytes::Bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json, serde_json::json!({"status": "ok"}));
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_payload() {
        let registry = prometheus::Registry::new();
        let counter = prometheus::IntCounter::new("worker_test_total", "test counter").unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.inc();

        let request = axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();
        let response =
            ServiceExt::<axum::http::Request<axum::body::Body>>::oneshot(router(registry), request)
                .await
                .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("worker_test_total 1"));
    }
}
