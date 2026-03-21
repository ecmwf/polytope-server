use axum::{routing::get, Json, Router};

pub fn router() -> Router {
    Router::new().route("/health", get(health))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_returns_ok() {
        let app: Router = router();
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
}
