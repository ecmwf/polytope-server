mod routes;
mod state;

use std::sync::Arc;

use axum::{routing::{get, post}, Router};
use state::AppState;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = std::env::var("BITS_CONFIG")
        .unwrap_or_else(|_| "routes: {}".to_string());

    let bits = bits::Bits::from_config(&config).expect("Failed to initialise bits");
    let state = Arc::new(AppState { bits });

    let app = Router::new()
        .route("/api/v1/test", get(routes::test))
        .route("/api/v1/collections", get(routes::list_collections))
        .route("/api/v1/requests", get(routes::list_requests))
        .route(
            "/api/v1/requests/:id",
            post(routes::submit_request)
                .get(routes::get_request)
                .delete(routes::delete_request),
        )
        .route("/api/v1/downloads/:id", get(routes::downloads_deprecated))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
