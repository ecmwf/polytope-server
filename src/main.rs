mod api;
mod config;
mod state;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use state::AppState;

#[derive(Parser)]
#[command(name = "polytope-server", about = "Polytope data retrieval server")]
struct Cli {
    /// Path to the YAML configuration file.
    config: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let cfg = config::ServerConfig::from_file(&cli.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config '{}': {}", cli.config, e);
        std::process::exit(1);
    });

    let bits_yaml = cfg.bits_yaml().unwrap_or_else(|e| {
        eprintln!("Failed to serialise bits config: {}", e);
        std::process::exit(1);
    });

    let bits = bits::Bits::from_config(&bits_yaml).unwrap_or_else(|e| {
        eprintln!("Failed to initialise bits: {}", e);
        std::process::exit(1);
    });

    let state = Arc::new(AppState { bits });
    let bind_addr = cfg.bind_addr();

    // v1: legacy Polytope API, retained for backwards compatibility
    let v1 = Router::new()
        .route("/test", get(api::v1::test))
        .route("/collections", get(api::v1::list_collections))
        .route("/requests", get(api::v1::list_requests))
        .route(
            "/requests/{id}",
            post(api::v1::submit_request)
                .get(api::v1::get_request)
                .delete(api::v1::delete_request),
        )
        .route("/downloads/{id}", get(api::v1::downloads_deprecated));

    // v2: idiomatic bits API
    let v2 = Router::new()
        .route("/test", get(api::v2::test))
        .route("/requests", post(api::v2::submit))
        .route("/requests/{id}", get(api::v2::poll).delete(api::v2::cancel));

    let app = Router::new()
        .nest("/api/v1", v1)
        .nest("/api/v2", v2)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to {}: {}", bind_addr, e);
            std::process::exit(1);
        });
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
