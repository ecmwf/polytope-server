mod config;
mod routes;
mod state;

use std::sync::Arc;

use axum::{routing::{get, post}, Router};
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

    let app = Router::new()
        .route("/api/v1/test", get(routes::test))
        .route("/api/v1/collections", get(routes::list_collections))
        .route("/api/v1/requests", get(routes::list_requests))
        .route(
            "/api/v1/requests/{id}",
            post(routes::submit_request)
                .get(routes::get_request)
                .delete(routes::delete_request),
        )
        .route("/api/v1/downloads/{id}", get(routes::downloads_deprecated))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap_or_else(|e| {
        eprintln!("Failed to bind to {}: {}", bind_addr, e);
        std::process::exit(1);
    });
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
