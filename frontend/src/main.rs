mod api;
mod config;
mod state;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use clap::Parser;
use state::AppState;

use bits_ecmwf as _;

#[derive(Parser)]
#[command(name = "polytope-server", about = "Polytope data retrieval server")]
struct Cli {
    config: String,
}

/// BitsSubmitter wraps Arc<AppState> to implement the polytope_edr::RequestSubmitter trait.
/// We hold Arc<AppState> (not bits::Bits directly) because bits::Bits is not Clone.
struct BitsSubmitter {
    state: Arc<AppState>,
}

impl polytope_edr::RequestSubmitter for BitsSubmitter {
    fn submit(
        &self,
        request: serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<polytope_edr::SubmitResponse, polytope_edr::SubmitError>> + Send>,
    > {
        let state = self.state.clone();
        Box::pin(async move {
            let job = bits::Job::new(request);
            let handle = state.bits.submit(job);
            Ok(polytope_edr::SubmitResponse {
                id: handle.id.clone(),
                poll_url: format!("/api/v2/requests/{}", handle.id),
            })
        })
    }
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

    let v2 = Router::new()
        .route("/health", get(api::v2::health))
        .route("/requests", post(api::v2::submit))
        .route("/requests/{id}", get(api::v2::poll).delete(api::v2::cancel));

    let openmeteo = api::openmeteo::router();

    let edr_router = if let Some(edr_value) = cfg.edr {
        let edr_config = polytope_edr::EdrConfig::from_value(edr_value)
            .expect("Failed to parse edr config");
        let submitter = Arc::new(BitsSubmitter {
            state: state.clone(),
        });
        tracing::info!("EDR endpoints enabled ({} collections)", edr_config.collections.len());
        Some(polytope_edr::router(edr_config, submitter, "/edr".to_string()))
    } else {
        tracing::info!("No edr config, EDR endpoints disabled");
        None
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .nest("/api/v1", v1)
        .nest("/api/v2", v2)
        .nest("/openmeteo/v1", openmeteo)
        .with_state(state);

    // Nest EDR router separately (it manages its own state internally)
    let app = if let Some(edr) = edr_router {
        app.nest("/edr", edr)
    } else {
        app
    };

    let app = app.layer(cors);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to {}: {}", bind_addr, e);
            std::process::exit(1);
        });
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
