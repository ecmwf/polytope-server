use clap::Parser;

#[derive(Parser)]
#[command(name = "polytope-server", about = "Polytope data retrieval server")]
struct Cli {
    config: String,
}

#[tokio::main]
async fn main() {
    polytope_observability::init_tracing("polytope-frontend");

    let cli = Cli::parse();
    let cfg = polytope_server::config::ServerConfig::from_file(&cli.config).unwrap_or_else(|e| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", config_path = %cli.config, error = %e, "failed to load config");
        std::process::exit(1);
    });
    tracing::info!("event.name" = "startup.config.loaded", outcome = "success", config_path = %cli.config, "config loaded");

    let bind_addr = cfg.bind_addr();
    let internal_poll_bind_addr = cfg.internal_poll_bind_addr();

    let (app, state) = polytope_server::build_app(cfg).unwrap_or_else(|e| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", error = %e, "failed to build app");
        std::process::exit(1);
    });

    for name in state.collections.keys() {
        tracing::debug!(collection = %name, "registered collection");
    }

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("event.name" = "startup.config.failed", outcome = "error", bind_addr = %bind_addr, error = %e, "failed to bind listener");
            std::process::exit(1);
        });
    tracing::info!("event.name" = "startup.server.listening", outcome = "success", addr = %listener.local_addr().unwrap(), "server listening");

    if let Some(internal_poll_bind_addr) = internal_poll_bind_addr {
        let internal_poll_listener = tokio::net::TcpListener::bind(&internal_poll_bind_addr)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("event.name" = "startup.config.failed", outcome = "error", bind_addr = %internal_poll_bind_addr, error = %e, "failed to bind internal poll listener");
                std::process::exit(1);
            });
        tracing::info!("event.name" = "startup.internal_poll.listening", outcome = "success", addr = %internal_poll_listener.local_addr().unwrap(), "internal poll listener listening");
        let internal_poll_app = polytope_server::build_internal_poll_app(state.clone());
        tokio::spawn(async move {
            if let Err(error) = axum::serve(internal_poll_listener, internal_poll_app).await {
                tracing::error!("event.name" = "startup.internal_poll.failed", outcome = "error", error = %error, "internal poll listener failed");
                std::process::exit(1);
            }
        });
    }

    let result = axum::serve(listener, app).await;
    tracing::info!(
        "event.name" = "startup.shutdown.complete",
        outcome = "success",
        "server shutdown complete"
    );
    result.unwrap();
}
