use clap::Parser;

#[derive(Parser)]
#[command(name = "polytope-server", about = "Polytope data retrieval server")]
struct Cli {
    config: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let cfg = polytope_server::config::ServerConfig::from_file(&cli.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config '{}': {}", cli.config, e);
        std::process::exit(1);
    });

    let bind_addr = cfg.bind_addr();

    let (app, _state) = polytope_server::build_app(cfg).unwrap_or_else(|e| {
        eprintln!("Failed to build app: {}", e);
        std::process::exit(1);
    });

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to {}: {}", bind_addr, e);
            std::process::exit(1);
        });
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
