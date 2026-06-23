use clap::Parser;

use polytope_server::config::ServerConfig;

#[derive(Parser)]
#[command(name = "polytope-server", about = "Polytope data retrieval server")]
struct Cli {
    config: String,
}

#[cfg(feature = "telemetry")]
fn init_meter_provider(
    site: &str,
    env: &str,
    broker_id: &str,
) -> (
    opentelemetry_sdk::metrics::SdkMeterProvider,
    prometheus::Registry,
) {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("prometheus exporter should build");

    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", "polytope-server"),
            KeyValue::new("service.instance.id", broker_id.to_owned()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment", env.to_owned()),
            KeyValue::new("bits.site", site.to_owned()),
            KeyValue::new("bits.env", env.to_owned()),
        ])
        .build();

    let rename_view = |i: &opentelemetry_sdk::metrics::Instrument| {
        let name = i.name();
        let renamed = name
            .replace("bits.jobs.", "polytope.broker.requests.")
            .replace("bits.job.", "polytope.broker.request.")
            .replace("bits.route_handle.jobs.", "polytope.broker.collection.requests.")
            .replace("bits.route_handle.job.", "polytope.broker.collection.request.")
            .replace("bits.dispatcher.", "polytope.broker.dispatcher.");
        if renamed != name {
            Some(
                opentelemetry_sdk::metrics::Stream::builder()
                    .with_name(renamed)
                    .build()
                    .expect("renamed stream should be valid"),
            )
        } else {
            None
        }
    };

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(exporter)
        .with_view(rename_view)
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());

    (provider, registry)
}

#[cfg(feature = "telemetry")]
async fn serve_metrics(registry: prometheus::Registry, port: u16) {
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::{Router, routing::get};
    use prometheus::Encoder;

    async fn handler(State(reg): State<prometheus::Registry>) -> impl IntoResponse {
        let encoder = prometheus::TextEncoder::new();
        let families = reg.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        (
            [(
                axum::http::header::CONTENT_TYPE,
                encoder.format_type().to_owned(),
            )],
            buf,
        )
    }

    let app = Router::new()
        .route("/metrics", get(handler))
        .with_state(registry);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap_or_else(|e| {
            tracing::error!(port, error = %e, "failed to bind metrics endpoint");
            std::process::exit(1);
        });

    tracing::info!(port, "prometheus /metrics endpoint listening");
    let _ = axum::serve(listener, app).await;
}

#[tokio::main]
async fn main() {
    polytope_observability::init_tracing("polytope-frontend");

    let cli = Cli::parse();
    let cfg = ServerConfig::from_file(&cli.config).unwrap_or_else(|e| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", config_path = %cli.config, error = %e, "failed to load config");
        std::process::exit(1);
    });
    tracing::info!("event.name" = "startup.config.loaded", outcome = "success", config_path = %cli.config, "config loaded");

    let bind_addr = cfg.bind_addr();

    #[cfg(feature = "telemetry")]
    let metrics_config = cfg.metrics.clone().unwrap_or_default();
    #[cfg(feature = "telemetry")]
    let polytope_env = cfg.polytope.env.clone();
    #[cfg(feature = "telemetry")]
    let polytope_site = cfg.polytope.site.clone();

    let (app, state) = polytope_server::build_app(cfg).unwrap_or_else(|e| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", error = %e, "failed to build app");
        std::process::exit(1);
    });

    #[cfg(feature = "telemetry")]
    let _meter_provider = if metrics_config.enabled {
        let (provider, registry) =
            init_meter_provider(&polytope_site, &polytope_env, state.bits.broker_id());
        tokio::spawn(serve_metrics(registry, metrics_config.port));
        Some(provider)
    } else {
        None
    };

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
    let result = axum::serve(listener, app).await;

    #[cfg(feature = "telemetry")]
    if let Some(provider) = _meter_provider {
        if let Err(e) = provider.shutdown() {
            tracing::warn!(error = %e, "meter provider shutdown failed");
        }
    }

    tracing::info!(
        "event.name" = "startup.shutdown.complete",
        outcome = "success",
        "server shutdown complete"
    );
    result.unwrap();
}
