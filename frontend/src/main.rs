use clap::Parser;

use polytope_server::config::ServerConfig;

#[derive(Parser)]
#[command(name = "polytope-server", about = "Polytope data retrieval server")]
struct Cli {
    config: String,
}

#[cfg(feature = "telemetry")]
fn init_meter_provider(
    endpoint: Option<&str>,
    site: &str,
    env: &str,
    broker_id: &str,
) -> Option<opentelemetry_sdk::metrics::SdkMeterProvider> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let endpoint = endpoint?;
    let metrics_endpoint = if endpoint.ends_with("/v1/metrics") {
        endpoint.to_owned()
    } else {
        format!("{}/v1/metrics", endpoint.trim_end_matches('/'))
    };

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

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(&metrics_endpoint)
        .build()
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to build OTLP metric exporter");
            std::process::exit(1);
        });

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter).build();

    let rename_view = |i: &opentelemetry_sdk::metrics::Instrument| {
        let name = i.name();
        let renamed = name
            .replace("bits.jobs.", "polytope.requests.")
            .replace("bits.job.", "polytope.request.")
            .replace("bits.route_handle.jobs.", "polytope.collection.requests.")
            .replace("bits.route_handle.job.", "polytope.collection.request.");
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
        .with_reader(reader)
        .with_view(rename_view)
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());
    tracing::info!(endpoint = %endpoint, broker_id = %broker_id, "OTLP metrics exporter enabled");
    Some(provider)
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
    let otlp_endpoint = cfg.otlp_endpoint();
    #[cfg(feature = "telemetry")]
    let polytope_env = cfg.polytope.env.clone();
    #[cfg(feature = "telemetry")]
    let polytope_site = cfg.polytope.site.clone();

    let (app, state) = polytope_server::build_app(cfg).unwrap_or_else(|e| {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", error = %e, "failed to build app");
        std::process::exit(1);
    });

    #[cfg(feature = "telemetry")]
    let _meter_provider = init_meter_provider(
        otlp_endpoint.as_deref(),
        &polytope_site,
        &polytope_env,
        state.bits.broker_id(),
    );

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
            tracing::warn!(error = %e, "OTLP meter provider shutdown failed");
        }
    }

    tracing::info!(
        "event.name" = "startup.shutdown.complete",
        outcome = "success",
        "server shutdown complete"
    );
    result.unwrap();
}
