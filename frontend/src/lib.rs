pub mod api;
pub mod auth;
pub mod collection;
pub mod config;
#[cfg(feature = "metkit")]
mod metkit_expansion;
pub mod state;

use std::sync::Arc;

use axum::{
    Router, middleware,
    routing::{get, post},
};
use state::AppState;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};

use bits_ecmwf as _;

/// Build the polytope-server Axum application from a config.
///
/// Returns a `(Router, Arc<AppState>)`. The caller is responsible for
/// binding a `TcpListener` and calling `axum::serve()`.
pub fn build_app(
    cfg: config::ServerConfig,
) -> Result<(Router, Arc<AppState>), Box<dyn std::error::Error>> {
    let bits_yaml = cfg.bits_yaml()?;
    let bits = bits::Bits::from_config(&bits_yaml)?;

    let allow_anonymous = cfg
        .authentication
        .as_ref()
        .is_some_and(|a| a.allow_anonymous);

    let auth_client = cfg.authentication.as_ref().map(|auth_cfg| {
        auth::AuthClient::new(
            &auth_cfg.url,
            auth_cfg.resolved_secret().as_bytes(),
            std::time::Duration::from_millis(auth_cfg.timeout_ms),
            auth_cfg.cache_ttl_secs.map(std::time::Duration::from_secs),
            auth_cfg.cache_capacity,
        )
    });

    let state = Arc::new(AppState {
        bits,
        auth_client,
        allow_anonymous,
    });

    let v1_protected = Router::new()
        .route("/collections", get(api::v1::list_collections))
        .route("/requests", get(api::v1::list_requests))
        .route(
            "/requests/{id}",
            post(api::v1::submit_request)
                .get(api::v1::get_request)
                .delete(api::v1::delete_request),
        )
        .route("/downloads/{id}", get(api::v1::downloads_deprecated));

    let v2_protected = Router::new()
        .route("/requests", post(api::v2::submit))
        .route("/requests/{id}", get(api::v2::poll).delete(api::v2::cancel));

    let openmeteo = api::openmeteo::router();

    let edr_router = if let Some(edr_value) = cfg.edr {
        let edr_config = polytope_edr::EdrConfig::from_value(edr_value)?;
        let submitter = Arc::new(BitsSubmitter {
            state: state.clone(),
        });
        Some(polytope_edr::router(
            edr_config,
            submitter,
            "/edr".to_string(),
        ))
    } else {
        None
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let mut protected = Router::new()
        .nest("/api/v1", v1_protected)
        .nest("/api/v2", v2_protected)
        .nest("/openmeteo/v1", openmeteo);

    if state.auth_client.is_some() {
        protected = protected.layer(middleware::from_fn_with_state(
            state.clone(),
            auth::middleware::auth_middleware,
        ));
    }

    let app = Router::new()
        .route("/api/v1/test", get(api::v1::test))
        .route("/api/v2/health", get(api::v2::health))
        .merge(protected)
        .with_state(state.clone());

    let app = if let Some(edr) = edr_router {
        app.nest("/edr", edr)
    } else {
        app
    };

    let app = app.layer(cors).layer(CompressionLayer::new());

    Ok((app, state))
}

/// BitsSubmitter wraps Arc<AppState> to implement the polytope_edr::RequestSubmitter trait.
struct BitsSubmitter {
    state: Arc<AppState>,
}

impl polytope_edr::RequestSubmitter for BitsSubmitter {
    fn submit(
        &self,
        request: serde_json::Value,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<polytope_edr::SubmitResponse, polytope_edr::SubmitError>,
                > + Send,
        >,
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
