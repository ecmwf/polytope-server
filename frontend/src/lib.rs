mod actions;
pub mod api;
pub mod auth;
pub mod config;
#[cfg(feature = "metkit")]
mod metkit_expansion;
pub mod state;

use std::sync::Arc;
use std::{collections::HashMap, io};

use axum::{
    Router, middleware,
    routing::{get, post},
};
use state::AppState;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};

/// Build the polytope-server Axum application from a config.
///
/// Returns a `(Router, Arc<AppState>)`. The caller is responsible for
/// binding a `TcpListener` and calling `axum::serve()`.
pub fn build_app(
    cfg: config::ServerConfig,
) -> Result<(Router, Arc<AppState>), Box<dyn std::error::Error>> {
    let bits_yaml = cfg.bits_yaml()?;
    let mut bits_value: serde_json::Value = serde_yaml::from_str(&bits_yaml)?;
    let collections_value = bits_value
        .as_object_mut()
        .and_then(|mapping| mapping.remove("collections"));

    let bits_yaml_no_collections = serde_yaml::to_string(&bits_value)?;
    let bits = bits::Bits::from_config(&bits_yaml_no_collections)?;

    let mut collections: HashMap<String, bits::RouteHandle> = HashMap::new();
    if let Some(collections_map) = collections_value {
        let map = collections_map.as_object().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "collections must be a mapping")
        })?;

        for (name, routes_value) in map {
            let handle = bits.add_route(name, routes_value)?;
            collections.insert(name.clone(), handle);
        }
    }

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
        collections,
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
        .route("/collections", get(api::v2::list_collections))
        .route("/{collection}/requests", post(api::v2::submit_collection))
        .route("/requests/{id}", get(api::v2::poll).delete(api::v2::cancel));

    let openmeteo = api::openmeteo::router();

    let edr_router = if let Some(edr_value) = cfg.edr {
        // Extract optional collection field from EDR config
        let collection = edr_value
            .get("collection")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default();

        // Validate that the collection exists if specified
        if !collection.is_empty() && !state.collections.contains_key(&collection) {
            return Err(format!(
                "EDR config references unknown bits collection '{}'",
                collection
            )
            .into());
        }

        let edr_config = polytope_edr::EdrConfig::from_value(edr_value)?;
        let submitter = Arc::new(BitsSubmitter {
            state: state.clone(),
            collection,
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
    collection: String,
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
        let collection = self.collection.clone();
        Box::pin(async move {
            let job = bits::Job::new(request);
            let handle = if collection.is_empty() {
                // Backward compat: no collection specified, use default bits.submit()
                state.bits.submit(job)
            } else {
                let route_handle = state
                    .collections
                    .get(&collection)
                    .ok_or_else(|| {
                        polytope_edr::SubmitError::Internal(format!(
                            "EDR collection '{}' not found in bits collections",
                            collection
                        ))
                    })?
                    .clone();
                route_handle.submit(job)
            };
            Ok(polytope_edr::SubmitResponse {
                id: handle.id.clone(),
                poll_url: format!("/api/v2/requests/{}", handle.id),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collections_parsed_from_config() {
        let yaml = r#"
bits:
  targets:
    my_target:
      type: http
      url: "http://localhost/"
  collections:
    ecmwf:
      - my_route:
          - target::my_target
    opendata:
      - other_route:
          - target::my_target
"#;
        let cfg: config::ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let (_, state) = build_app(cfg).unwrap();

        assert!(state.collections.contains_key("ecmwf"));
        assert!(state.collections.contains_key("opendata"));
        assert_eq!(state.collections.len(), 2);
    }

    #[test]
    fn test_no_collections_still_works() {
        let yaml = r#"
bits:
  targets:
    my_target:
      type: http
      url: "http://localhost/"
  routes:
    - default:
        - target::my_target
"#;
        let cfg: config::ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let (_, state) = build_app(cfg).unwrap();

        assert!(state.collections.is_empty());
    }
}
