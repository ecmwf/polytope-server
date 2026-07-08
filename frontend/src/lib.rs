mod actions;
pub mod api;
pub mod auth;
pub mod config;
#[cfg(feature = "metkit")]
mod metkit_expansion;
pub mod state;
pub mod support;

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
    // `cfg.bits_yaml()` returns the contents of the chart's outer `bits:`
    // block. That block IS the top-level YAML consumed by
    // `bits::parse_bootstrap` (with sibling keys `bits`, `targets`, `routes`,
    // ...). Strip `collections` out for separate registration via
    // `add_route`, then pass the remainder through unchanged.
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
        admin_bypass_roles: cfg.admin_bypass_roles,
        support: cfg.support,
    });

    let v1_protected = Router::new()
        .route("/collections", get(api::v1::list_collections))
        .route("/requests", get(api::v1::list_requests))
        .route("/user", get(api::v1::user_info))
        .route(
            "/requests/{id}",
            post(api::v1::submit_request)
                .get(api::v1::get_request)
                .delete(api::v1::delete_request),
        )
        .route("/downloads/{id}", get(api::v1::downloads_deprecated))
        .route(
            "/uploads/{id}",
            get(api::v1::uploads_deprecated).post(api::v1::uploads_deprecated),
        );

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

    let protected = Router::new()
        .nest("/api/v1", v1_protected)
        .nest("/api/v2", v2_protected)
        .nest("/openmeteo/v1", openmeteo)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::middleware::auth_middleware,
        ));

    let app = Router::new()
        .route("/api/v1/test", get(api::v1::test))
        .route("/api/v2/health", get(api::v2::health))
        .merge(protected)
        .with_state(state.clone());

    let app = if let Some(edr) = edr_router {
        app.nest(
            "/edr",
            edr.layer(middleware::from_fn_with_state(
                state.clone(),
                auth::middleware::auth_middleware,
            )),
        )
    } else {
        app
    };

    let app = app
        .layer(cors)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            support::request_context_middleware,
        ))
        .layer(CompressionLayer::new());

    Ok((app, state))
}

pub fn build_internal_poll_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/internal/poll/{id}", get(api::v2::poll))
        .with_state(state)
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
                match state.bits.submit(job) {
                    bits::SubmitOutcome::Accepted(handle) => handle,
                    bits::SubmitOutcome::Overloaded => {
                        return Err(polytope_edr::SubmitError::Upstream(
                            "broker at capacity".to_string(),
                        ));
                    }
                }
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
                match route_handle.submit(job) {
                    bits::SubmitOutcome::Accepted(handle) => handle,
                    bits::SubmitOutcome::Overloaded => {
                        return Err(polytope_edr::SubmitError::Upstream(
                            "broker at capacity".to_string(),
                        ));
                    }
                }
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

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    fn internal_poll_test_config(auth: bool) -> config::ServerConfig {
        let authentication = if auth {
            r#"
authentication:
  url: "http://127.0.0.1:1"
  secret: "testsecret"
"#
        } else {
            ""
        };
        let yaml = format!(
            r#"
polytope:
  site: bol
  env: dev
bits: {{}}
{authentication}"#
        );
        serde_yaml::from_str(&yaml).expect("test config should parse")
    }

    fn edr_test_config() -> config::ServerConfig {
        let yaml = r#"
polytope:
  site: bol
  env: dev
bits: {}
authentication:
  url: "http://127.0.0.1:1"
  secret: "testsecret"
edr:
  collections:
    operational-data:
      title: "Operational Data"
      extents:
        spatial:
          bbox: [-180.0, -90.0, 180.0, 90.0]
      base_request:
        class: "od"
        stream: "oper"
        type: "fc"
        levtype: "sfc"
        expver: "0001"
      supported_queries:
        - position
      parameters: {}
"#;
        serde_yaml::from_str(yaml).expect("EDR test config should parse")
    }

    async fn response_status(app: Router, method: axum::http::Method, path: &str) -> StatusCode {
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
    }

    #[tokio::test]
    async fn request_id_config_server_emits_new_format_request_id() {
        let yaml = r#"
polytope:
  site: bol
  env: dev
bits: {}
"#;
        let cfg: config::ServerConfig =
            serde_yaml::from_str(yaml).expect("top-level polytope site/env config should parse");
        let (_, state) = build_app(cfg).expect("polytope site/env should be injected into BITS");

        let handle = state
            .bits
            .submit(bits::Job::new(serde_json::json!({})))
            .expect_accepted("test broker should accept the request");
        let decoded = bits::request_id::decode(&handle.id)
            .expect("server-emitted request ID should use the new decodable format");

        assert_eq!(decoded.site, "bol");
        assert_eq!(decoded.env, "dev");
    }

    #[tokio::test]
    async fn internal_poll_router_exposes_only_internal_poll_get() {
        let (_, state) = build_app(internal_poll_test_config(false)).unwrap();
        let app = build_internal_poll_app(state);

        let poll_status = response_status(
            app.clone(),
            axum::http::Method::GET,
            "/internal/poll/unknown-request-id",
        )
        .await;
        assert_eq!(poll_status, StatusCode::NOT_FOUND);

        for path in [
            "/api/v1/test",
            "/api/v2/health",
            "/api/v2/collections",
            "/api/v2/requests/unknown-request-id",
            "/api/v2/ecmwf/requests",
            "/openmeteo/v1/forecast",
        ] {
            let status = response_status(app.clone(), axum::http::Method::GET, path).await;
            assert_eq!(
                status,
                StatusCode::NOT_FOUND,
                "{path} should not be mounted"
            );
        }

        let submit_status = response_status(
            app.clone(),
            axum::http::Method::POST,
            "/internal/poll/unknown-request-id",
        )
        .await;
        assert_eq!(submit_status, StatusCode::METHOD_NOT_ALLOWED);

        let cancel_status = response_status(
            app,
            axum::http::Method::DELETE,
            "/internal/poll/unknown-request-id",
        )
        .await;
        assert_eq!(cancel_status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn internal_poll_is_auth_exempt_while_public_poll_remains_protected() {
        let (public_app, state) = build_app(internal_poll_test_config(true)).unwrap();
        let internal_app = build_internal_poll_app(state);

        let public_status = response_status(
            public_app,
            axum::http::Method::GET,
            "/api/v2/requests/unknown-request-id",
        )
        .await;
        assert_eq!(public_status, StatusCode::UNAUTHORIZED);

        let internal_status = response_status(
            internal_app,
            axum::http::Method::GET,
            "/internal/poll/unknown-request-id",
        )
        .await;
        assert_eq!(internal_status, StatusCode::NOT_FOUND);
        assert_ne!(internal_status, StatusCode::UNAUTHORIZED);
        assert_ne!(internal_status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn edr_routes_require_authentication() {
        let (app, _) = build_app(edr_test_config()).unwrap();
        let status = response_status(app, axum::http::Method::GET, "/edr/").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_collections_parsed_from_config() {
        let yaml = r#"
polytope:
  site: bol
  env: dev
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
polytope:
  site: bol
  env: dev
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
