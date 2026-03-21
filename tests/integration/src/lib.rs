use std::error::Error;
use std::sync::Arc;

use authotron::config::{
    ConfigV2, JWTConfig, LoggingConfig, MetricsConfig, ServerConfig as AuthotronServerConfig,
    StoreConfig,
};
use authotron::providers::plain_provider::{PlainAuthConfig, PlainUserEntry};
use authotron::providers::ProviderConfig;
use authotron::startup;
use axum::{
    body::Body,
    extract::Request,
    http::{Response, StatusCode},
    routing::{any, get},
    Router,
};
#[cfg(test)]
use polytope_client::auth::AuthHeader;
#[cfg(test)]
use polytope_client::polytope_client::{ApiVersion, PolytopeClient};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const FAKE_GRIB: &[u8] = b"\x00\x01\x02\x03GRIB_FAKE_DATA\xff\xfe";
const JWT_SECRET: &str = "integration-test-secret";

const ALPHA_ADMIN_USER: &str = "alpha_admin";
const ALPHA_ADMIN_PASS: &str = "adminpass";
const ALPHA_REGULAR_USER: &str = "alpha_regular";
const ALPHA_REGULAR_PASS: &str = "regularpass";
const BETA_ADMIN_USER: &str = "beta_admin";
const BETA_ADMIN_PASS: &str = "betapass";

pub async fn spawn_mock_backend() -> Result<(String, JoinHandle<()>), Box<dyn Error>> {
    async fn handle_any(_req: Request<Body>) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/x-grib")
            .header("Content-Length", FAKE_GRIB.len().to_string())
            .body(Body::from(FAKE_GRIB.to_vec()))
            .expect("valid response")
    }

    async fn handle_download() -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/x-grib")
            .header("Content-Length", FAKE_GRIB.len().to_string())
            .body(Body::from(FAKE_GRIB.to_vec()))
            .expect("valid response")
    }

    let app = Router::new()
        .route("/", any(handle_any))
        .route("/download/{*path}", get(handle_download))
        .route("/{*path}", any(handle_any));

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok((format!("http://{}", addr), handle))
}

pub async fn spawn_authotron(jwt_secret: &str) -> Result<(String, JoinHandle<()>), Box<dyn Error>> {
    let config = Arc::new(ConfigV2 {
        store: StoreConfig {
            enabled: false,
            backend: None,
        },
        services: vec![],
        providers: vec![
            ProviderConfig::Plain(PlainAuthConfig {
                name: "alpha-provider".to_string(),
                realm: "alpha".to_string(),
                users: vec![
                    PlainUserEntry {
                        username: ALPHA_ADMIN_USER.to_string(),
                        password: ALPHA_ADMIN_PASS.to_string(),
                        roles: Some(vec!["admin".to_string()]),
                    },
                    PlainUserEntry {
                        username: ALPHA_REGULAR_USER.to_string(),
                        password: ALPHA_REGULAR_PASS.to_string(),
                        roles: None,
                    },
                ],
            }),
            ProviderConfig::Plain(PlainAuthConfig {
                name: "beta-provider".to_string(),
                realm: "beta".to_string(),
                users: vec![PlainUserEntry {
                    username: BETA_ADMIN_USER.to_string(),
                    password: BETA_ADMIN_PASS.to_string(),
                    roles: Some(vec!["admin".to_string()]),
                }],
            }),
        ],
        augmenters: vec![],
        server: AuthotronServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
        },
        metrics: MetricsConfig {
            enabled: false,
            port: 0,
        },
        jwt: JWTConfig {
            iss: "integration-tests".to_string(),
            aud: None,
            exp: 3600,
            secret: jwt_secret.to_string(),
        },
        include_legacy_headers: None,
        logging: LoggingConfig {
            level: "error".to_string(),
            format: "console".to_string(),
            service_name: "authotron-integration-tests".to_string(),
            service_version: "0.0.0".to_string(),
        },
        auth: authotron::config::AuthConfig::default(),
    });

    let (app, _state) = startup::build_app(config).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    Ok((format!("http://{}", addr), handle))
}

pub async fn spawn_polytope_server(
    authotron_url: Option<&str>,
    bits_yaml: &str,
) -> Result<(String, JoinHandle<()>), Box<dyn Error>> {
    let server_config = if let Some(auth_url) = authotron_url {
        format!(
            r#"
server:
  host: "127.0.0.1"
  port: 0
authentication:
  url: "{auth_url}"
  secret: "{JWT_SECRET}"
bits:
{bits_yaml}
"#
        )
    } else {
        format!(
            r#"
server:
  host: "127.0.0.1"
  port: 0
bits:
{bits_yaml}
"#
        )
    };

    let mut config_file = NamedTempFile::new()?;
    use std::io::Write as _;
    config_file.write_all(server_config.as_bytes())?;

    let cfg = polytope_server::config::ServerConfig::from_file(
        config_file.path().to_str().expect("utf8 path"),
    )?;

    let (app, _state) = polytope_server::build_app(cfg)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok((format!("http://{}", addr), handle))
}

#[cfg(test)]
fn fallback_bits_yaml(backend_url: &str) -> String {
    format!(
        r#"
  routes:
    - default:
        - switch:
            - admin_only:
                - check::has_role:
                    role: admin
                    realm: alpha
                - target::http:
                    url: "{backend_url}/"
            - catch_all:
                - target::http:
                    url: "{backend_url}/"
"#
    )
}

#[cfg(test)]
fn strict_bits_yaml(backend_url: &str) -> String {
    format!(
        r#"
  routes:
    - admin_only:
        - check::has_role:
            role: admin
            realm: alpha
        - target::http:
            url: "{backend_url}/"
"#
    )
}

#[cfg(test)]
fn simple_bits_yaml(backend_url: &str) -> String {
    format!(
        r#"
  routes:
    - default:
        - target::http:
            url: "{backend_url}/"
"#
    )
}

#[tokio::test]
async fn health_check_v1() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (server_url, server) = spawn_polytope_server(None, &simple_bits_yaml(&backend_url))
        .await
        .expect("spawn polytope server");

    let res = reqwest::get(format!("{server_url}/api/v1/test"))
        .await
        .expect("request succeeds");
    assert_eq!(res.status(), StatusCode::OK);

    server.abort();
    backend.abort();
}

#[tokio::test]
async fn health_check_v2() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (server_url, server) = spawn_polytope_server(None, &simple_bits_yaml(&backend_url))
        .await
        .expect("spawn polytope server");

    let res = reqwest::get(format!("{server_url}/api/v2/health"))
        .await
        .expect("request succeeds");
    assert_eq!(res.status(), StatusCode::OK);

    server.abort();
    backend.abort();
}

#[tokio::test]
async fn authenticated_retrieve_v1() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            ALPHA_ADMIN_USER.to_string(),
            ALPHA_ADMIN_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(0.1),
    )
    .expect("client created")
    .api_version(ApiVersion::V1);

    let (_, _, size) = client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
        .expect("retrieve succeeds");
    assert_eq!(size, FAKE_GRIB.len() as u64);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn authenticated_retrieve_v2() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            ALPHA_ADMIN_USER.to_string(),
            ALPHA_ADMIN_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(0.1),
    )
    .expect("client created");

    let (_, _, size) = client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
        .expect("retrieve succeeds");
    assert_eq!(size, FAKE_GRIB.len() as u64);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn unauthenticated_rejected() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{server_url}/api/v2/requests"))
        .json(&serde_json::json!({"class": "od"}))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn role_admin_correct_realm_passes() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &fallback_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            ALPHA_ADMIN_USER.to_string(),
            ALPHA_ADMIN_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(0.1),
    )
    .expect("client created");

    let (_, _, size) = client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
        .expect("retrieve succeeds");
    assert_eq!(size, FAKE_GRIB.len() as u64);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn role_regular_user_falls_through() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &fallback_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            ALPHA_REGULAR_USER.to_string(),
            ALPHA_REGULAR_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(0.1),
    )
    .expect("client created");

    let (_, _, size) = client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
        .expect("retrieve succeeds");
    assert_eq!(size, FAKE_GRIB.len() as u64);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn role_wrong_realm_falls_through() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &fallback_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            BETA_ADMIN_USER.to_string(),
            BETA_ADMIN_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(0.1),
    )
    .expect("client created");

    let (_, _, size) = client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
        .expect("retrieve succeeds");
    assert_eq!(size, FAKE_GRIB.len() as u64);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn strict_admin_allowed() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            ALPHA_ADMIN_USER.to_string(),
            ALPHA_ADMIN_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(0.1),
    )
    .expect("client created");

    let (_, _, size) = client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
        .expect("retrieve succeeds");
    assert_eq!(size, FAKE_GRIB.len() as u64);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn strict_regular_rejected() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let auth = AuthHeader::Basic(
        ALPHA_REGULAR_USER.to_string(),
        ALPHA_REGULAR_PASS.to_string(),
    );
    let auth_value: String = auth.into();

    let res = client
        .post(format!("{server_url}/api/v2/requests"))
        .header("Authorization", auth_value)
        .json(&serde_json::json!({"class": "od"}))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn strict_wrong_realm_rejected() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url))
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let auth = AuthHeader::Basic(BETA_ADMIN_USER.to_string(), BETA_ADMIN_PASS.to_string());
    let auth_value: String = auth.into();

    let res = client
        .post(format!("{server_url}/api/v2/requests"))
        .header("Authorization", auth_value)
        .json(&serde_json::json!({"class": "od"}))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    server.abort();
    authotron.abort();
    backend.abort();
}
