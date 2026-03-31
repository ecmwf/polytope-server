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

#[cfg(test)]
pub async fn spawn_mock_backend_counted(
) -> Result<(String, JoinHandle<()>, Arc<std::sync::atomic::AtomicUsize>), Box<dyn Error>> {
    use std::sync::atomic::AtomicUsize;
    let hits = Arc::new(AtomicUsize::new(0));

    let app = Router::new()
        .route("/", any(counted_handler))
        .route("/download/{*path}", get(counted_handler))
        .route("/{*path}", any(counted_handler))
        .with_state(hits.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok((format!("http://{}", addr), handle, hits))
}

#[cfg(test)]
async fn counted_handler(
    axum::extract::State(hits): axum::extract::State<Arc<std::sync::atomic::AtomicUsize>>,
    _req: Request<Body>,
) -> Response<Body> {
    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/x-grib")
        .header("Content-Length", FAKE_GRIB.len().to_string())
        .body(Body::from(FAKE_GRIB.to_vec()))
        .expect("valid response")
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

pub async fn spawn_bobs() -> Result<(String, JoinHandle<()>, tempfile::TempDir), Box<dyn Error>> {
    let tmp_dir = tempfile::tempdir()?;
    let config = Arc::new(bobs::config::Config {
        host: "127.0.0.1".to_string(),
        port: 0,
        data_dir: tmp_dir.path().to_path_buf(),
        host_prefix: "test".to_string(),
        domain: "localhost".to_string(),
        route_name: "bobs".to_string(),
        ..bobs::config::Config::default()
    });

    let manager = Arc::new(bobs::manager::SpoolManager::<bobs::io::TokioFileIO>::new(
        tmp_dir.path().join("spools.redb"),
        tmp_dir.path(),
        config.page_size,
        config.max_cache_bytes,
    )?);

    let state = Arc::new(bobs::http::AppState {
        manager,
        config: config.clone(),
        hostname: "test-bobs-0".to_string(),
        ordinal: "0".to_string(),
    });

    let app = bobs::http::router::<bobs::io::TokioFileIO>().with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok((format!("http://{}", addr), handle, tmp_dir))
}

pub async fn spawn_test_worker(
    broker_url: &str,
    pool_name: &str,
    delivery_config: polytope_worker_common::delivery_config::DeliveryConfig,
) -> JoinHandle<()> {
    let config = polytope_worker_common::WorkerConfig {
        broker_url: format!("{}/{}", broker_url.trim_end_matches('/'), pool_name),
        poll_timeout_ms: 5000,
        heartbeat_interval: std::time::Duration::from_secs(30),
        retry_backoff: std::time::Duration::from_millis(100),
        management_port: 0,
    };

    let processor = test_worker::BehaviourProcessor::new(test_worker::TestConfig {
        behaviour: test_worker::Behaviour::Echo,
        content_type: "application/x-grib".to_string(),
    });

    tokio::spawn(async move {
        let _ = polytope_worker_common::run_worker_loop(config, delivery_config, processor).await;
    })
}

pub async fn spawn_polytope_server(
    authotron_url: Option<&str>,
    bits_yaml: &str,
    allow_anonymous: bool,
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
  allow_anonymous: {allow_anonymous}
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
  collections:
    all:
      - default:
          - switch:
              - admin_only:
                  - check::has_role:
                      alpha:
                        - admin
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
  collections:
    all:
      - admin_only:
          - check::has_role:
              alpha:
                - admin
          - target::http:
              url: "{backend_url}/"
"#
    )
}

#[cfg(test)]
fn auth_split_yaml(auth_backend_url: &str, public_backend_url: &str) -> String {
    format!(
        r#"
  collections:
    all:
      - authenticated:
          - check::has_role:
              alpha:
                - admin
          - target::http:
              url: "{auth_backend_url}/"
      - public:
          - target::http:
              url: "{public_backend_url}/"
"#
    )
}

#[cfg(test)]
fn simple_bits_yaml(backend_url: &str) -> String {
    format!(
        r#"
  collections:
    all:
      - switch:
          - target::http:
              url: "{backend_url}/"
"#
    )
}

#[cfg(test)]
fn bobs_bits_yaml(worker_server_port: u16) -> String {
    format!(
        r#"
  bits:
    worker_server:
      host: "127.0.0.1"
      port: {worker_server_port}
  targets:
    test_pool:
      type: remote
  collections:
    all:
      - switch:
          - target::test_pool
"#
    )
}

#[cfg(test)]
async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn health_check_v1() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (server_url, server) = spawn_polytope_server(None, &simple_bits_yaml(&backend_url), false)
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
    let (server_url, server) = spawn_polytope_server(None, &simple_bits_yaml(&backend_url), false)
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
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), false)
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
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), false)
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
async fn bobs_delivery_pipeline() {
    let worker_port = free_port().await;

    let (bobs_url, bobs_handle, _bobs_dir) = spawn_bobs().await.expect("spawn bobs");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &bobs_bits_yaml(worker_port), false)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build reqwest client");
    for _ in 0..100 {
        if client
            .get(format!("{server_url}/api/v2/health"))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    for _ in 0..100 {
        if client
            .get(format!(
                "http://127.0.0.1:{worker_port}/test_pool/work?timeout_ms=0"
            ))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let delivery = polytope_worker_common::delivery_config::DeliveryConfig {
        delivery_type: polytope_worker_common::delivery_config::DeliveryType::Bobs,
        bobs_url: Some(format!("{bobs_url}/api/v1")),
        s3_bucket: None,
        s3_region: None,
        s3_endpoint_url: None,
        s3_force_path_style: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_presigned_url_expiry_secs: None,
        s3_public_url: None,
        s3_key_prefix: String::new(),
    };
    let worker_handle = spawn_test_worker(
        &format!("http://127.0.0.1:{worker_port}"),
        "test_pool",
        delivery,
    )
    .await;

    let polytope_client = PolytopeClient::new(
        server_url,
        Some(AuthHeader::Basic(
            ALPHA_ADMIN_USER.to_string(),
            ALPHA_ADMIN_PASS.to_string(),
        )),
        None,
        Some(0.01),
        Some(1.05),
        Some(5.0),
    )
    .expect("client created");

    let retrieve_error = match polytope_client
        .retrieve("all", serde_json::json!({"class": "od"}))
        .await
    {
        Ok(_) => panic!("retrieve should fail on non-routable test redirect host"),
        Err(err) => err,
    };
    let redirect_url = retrieve_error
        .downcast::<reqwest::Error>()
        .ok()
        .and_then(|err| err.url().cloned())
        .expect("retrieve error contains redirect url");

    let key = redirect_url
        .path()
        .split('/')
        .next_back()
        .expect("redirect key");
    let bobs_read_resp = client
        .get(format!("{bobs_url}/api/v1/read/{key}"))
        .send()
        .await
        .expect("bobs read succeeds");
    assert_eq!(bobs_read_resp.status(), StatusCode::OK);
    let body = bobs_read_resp.bytes().await.expect("read bytes");
    let size = body.len() as u64;

    assert!(size > 0, "should have received data through BOBS pipeline");

    worker_handle.abort();
    server.abort();
    authotron.abort();
    bobs_handle.abort();
}

#[tokio::test]
async fn unauthenticated_rejected() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), false)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
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
    let (server_url, server) = spawn_polytope_server(
        Some(&authotron_url),
        &fallback_bits_yaml(&backend_url),
        false,
    )
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
    let (server_url, server) = spawn_polytope_server(
        Some(&authotron_url),
        &fallback_bits_yaml(&backend_url),
        false,
    )
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
    let (server_url, server) = spawn_polytope_server(
        Some(&authotron_url),
        &fallback_bits_yaml(&backend_url),
        false,
    )
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
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url), false)
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
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url), false)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let auth = AuthHeader::Basic(
        ALPHA_REGULAR_USER.to_string(),
        ALPHA_REGULAR_PASS.to_string(),
    );
    let auth_value: String = auth.into();

    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
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
async fn strict_no_roles_user_rejected() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url), false)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let auth = AuthHeader::Basic(
        ALPHA_REGULAR_USER.to_string(),
        ALPHA_REGULAR_PASS.to_string(),
    );
    let auth_value: String = auth.into();

    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
        .header("Authorization", auth_value)
        .json(&serde_json::json!({"class": "od"}))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(
        res.status(),
        StatusCode::BAD_REQUEST,
        "user with no roles should be rejected by has_role check"
    );

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn strict_wrong_realm_rejected() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url), false)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let auth = AuthHeader::Basic(BETA_ADMIN_USER.to_string(), BETA_ADMIN_PASS.to_string());
    let auth_value: String = auth.into();

    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
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
async fn anonymous_mode_unauthenticated_submit_succeeds() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), true)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client");
    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
        .json(&serde_json::json!({"class": "od"}))
        .send()
        .await
        .expect("request succeeds");
    assert!(matches!(
        res.status(),
        StatusCode::OK | StatusCode::SEE_OTHER
    ));

    server.abort();
    authotron.abort();
    backend.abort();
}

#[tokio::test]
async fn anonymous_mode_role_split_routes_to_public() {
    let (auth_backend_url, auth_backend, auth_hits) = spawn_mock_backend_counted()
        .await
        .expect("spawn auth backend");
    let (public_backend_url, public_backend, public_hits) = spawn_mock_backend_counted()
        .await
        .expect("spawn public backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) = spawn_polytope_server(
        Some(&authotron_url),
        &auth_split_yaml(&auth_backend_url, &public_backend_url),
        true,
    )
    .await
    .expect("spawn polytope server");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client");
    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
        .json(&serde_json::json!({"class": "od"}))
        .send()
        .await
        .expect("request succeeds");
    assert!(matches!(
        res.status(),
        StatusCode::OK | StatusCode::SEE_OTHER
    ));
    assert_eq!(
        auth_hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "anonymous request should not reach the authenticated backend"
    );
    assert!(
        public_hits.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "anonymous request should be routed to the public backend"
    );

    server.abort();
    authotron.abort();
    public_backend.abort();
    auth_backend.abort();
}

#[tokio::test]
async fn anonymous_mode_strict_route_rejects_at_routing() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &strict_bits_yaml(&backend_url), true)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client");
    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
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
async fn anonymous_mode_invalid_token_still_401() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), true)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client");
    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
        .header("Authorization", "Bearer invalid-token")
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
async fn anonymous_mode_empty_auth_header_401() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), true)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client");
    let res = client
        .post(format!("{server_url}/api/v2/all/requests"))
        .header("Authorization", "")
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
async fn default_mode_test_endpoint_accessible_without_auth() {
    let (backend_url, backend) = spawn_mock_backend().await.expect("spawn backend");
    let (authotron_url, authotron) = spawn_authotron(JWT_SECRET).await.expect("spawn authotron");
    let (server_url, server) =
        spawn_polytope_server(Some(&authotron_url), &simple_bits_yaml(&backend_url), false)
            .await
            .expect("spawn polytope server");

    let client = reqwest::Client::new();
    let res = client
        .get(format!("{server_url}/api/v1/test"))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "/api/v1/test should be accessible without auth even in default mode"
    );

    server.abort();
    authotron.abort();
    backend.abort();
}
