use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;

use super::mock_roles::{
    MockRolesAudit, REQUEST_ID_HEADER, has_mock_roles_header, has_mock_user_header,
    parse_mock_roles_header, parse_mock_user_header,
};
use super::mock_time::{
    MOCK_TIME_HEADER, MockTimeAudit, has_mock_time_header, normalise_mocked_now,
    parse_mock_time_header,
};
use super::{AuthError, AuthUser, is_admin_bypass_user};
use crate::state::AppState;

#[derive(Clone, Debug, Default)]
pub struct SubmissionContext {
    pub headers: HeaderMap,
    pub auth_user: Option<AuthUser>,
    pub mock_roles_audit: Option<MockRolesAudit>,
    pub mock_time: Option<super::MockTime>,
    pub mock_time_audit: Option<MockTimeAudit>,
}

impl SubmissionContext {
    fn from_request(req: &Request<Body>) -> Self {
        Self {
            headers: req.headers().clone(),
            auth_user: req.extensions().get::<AuthUser>().cloned(),
            mock_roles_audit: req.extensions().get::<MockRolesAudit>().cloned(),
            mock_time: req.extensions().get::<super::MockTime>().cloned(),
            mock_time_audit: req.extensions().get::<MockTimeAudit>().cloned(),
        }
    }
}

tokio::task_local! {
    static CURRENT_SUBMISSION_CONTEXT: SubmissionContext;
}

pub async fn scope_submission_context<F>(context: SubmissionContext, future: F) -> F::Output
where
    F: Future,
{
    CURRENT_SUBMISSION_CONTEXT.scope(context, future).await
}

pub fn current_submission_context() -> Option<SubmissionContext> {
    CURRENT_SUBMISSION_CONTEXT.try_with(Clone::clone).ok()
}

fn log_auth_rejected(status: StatusCode, reason: &str) {
    tracing::warn!(
        "event.name" = "api.auth.rejected",
        outcome = "rejected",
        status = status.as_u16() as u64,
        reason,
        "authentication rejected"
    );
}

fn unauthorized(message: &str) -> Response {
    log_auth_rejected(StatusCode::UNAUTHORIZED, message);
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"error": message})),
    )
        .into_response()
}

async fn unauthorized_with_auth_discovery(
    auth_client: &crate::auth::AuthClient,
    message: &str,
) -> Response {
    match auth_client.authenticate("Bearer").await {
        Err(AuthError::Unauthorized {
            www_authenticate, ..
        }) => {
            log_auth_rejected(StatusCode::UNAUTHORIZED, message);
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, www_authenticate.as_str())],
                Json(json!({"error": message})),
            )
                .into_response()
        }
        Err(AuthError::ServiceUnavailable { message }) => {
            log_auth_rejected(StatusCode::SERVICE_UNAVAILABLE, "auth service unavailable");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": message})),
            )
                .into_response()
        }
        _ => unauthorized(message),
    }
}

fn bad_request(message: &str) -> Response {
    log_auth_rejected(StatusCode::BAD_REQUEST, message);
    (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
}

fn forbidden(message: &str) -> Response {
    log_auth_rejected(StatusCode::FORBIDDEN, message);
    (StatusCode::FORBIDDEN, Json(json!({"error": message}))).into_response()
}

async fn run_with_submission_context(req: Request<Body>, next: Next) -> Response {
    let context = SubmissionContext::from_request(&req);
    scope_submission_context(context, next.run(req)).await
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let mock_header_present = has_mock_roles_header(req.headers())
        || has_mock_time_header(req.headers())
        || has_mock_user_header(req.headers());
    let auth_client = match &state.auth_client {
        Some(client) => client,
        None => {
            if mock_header_present {
                return unauthorized("Polytope mock headers require authentication");
            }
            return run_with_submission_context(req, next).await;
        }
    };

    let prof_t0 = Instant::now();
    let prof_method = req.method().to_string();
    let prof_path = req.uri().path().to_string();
    let auth_header = match req.headers().get(header::AUTHORIZATION) {
        Some(value) => match value.to_str() {
            Ok(s) if !s.is_empty() => s.to_string(),
            Ok(_) => return unauthorized("empty Authorization header"),
            Err(_) => return unauthorized("invalid Authorization header encoding"),
        },
        None => {
            if mock_header_present {
                return unauthorized("Polytope mock headers require authentication");
            }
            if state.allow_anonymous {
                return run_with_submission_context(req, next).await;
            }
            if prof_path.starts_with("/mcp") {
                return unauthorized_with_auth_discovery(
                    auth_client,
                    "missing Authorization header",
                )
                .await;
            }
            return unauthorized("missing Authorization header");
        }
    };

    let prof_auth_started = Instant::now();
    let prof_auth_result = auth_client.authenticate(&auth_header).await;
    let prof_auth_ms = prof_auth_started.elapsed().as_millis() as u64;
    match prof_auth_result {
        Ok(user) => {
            tracing::debug!(
                username = user.username.as_str(),
                realm = user.realm.as_str(),
                "authenticated request"
            );

            let mock_roles = match parse_mock_roles_header(req.headers(), &state.admin_bypass_roles)
            {
                Ok(mock_roles) => mock_roles,
                Err(err) => return bad_request(&err.message()),
            };
            let mock_time = match parse_mock_time_header(req.headers()) {
                Ok(mock_time) => mock_time,
                Err(err) => return bad_request(&err.message()),
            };
            let mock_user = match parse_mock_user_header(req.headers()) {
                Ok(mock_user) => mock_user,
                Err(err) => return bad_request(&err.message()),
            };
            // A mock username must come with mock roles, so the synthetic
            // identity never silently inherits the admin's realm/roles.
            if mock_user.is_some() && mock_roles.is_none() {
                return bad_request(
                    "Polytope-Mock-User requires Polytope-Mock-Roles to set the mock identity's realm/roles",
                );
            }

            if (mock_roles.is_some() || mock_time.is_some() || mock_user.is_some())
                && !is_admin_bypass_user(&user, &state.admin_bypass_roles)
            {
                return forbidden("Polytope mock headers require a configured admin user");
            }

            let mut req = req;
            let request_id = req
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let path = req.uri().path().to_string();

            if let Some(mock_time) = mock_time {
                let mocked_now = normalise_mocked_now(mock_time.now);
                let audit = MockTimeAudit {
                    real_username: user.username.clone(),
                    real_realm: user.realm.clone(),
                    mocked_now,
                    path: path.clone(),
                    request_id: request_id.clone(),
                    header: MOCK_TIME_HEADER,
                };
                tracing::info!(
                    "event.name" = "api.auth.mock_accepted",
                    real_username = audit.real_username.as_str(),
                    real_realm = audit.real_realm.as_str(),
                    mocked_now = audit.mocked_now.as_str(),
                    path = audit.path.as_str(),
                    request_id = audit.request_id.as_deref(),
                    header = MOCK_TIME_HEADER,
                    "accepted mocked-time request"
                );
                req.extensions_mut().insert(mock_time);
                req.extensions_mut().insert(audit);
            }

            let effective_user = if mock_roles.is_some() || mock_user.is_some() {
                let mocked_username = mock_user.clone().unwrap_or_else(|| user.username.clone());
                let (mocked_realm, mocked_roles) = match &mock_roles {
                    Some(mock_roles) => (mock_roles.realm.clone(), mock_roles.roles.clone()),
                    None => (user.realm.clone(), user.roles.clone()),
                };
                let audit = MockRolesAudit {
                    real_username: user.username.clone(),
                    real_realm: user.realm.clone(),
                    mocked_realm: mocked_realm.clone(),
                    mocked_roles: mocked_roles.clone(),
                    path,
                    request_id,
                };
                tracing::info!(
                    "event.name" = "api.auth.mock_accepted",
                    real_username = audit.real_username.as_str(),
                    real_realm = audit.real_realm.as_str(),
                    mocked_username = mocked_username.as_str(),
                    mocked_realm = audit.mocked_realm.as_str(),
                    mocked_roles = ?audit.mocked_roles,
                    path = audit.path.as_str(),
                    request_id = audit.request_id.as_deref(),
                    "accepted mocked request"
                );
                req.extensions_mut().insert(audit);

                AuthUser {
                    version: user.version,
                    username: mocked_username,
                    realm: mocked_realm,
                    roles: mocked_roles,
                    attributes: HashMap::new(),
                    scopes: HashMap::new(),
                }
            } else {
                user
            };

            let support_realm = effective_user.realm.clone();
            req.extensions_mut().insert(effective_user);
            let mut prof_response = run_with_submission_context(req, next).await;
            if let Some(url) = state.support.resolve(Some(&support_realm)) {
                prof_response
                    .extensions_mut()
                    .insert(crate::support::SupportUrl(url.to_string()));
            }
            tracing::debug!(
                "event.name" = "api.request.profiled",
                method = %prof_method,
                path = %prof_path,
                auth_ms = prof_auth_ms,
                total_ms = prof_t0.elapsed().as_millis() as u64,
                "request profiled"
            );
            prof_response
        }
        Err(AuthError::Unauthorized {
            message,
            www_authenticate,
        }) => {
            log_auth_rejected(
                StatusCode::UNAUTHORIZED,
                "auth service rejected credentials",
            );
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, www_authenticate.as_str())],
                Json(json!({"error": message})),
            )
                .into_response()
        }
        Err(AuthError::InvalidJwt { message }) => {
            log_auth_rejected(StatusCode::UNAUTHORIZED, "invalid jwt");
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                Json(json!({"error": format!("invalid token: {}", message)})),
            )
                .into_response()
        }
        Err(AuthError::ServiceUnavailable { message }) => {
            log_auth_rejected(StatusCode::SERVICE_UNAVAILABLE, "auth service unavailable");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": message})),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::{collections::HashMap, time::Duration as StdDuration};

    use axum::{
        Router,
        body::Body,
        http::{HeaderValue, Request, StatusCode},
        middleware,
        routing::{get, post},
    };

    use http_body_util::BodyExt;
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::auth::AuthClient;
    use crate::state::AppState;

    fn test_bits() -> bits::Bits {
        use std::time::Duration;
        bits::Bits::from_router_for_tests(
            bits::routing::switch::Switch::new(vec![]),
            "test".to_string(),
            "http://localhost:0".to_string(),
            Duration::from_secs(1),
            None,
            None,
            Duration::from_secs(30),
        )
    }

    fn make_test_jwt(secret: &str) -> String {
        make_test_jwt_with(
            secret,
            "testuser",
            "testrealm",
            &["admin"],
            json!({}),
            json!({}),
        )
    }

    fn make_test_jwt_with(
        secret: &str,
        username: &str,
        realm: &str,
        roles: &[&str],
        attributes: Value,
        scopes: Value,
    ) -> String {
        let claims = json!({
            "version": 1,
            "username": username,
            "realm": realm,
            "roles": roles,
            "attributes": attributes,
            "scopes": scopes,
            "exp": (chrono::Utc::now().timestamp() as usize) + 3600,
        });
        jsonwebtoken::encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    async fn stub_handler() -> &'static str {
        "ok"
    }

    fn build_test_app(auth_client: Option<AuthClient>) -> Router {
        build_test_app_with_anon(auth_client, false)
    }

    fn build_test_app_with_anon(auth_client: Option<AuthClient>, allow_anonymous: bool) -> Router {
        build_test_app_with_anon_and_admin(auth_client, allow_anonymous, None)
    }

    fn build_test_app_with_anon_and_admin(
        auth_client: Option<AuthClient>,
        allow_anonymous: bool,
        admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
    ) -> Router {
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client,
            collections: HashMap::new(),
            allow_anonymous,
            admin_bypass_roles,
            support: Default::default(),
        });

        let v1 = Router::new()
            .route("/collections", get(stub_handler))
            .route("/requests", get(stub_handler))
            .route(
                "/requests/{id}",
                post(stub_handler).get(stub_handler).delete(stub_handler),
            )
            .route("/downloads/{id}", get(stub_handler));

        let v2_protected = Router::new()
            .route("/collections", get(stub_handler))
            .route("/{collection}/requests", post(stub_handler))
            .route("/requests/{id}", get(stub_handler).delete(stub_handler));

        let openmeteo = Router::new().route("/forecast", get(stub_handler));

        let protected = Router::new()
            .nest("/api/v1", v1)
            .nest("/api/v2", v2_protected)
            .nest("/openmeteo/v1", openmeteo)
            .route("/mcp", get(stub_handler).post(stub_handler))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                super::auth_middleware,
            ));

        Router::new()
            .route("/api/v1/test", get(stub_handler))
            .route("/api/v2/health", get(stub_handler))
            .merge(protected)
            .with_state(state)
    }

    async fn setup_auth_client(mock_server: &mockito::Server, secret: &str) -> AuthClient {
        AuthClient::new(
            &mock_server.url(),
            secret.as_bytes(),
            StdDuration::from_secs(5),
            None,
            None,
        )
    }

    // ── Public routes ─────────────────────────────────────────────────

    #[tokio::test]
    async fn health_accessible_without_auth() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app(Some(client));

        let resp = app
            .oneshot(Request::get("/api/v2/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_accessible_when_auth_disabled() {
        let app = build_test_app(None);

        let resp = app
            .oneshot(Request::get("/api/v2/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ── Default mode: auth required ──────────────────────────────────

    #[tokio::test]
    async fn protected_routes_require_auth() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app(Some(client));

        for (method, path) in [
            ("GET", "/api/v1/collections"),
            ("GET", "/api/v1/requests"),
            ("POST", "/api/v2/requests"),
            ("GET", "/api/v2/requests/abc"),
            ("DELETE", "/api/v2/requests/abc"),
            ("GET", "/openmeteo/v1/forecast"),
            ("POST", "/mcp"),
        ] {
            assert_401(app.clone(), method, path).await;
        }
    }

    #[tokio::test]
    async fn mcp_missing_auth_uses_authotron_discovery_challenge() {
        let mut server = mockito::Server::new_async().await;
        let challenge = r#"Bearer resource_metadata="https://polytope.example/.well-known/oauth-protected-resource/mcp""#;
        let _auth = server
            .mock("GET", "/authenticate")
            .match_header("authorization", "Bearer")
            .with_status(401)
            .with_header("WWW-Authenticate", challenge)
            .create_async()
            .await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app(Some(client));

        let resp = app
            .oneshot(Request::post("/mcp").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers()
                .get("www-authenticate")
                .and_then(|value| value.to_str().ok()),
            Some(challenge)
        );
    }

    #[tokio::test]
    async fn test_endpoint_accessible_without_auth() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app(Some(client));

        assert_not_401(app, "GET", "/api/v1/test").await;
    }

    // ── Anonymous mode: allow_anonymous = true ────────────────────────

    #[tokio::test]
    async fn anonymous_mode_passes_unauthenticated_requests() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app_with_anon(Some(client), true);

        for (method, path) in [
            ("GET", "/api/v1/collections"),
            ("GET", "/api/v1/requests"),
            ("POST", "/api/v2/requests"),
            ("GET", "/api/v2/requests/abc"),
            ("DELETE", "/api/v2/requests/abc"),
            ("GET", "/openmeteo/v1/forecast"),
            ("POST", "/mcp"),
        ] {
            assert_not_401(app.clone(), method, path).await;
        }
    }

    #[tokio::test]
    async fn anonymous_mode_non_utf8_auth_header_still_401() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app_with_anon(Some(client), true);

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", b"\xff\xfe".as_slice())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn anonymous_mode_empty_auth_header_still_401() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app_with_anon(Some(client), true);

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Auth disabled: all routes accessible ──────────────────────────

    async fn assert_not_401(app: Router, method: &str, path: &str) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("Content-Type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{} {} should not be 401",
            method,
            path
        );
    }

    async fn assert_401(app: Router, method: &str, path: &str) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("Content-Type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{} {} should be 401 without auth",
            method,
            path
        );
        assert!(
            resp.headers().get("WWW-Authenticate").is_some(),
            "{} {} missing WWW-Authenticate header",
            method,
            path
        );
    }

    #[tokio::test]
    async fn auth_disabled_all_routes_open() {
        let app = build_test_app(None);
        assert_not_401(app.clone(), "POST", "/api/v2/ecmwf/requests").await;
        assert_not_401(app.clone(), "GET", "/api/v1/test").await;
        assert_not_401(app, "GET", "/openmeteo/v1/forecast").await;
    }

    #[tokio::test]
    async fn auth_disabled_mock_header_is_unauthorized() {
        let app = build_test_app(None);
        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Polytope-Mock-Roles", "beta:viewer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Valid auth passes through ─────────────────────────────────────

    #[tokio::test]
    async fn empty_authorization_header_returns_401() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app(Some(client));

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "empty Authorization header");
    }

    #[tokio::test]
    async fn email_key_auth_passes_through() {
        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .match_header("Authorization", "Bearer abc123")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app(Some(client));

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "EmailKey user@test.com:abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");
    }

    #[tokio::test]
    async fn valid_auth_passes_through_to_handler() {
        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app(Some(client));

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "Bearer sometoken")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(401)
            .with_header("WWW-Authenticate", "Bearer")
            .create_async()
            .await;

        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app(Some(client));

        let resp = app
            .oneshot(
                Request::post("/api/v2/ecmwf/requests")
                    .header("Authorization", "Bearer badtoken")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── User is injected into request extensions ──────────────────────

    #[tokio::test]
    async fn user_available_in_extensions() {
        use axum::extract::Request as AxumRequest;

        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client: Some(client),
            collections: HashMap::new(),
            allow_anonymous: false,
            admin_bypass_roles: None,
            support: Default::default(),
        });

        async fn check_user(req: AxumRequest) -> StatusCode {
            let user = req.extensions().get::<crate::auth::AuthUser>();
            match user {
                Some(u) if u.username == "testuser" => StatusCode::OK,
                Some(_) => StatusCode::EXPECTATION_FAILED,
                None => StatusCode::EXPECTATION_FAILED,
            }
        }

        let mut protected = Router::new().route("/check", get(check_user));
        protected = protected.layer(middleware::from_fn_with_state(
            state.clone(),
            super::auth_middleware,
        ));

        let app = Router::new().merge(protected).with_state(state);

        let resp = app
            .oneshot(
                Request::get("/check")
                    .header("Authorization", "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authenticated_job_user_auth_contract_shape_is_canonical() {
        use axum::{Json, extract::Request as AxumRequest};

        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client: Some(client),
            collections: HashMap::new(),
            allow_anonymous: false,
            admin_bypass_roles: None,
            support: Default::default(),
        });

        async fn contract_payload(req: AxumRequest) -> Json<Value> {
            let user = req
                .extensions()
                .get::<crate::auth::AuthUser>()
                .expect("auth middleware should inject AuthUser");

            let mut user_context = serde_json::Map::new();
            user_context.insert("auth".to_string(), serde_json::to_value(user).unwrap());
            Json(Value::Object(user_context))
        }

        let mut protected = Router::new().route("/contract", get(contract_payload));
        protected = protected.layer(middleware::from_fn_with_state(
            state.clone(),
            super::auth_middleware,
        ));

        let app = Router::new().merge(protected).with_state(state);

        let resp = app
            .oneshot(
                Request::get("/contract")
                    .header("Authorization", "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let job_user: Value = serde_json::from_slice(&body).unwrap();

        assert!(
            job_user.get("auth").is_some(),
            "job.user must contain auth key"
        );
        assert!(
            job_user.get("authentication").is_none(),
            "job.user must not rename auth key"
        );

        let auth = job_user
            .get("auth")
            .and_then(Value::as_object)
            .expect("job.user.auth must be an object");

        let mut keys: Vec<&str> = auth.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "attributes",
                "realm",
                "roles",
                "scopes",
                "username",
                "version"
            ],
            "job.user.auth fields drifted from canonical contract"
        );

        assert_eq!(auth.get("version"), Some(&json!(1)));
        assert_eq!(auth.get("username"), Some(&json!("testuser")));
        assert_eq!(auth.get("realm"), Some(&json!("testrealm")));
        let roles = auth
            .get("roles")
            .and_then(Value::as_array)
            .expect("job.user.auth.roles must be an array");
        assert!(
            roles.iter().any(|r| r == "admin"),
            "job.user.auth.roles must contain admin role from token"
        );
        assert_eq!(auth.get("attributes"), Some(&json!({})));
        assert_eq!(auth.get("scopes"), Some(&json!({})));
    }

    #[tokio::test]
    async fn admin_mock_roles_injects_sanitized_effective_user_and_audit() {
        use axum::{Json, extract::Request as AxumRequest};

        let secret = "testsecret";
        let jwt = make_test_jwt_with(
            secret,
            "admin-user",
            "alpha",
            &["admin", "ops"],
            json!({"department": "secret"}),
            json!({"scope": ["admin-power"]}),
        );
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client: Some(client),
            collections: HashMap::new(),
            allow_anonymous: false,
            admin_bypass_roles: Some(HashMap::from([(
                "alpha".to_string(),
                vec!["admin".to_string()],
            )])),
            support: Default::default(),
        });

        async fn payload(req: AxumRequest) -> Json<Value> {
            let user = req
                .extensions()
                .get::<crate::auth::AuthUser>()
                .expect("effective AuthUser is inserted");
            let audit = req
                .extensions()
                .get::<crate::auth::MockRolesAudit>()
                .expect("mock audit is inserted");
            Json(json!({
                "user": user,
                "audit": {
                    "real_username": audit.real_username,
                    "real_realm": audit.real_realm,
                    "mocked_realm": audit.mocked_realm,
                    "mocked_roles": audit.mocked_roles,
                    "path": audit.path,
                    "request_id": audit.request_id,
                }
            }))
        }

        let protected =
            Router::new()
                .route("/check", get(payload))
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    super::auth_middleware,
                ));
        let app = Router::new().merge(protected).with_state(state);

        let resp = app
            .oneshot(
                Request::get("/check")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Roles", "beta:viewer,data")
                    .header("X-Request-Id", "request-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["user"]["version"], json!(1));
        assert_eq!(payload["user"]["username"], json!("admin-user"));
        assert_eq!(payload["user"]["realm"], json!("beta"));
        assert_eq!(payload["user"]["roles"], json!(["viewer", "data"]));
        assert_eq!(payload["user"]["attributes"], json!({}));
        assert_eq!(payload["user"]["scopes"], json!({}));
        assert_eq!(payload["audit"]["real_username"], json!("admin-user"));
        assert_eq!(payload["audit"]["real_realm"], json!("alpha"));
        assert_eq!(payload["audit"]["request_id"], json!("request-123"));
    }

    #[tokio::test]
    async fn admin_mock_time_injects_mock_time_and_audit() {
        use axum::{Json, extract::Request as AxumRequest};

        let secret = "testsecret";
        let jwt = make_test_jwt_with(
            secret,
            "admin-user",
            "alpha",
            &["admin", "ops"],
            json!({}),
            json!({}),
        );
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client: Some(client),
            collections: HashMap::new(),
            allow_anonymous: false,
            admin_bypass_roles: Some(HashMap::from([(
                "alpha".to_string(),
                vec!["admin".to_string()],
            )])),
            support: Default::default(),
        });

        async fn payload(req: AxumRequest) -> Json<Value> {
            let mock_time = req
                .extensions()
                .get::<crate::auth::MockTime>()
                .expect("mock time is inserted");
            let audit = req
                .extensions()
                .get::<crate::auth::MockTimeAudit>()
                .expect("mock time audit is inserted");
            Json(json!({
                "mocked_now": crate::auth::mock_time::normalise_mocked_now(mock_time.now),
                "audit": {
                    "real_username": audit.real_username,
                    "real_realm": audit.real_realm,
                    "mocked_now": audit.mocked_now,
                    "path": audit.path,
                    "request_id": audit.request_id,
                    "header": audit.header,
                }
            }))
        }

        let protected =
            Router::new()
                .route("/check", get(payload))
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    super::auth_middleware,
                ));
        let app = Router::new().merge(protected).with_state(state);

        let resp = app
            .oneshot(
                Request::get("/check")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Time", "2040-05-06T08:08:09+01:00")
                    .header("X-Request-Id", "request-456")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["mocked_now"], json!("2040-05-06T07:08:09Z"));
        assert_eq!(payload["audit"]["real_username"], json!("admin-user"));
        assert_eq!(payload["audit"]["real_realm"], json!("alpha"));
        assert_eq!(
            payload["audit"]["mocked_now"],
            json!("2040-05-06T07:08:09Z")
        );
        assert_eq!(payload["audit"]["path"], json!("/check"));
        assert_eq!(payload["audit"]["request_id"], json!("request-456"));
        assert_eq!(payload["audit"]["header"], json!("polytope-mock-time"));
    }

    #[tokio::test]
    async fn non_admin_mock_time_is_forbidden() {
        let secret = "testsecret";
        let jwt = make_test_jwt_with(
            secret,
            "regular",
            "alpha",
            &["viewer"],
            json!({}),
            json!({}),
        );
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;
        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app_with_anon_and_admin(
            Some(client),
            false,
            Some(HashMap::from([(
                "alpha".to_string(),
                vec!["admin".to_string()],
            )])),
        );

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Time", "2040-05-06T07:08:09Z")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn auth_disabled_mock_time_is_unauthorized() {
        let app = build_test_app(None);
        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Polytope-Mock-Time", "2040-05-06T07:08:09Z")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn anonymous_mode_mock_time_is_unauthorized() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app_with_anon(Some(client), true);

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Polytope-Mock-Time", "2040-05-06T07:08:09Z")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn malformed_mock_time_is_bad_request() {
        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;
        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app_with_anon_and_admin(
            Some(client),
            false,
            Some(HashMap::from([(
                "testrealm".to_string(),
                vec!["admin".to_string()],
            )])),
        );

        let mut multiple = Request::get("/api/v1/collections")
            .header("Authorization", "Bearer token")
            .header("Polytope-Mock-Time", "2040-05-06T07:08:09Z")
            .body(Body::empty())
            .unwrap();
        multiple.headers_mut().append(
            "Polytope-Mock-Time",
            HeaderValue::from_static("2040-05-06T07:09:09Z"),
        );

        let requests = vec![
            multiple,
            Request::get("/api/v1/collections")
                .header("Authorization", "Bearer token")
                .header(
                    "Polytope-Mock-Time",
                    HeaderValue::from_bytes(b"12:34:\xff").unwrap(),
                )
                .body(Body::empty())
                .unwrap(),
            Request::get("/api/v1/collections")
                .header("Authorization", "Bearer token")
                .header("Polytope-Mock-Time", "not a time")
                .body(Body::empty())
                .unwrap(),
        ];

        for request in requests {
            let resp = app.clone().oneshot(request).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn admin_mock_roles_and_mock_time_are_both_effective() {
        use axum::{Json, extract::Request as AxumRequest};

        let secret = "testsecret";
        let jwt = make_test_jwt_with(
            secret,
            "admin-user",
            "alpha",
            &["admin", "ops"],
            json!({"department": "secret"}),
            json!({"scope": ["admin-power"]}),
        );
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = setup_auth_client(&server, secret).await;
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client: Some(client),
            collections: HashMap::new(),
            allow_anonymous: false,
            admin_bypass_roles: Some(HashMap::from([(
                "alpha".to_string(),
                vec!["admin".to_string()],
            )])),
            support: Default::default(),
        });

        async fn payload(req: AxumRequest) -> Json<Value> {
            let user = req
                .extensions()
                .get::<crate::auth::AuthUser>()
                .expect("effective AuthUser is inserted");
            let roles_audit = req
                .extensions()
                .get::<crate::auth::MockRolesAudit>()
                .expect("mock roles audit is inserted");
            let mock_time = req
                .extensions()
                .get::<crate::auth::MockTime>()
                .expect("mock time is inserted");
            let time_audit = req
                .extensions()
                .get::<crate::auth::MockTimeAudit>()
                .expect("mock time audit is inserted");
            Json(json!({
                "user": user,
                "roles_audit": {
                    "real_username": roles_audit.real_username,
                    "real_realm": roles_audit.real_realm,
                    "mocked_realm": roles_audit.mocked_realm,
                    "mocked_roles": roles_audit.mocked_roles,
                },
                "mocked_now": crate::auth::mock_time::normalise_mocked_now(mock_time.now),
                "time_audit": {
                    "real_username": time_audit.real_username,
                    "real_realm": time_audit.real_realm,
                    "mocked_now": time_audit.mocked_now,
                    "path": time_audit.path,
                    "request_id": time_audit.request_id,
                    "header": time_audit.header,
                }
            }))
        }

        let protected =
            Router::new()
                .route("/check", get(payload))
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    super::auth_middleware,
                ));
        let app = Router::new().merge(protected).with_state(state);

        let resp = app
            .oneshot(
                Request::get("/check")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Roles", "beta:viewer,data")
                    .header("Polytope-Mock-Time", "2040-05-06T08:08:09+01:00")
                    .header("X-Request-Id", "request-789")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["user"]["realm"], json!("beta"));
        assert_eq!(payload["user"]["roles"], json!(["viewer", "data"]));
        assert_eq!(payload["roles_audit"]["real_username"], json!("admin-user"));
        assert_eq!(payload["roles_audit"]["real_realm"], json!("alpha"));
        assert_eq!(payload["roles_audit"]["mocked_realm"], json!("beta"));
        assert_eq!(payload["mocked_now"], json!("2040-05-06T07:08:09Z"));
        assert_eq!(payload["time_audit"]["header"], json!("polytope-mock-time"));
    }

    #[tokio::test]
    async fn malformed_mock_roles_wins_over_malformed_mock_time() {
        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;
        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app_with_anon_and_admin(
            Some(client),
            false,
            Some(HashMap::from([(
                "testrealm".to_string(),
                vec!["admin".to_string()],
            )])),
        );

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Roles", "beta:")
                    .header("Polytope-Mock-Time", "not a time")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"],
            json!("Polytope-Mock-Roles must include at least one role")
        );
    }

    #[tokio::test]
    async fn non_admin_mock_roles_is_forbidden() {
        let secret = "testsecret";
        let jwt = make_test_jwt_with(
            secret,
            "regular",
            "alpha",
            &["viewer"],
            json!({}),
            json!({}),
        );
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;
        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app_with_anon_and_admin(
            Some(client),
            false,
            Some(HashMap::from([(
                "alpha".to_string(),
                vec!["admin".to_string()],
            )])),
        );

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Roles", "beta:viewer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn malformed_mock_roles_is_bad_request() {
        let secret = "testsecret";
        let jwt = make_test_jwt(secret);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;
        let client = setup_auth_client(&server, secret).await;
        let app = build_test_app_with_anon_and_admin(
            Some(client),
            false,
            Some(HashMap::from([(
                "testrealm".to_string(),
                vec!["admin".to_string()],
            )])),
        );

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Authorization", "Bearer token")
                    .header("Polytope-Mock-Roles", "beta:")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn anonymous_mode_mock_header_is_unauthorized() {
        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let app = build_test_app_with_anon(Some(client), true);

        let resp = app
            .oneshot(
                Request::get("/api/v1/collections")
                    .header("Polytope-Mock-Roles", "beta:viewer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn user_not_in_extensions_when_anonymous() {
        use axum::extract::Request as AxumRequest;

        let server = mockito::Server::new_async().await;
        let client = setup_auth_client(&server, "secret").await;
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client: Some(client),
            collections: HashMap::new(),
            allow_anonymous: true,
            admin_bypass_roles: None,
            support: Default::default(),
        });

        async fn check_no_user(req: AxumRequest) -> StatusCode {
            if req.extensions().get::<crate::auth::AuthUser>().is_none() {
                StatusCode::OK
            } else {
                StatusCode::EXPECTATION_FAILED
            }
        }

        let mut protected = Router::new().route("/check", get(check_no_user));
        protected = protected.layer(middleware::from_fn_with_state(
            state.clone(),
            super::auth_middleware,
        ));

        let app = Router::new().merge(protected).with_state(state);

        let resp = app
            .oneshot(Request::get("/check").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "anonymous request should not have AuthUser in extensions"
        );
    }
}
