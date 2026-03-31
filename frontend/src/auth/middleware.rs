use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;

use super::AuthError;
use crate::state::AppState;

fn unauthorized(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"error": message})),
    )
        .into_response()
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let auth_client = match &state.auth_client {
        Some(client) => client,
        None => return next.run(req).await,
    };

    let auth_header = match req.headers().get(header::AUTHORIZATION) {
        Some(value) => match value.to_str() {
            Ok(s) if !s.is_empty() => s.to_string(),
            Ok(_) => return unauthorized("empty Authorization header"),
            Err(_) => return unauthorized("invalid Authorization header encoding"),
        },
        None => {
            if state.allow_anonymous {
                return next.run(req).await;
            }
            return unauthorized("missing Authorization header");
        }
    };

    match auth_client.authenticate(&auth_header).await {
        Ok(user) => {
            tracing::debug!(
                username = user.username.as_str(),
                realm = user.realm.as_str(),
                "authenticated request"
            );
            let mut req = req;
            req.extensions_mut().insert(user);
            next.run(req).await
        }
        Err(AuthError::Unauthorized {
            message,
            www_authenticate,
        }) => (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, www_authenticate.as_str())],
            Json(json!({"error": message})),
        )
            .into_response(),
        Err(AuthError::InvalidJwt { message }) => (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(json!({"error": format!("invalid token: {}", message)})),
        )
            .into_response(),
        Err(AuthError::ServiceUnavailable { message }) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": message})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::{collections::HashMap, time::Duration as StdDuration};

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        middleware,
        routing::{get, post},
    };

    use http_body_util::BodyExt;
    use jsonwebtoken::{EncodingKey, Header};
    use serde::Serialize;
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

    #[derive(Serialize)]
    struct TestClaims {
        username: String,
        realm: String,
        roles: Vec<String>,
        exp: usize,
    }

    fn make_test_jwt(secret: &str) -> String {
        let claims = TestClaims {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: vec!["admin".to_string()],
            exp: (chrono::Utc::now().timestamp() as usize) + 3600,
        };
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
        let state = Arc::new(AppState {
            bits: test_bits(),
            auth_client,
            collections: HashMap::new(),
            allow_anonymous,
            admin_bypass_roles: None,
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

        let mut protected = Router::new()
            .nest("/api/v1", v1)
            .nest("/api/v2", v2_protected)
            .nest("/openmeteo/v1", openmeteo);

        if state.auth_client.is_some() {
            protected = protected.layer(middleware::from_fn_with_state(
                state.clone(),
                super::auth_middleware,
            ));
        }

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
        ] {
            assert_401(app.clone(), method, path).await;
        }
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
