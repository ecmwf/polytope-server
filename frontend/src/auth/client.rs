use std::time::Duration;

use moka::future::Cache;
use reqwest::header::{self, HeaderValue};
use reqwest::Client;

use super::jwt::{convert_email_key, decode_jwt};
use super::types::{AuthError, User};
use crate::config::AuthConfig;

pub struct AuthClient {
    http: Client,
    url: String,
    secret: Vec<u8>,
    cache: Cache<String, User>,
}

impl AuthClient {
    pub fn new(config: &AuthConfig) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .expect("failed to build HTTP client");

        let cache = Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(60))
            .build();

        let secret = config.resolved_secret();
        Self {
            http,
            url: config.url.clone(),
            secret: secret.as_bytes().to_vec(),
            cache,
        }
    }

    pub async fn authenticate(&self, auth_header: &str) -> Result<User, AuthError> {
        let converted = convert_email_key(auth_header);

        if let Some(user) = self.cache.get(&converted).await {
            return Ok(user);
        }

        let response = self
            .http
            .get(format!("{}/authenticate", self.url.trim_end_matches('/')))
            .header("Authorization", &converted)
            .send()
            .await
            .map_err(|e| AuthError::ServiceUnavailable {
                message: format!("auth service error: {}", e),
            })?;

        if !response.status().is_success() {
            let www_auth = response
                .headers()
                .get(header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("Bearer")
                .to_string();
            return Err(AuthError::Unauthorized {
                message: "authentication failed".to_string(),
                www_authenticate: www_auth,
            });
        }

        let raw_header: &HeaderValue = response
            .headers()
            .get(header::AUTHORIZATION)
            .ok_or_else(|| AuthError::InvalidJwt {
                message: "missing Authorization header in auth-o-tron response".to_string(),
            })?;

        let auth_response_header = raw_header
            .to_str()
            .map_err(|_| AuthError::InvalidJwt {
                message: "non-UTF-8 Authorization header in auth-o-tron response".to_string(),
            })?
            .to_string();

        let jwt_token = auth_response_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| AuthError::InvalidJwt {
                message: "Authorization header is not Bearer scheme".to_string(),
            })?;

        let user = decode_jwt(jwt_token, &self.secret)?;

        self.cache.insert(converted, user.clone()).await;

        Ok(user)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header};
    use serde::Serialize;

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

    fn test_config(url: &str) -> AuthConfig {
        AuthConfig {
            url: url.to_string(),
            secret: "testsecret".to_string(),
            timeout_ms: 5000,
        }
    }

    #[tokio::test]
    async fn test_successful_auth() {
        let mut server = mockito::Server::new_async().await;
        let jwt = make_test_jwt("testsecret");

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .create_async()
            .await;

        let client = AuthClient::new(&test_config(&server.url()));
        let user = client.authenticate("Bearer sometoken").await.unwrap();

        assert_eq!(user.username, "testuser");
        assert_eq!(user.realm, "testrealm");
        assert!(user.roles.contains(&"admin".to_string()));
        assert!(user.roles.contains(&"default".to_string()));
    }

    #[tokio::test]
    async fn test_auth_failure_401() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(401)
            .with_header("WWW-Authenticate", r#"Bearer realm="test""#)
            .create_async()
            .await;

        let client = AuthClient::new(&test_config(&server.url()));
        let result = client.authenticate("Bearer badtoken").await;

        match result {
            Err(AuthError::Unauthorized { www_authenticate, .. }) => {
                assert_eq!(www_authenticate, r#"Bearer realm="test""#);
            }
            other => panic!("expected Unauthorized, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let mut server = mockito::Server::new_async().await;
        let jwt = make_test_jwt("testsecret");

        let mock = server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", jwt))
            .expect(1)
            .create_async()
            .await;

        let client = AuthClient::new(&test_config(&server.url()));

        let user1 = client.authenticate("Bearer cachedtoken").await.unwrap();
        let user2 = client.authenticate("Bearer cachedtoken").await.unwrap();

        assert_eq!(user1.username, user2.username);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_auth_service_unreachable() {
        let config = AuthConfig {
            url: "http://127.0.0.1:1".to_string(),
            secret: "testsecret".to_string(),
            timeout_ms: 1000,
        };
        let client = AuthClient::new(&config);
        let result = client.authenticate("Bearer token").await;

        assert!(matches!(result, Err(AuthError::ServiceUnavailable { .. })));
    }

    #[tokio::test]
    async fn test_missing_authorization_in_response() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .create_async()
            .await;

        let client = AuthClient::new(&test_config(&server.url()));
        let result = client.authenticate("Bearer sometoken").await;

        match result {
            Err(AuthError::InvalidJwt { message }) => {
                assert!(
                    message.contains("missing"),
                    "expected 'missing' in message, got: {}",
                    message
                );
            }
            other => panic!("expected InvalidJwt, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_non_bearer_authorization_in_response() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", "Basic dXNlcjpwYXNz")
            .create_async()
            .await;

        let client = AuthClient::new(&test_config(&server.url()));
        let result = client.authenticate("Bearer sometoken").await;

        match result {
            Err(AuthError::InvalidJwt { message }) => {
                assert!(
                    message.contains("not Bearer"),
                    "expected 'not Bearer' in message, got: {}",
                    message
                );
            }
            other => panic!("expected InvalidJwt, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_invalid_jwt_response() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("GET", "/authenticate")
            .with_status(200)
            .with_header("Authorization", "Bearer not_a_valid_jwt")
            .create_async()
            .await;

        let client = AuthClient::new(&test_config(&server.url()));
        let result = client.authenticate("Bearer sometoken").await;

        assert!(matches!(result, Err(AuthError::InvalidJwt { .. })));
    }
}
