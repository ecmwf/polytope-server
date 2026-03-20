use std::collections::{HashMap, HashSet};

use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::Deserialize;

use super::types::{AuthError, User};

#[derive(Debug, Deserialize)]
struct Claims {
    username: String,
    realm: String,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    attributes: HashMap<String, serde_json::Value>,
    #[allow(dead_code)]
    exp: usize,
}

pub fn decode_jwt(token: &str, secret: &[u8]) -> Result<User, AuthError> {
    let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_aud = false;

    let token_data = decode::<Claims>(token, &DecodingKey::from_secret(secret), &validation)
        .map_err(|e| AuthError::InvalidJwt {
            message: e.to_string(),
        })?;

    let claims = token_data.claims;

    let mut roles_set: HashSet<String> = claims.roles.into_iter().collect();
    roles_set.insert("default".to_string());
    let roles: Vec<String> = roles_set.into_iter().collect();

    Ok(User {
        username: claims.username,
        realm: claims.realm,
        roles,
        attributes: claims.attributes,
    })
}

pub fn convert_email_key(auth_header: &str) -> String {
    if auth_header.starts_with("EmailKey ") {
        if let Some(colon_pos) = auth_header.find(':') {
            let part_after_colon = &auth_header[colon_pos + 1..];
            return format!("Bearer {}", part_after_colon);
        }
    }
    auth_header.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header};
    use serde::Serialize;

    #[derive(Debug, Serialize)]
    struct TestClaims {
        username: String,
        realm: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        roles: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        attributes: Option<HashMap<String, serde_json::Value>>,
        exp: usize,
    }

    fn make_token(claims: &TestClaims, secret: &[u8]) -> String {
        jsonwebtoken::encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    #[test]
    fn test_decode_valid_jwt() {
        let secret = b"my_test_secret";
        let exp = chrono::Utc::now().timestamp() as usize + 3600;
        let claims = TestClaims {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: Some(vec!["admin".to_string()]),
            attributes: Some(HashMap::from([(
                "org".to_string(),
                serde_json::json!("ecmwf"),
            )])),
            exp,
        };
        let token = make_token(&claims, secret);
        let user = decode_jwt(&token, secret).unwrap();

        assert_eq!(user.username, "testuser");
        assert_eq!(user.realm, "testrealm");
        assert!(user.roles.contains(&"admin".to_string()));
        assert!(user.roles.contains(&"default".to_string()));
        assert_eq!(user.attributes["org"], serde_json::json!("ecmwf"));
    }

    #[test]
    fn test_decode_jwt_no_roles() {
        let secret = b"my_test_secret";
        let exp = chrono::Utc::now().timestamp() as usize + 3600;
        let claims = TestClaims {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: None,
            attributes: None,
            exp,
        };
        let token = make_token(&claims, secret);
        let user = decode_jwt(&token, secret).unwrap();

        assert_eq!(user.roles, vec!["default".to_string()]);
    }

    #[test]
    fn test_decode_jwt_default_role_dedup() {
        let secret = b"my_test_secret";
        let exp = chrono::Utc::now().timestamp() as usize + 3600;
        let claims = TestClaims {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: Some(vec!["default".to_string(), "admin".to_string()]),
            attributes: None,
            exp,
        };
        let token = make_token(&claims, secret);
        let user = decode_jwt(&token, secret).unwrap();

        let default_count = user.roles.iter().filter(|r| *r == "default").count();
        assert_eq!(default_count, 1, "default role should appear exactly once");
        assert!(user.roles.contains(&"admin".to_string()));
    }

    #[test]
    fn test_decode_wrong_secret() {
        let exp = chrono::Utc::now().timestamp() as usize + 3600;
        let claims = TestClaims {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: None,
            attributes: None,
            exp,
        };
        let token = make_token(&claims, b"secret_a");
        let result = decode_jwt(&token, b"secret_b");

        assert!(matches!(result, Err(AuthError::InvalidJwt { .. })));
    }

    #[test]
    fn test_decode_expired_jwt() {
        let secret = b"my_test_secret";
        let exp = chrono::Utc::now().timestamp() as usize - 3600;
        let claims = TestClaims {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: None,
            attributes: None,
            exp,
        };
        let token = make_token(&claims, secret);
        let result = decode_jwt(&token, secret);

        assert!(matches!(result, Err(AuthError::InvalidJwt { .. })));
    }

    #[test]
    fn test_decode_missing_username() {
        #[derive(Serialize)]
        struct NoUsernameClaims {
            realm: String,
            exp: usize,
        }
        let secret = b"my_test_secret";
        let exp = chrono::Utc::now().timestamp() as usize + 3600;
        let claims = NoUsernameClaims {
            realm: "testrealm".to_string(),
            exp,
        };
        let token = jsonwebtoken::encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap();
        let result = decode_jwt(&token, secret);

        assert!(matches!(result, Err(AuthError::InvalidJwt { .. })));
    }

    #[test]
    fn test_email_key_conversion() {
        let result = convert_email_key("EmailKey user@test.com:abc123");
        assert_eq!(result, "Bearer abc123");
    }

    #[test]
    fn test_email_key_passthrough() {
        let result = convert_email_key("Bearer xyz");
        assert_eq!(result, "Bearer xyz");
    }
}
