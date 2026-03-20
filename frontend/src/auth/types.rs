use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub realm: String,
    pub roles: Vec<String>,
    #[serde(default)]
    pub attributes: HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub enum AuthError {
    Unauthorized {
        message: String,
        www_authenticate: String,
    },
    InvalidJwt {
        message: String,
    },
    ServiceUnavailable {
        message: String,
    },
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::Unauthorized { message, .. } => write!(f, "Unauthorized: {}", message),
            AuthError::InvalidJwt { message } => write!(f, "Invalid JWT: {}", message),
            AuthError::ServiceUnavailable { message } => {
                write!(f, "Service unavailable: {}", message)
            }
        }
    }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_serde_roundtrip() {
        let user = User {
            username: "testuser".to_string(),
            realm: "testrealm".to_string(),
            roles: vec!["admin".to_string(), "default".to_string()],
            attributes: HashMap::from([("org".to_string(), serde_json::json!("ecmwf"))]),
        };
        let json = serde_json::to_string(&user).unwrap();
        let deserialized: User = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.username, "testuser");
        assert_eq!(deserialized.realm, "testrealm");
        assert_eq!(deserialized.roles.len(), 2);
        assert!(deserialized.roles.contains(&"admin".to_string()));
        assert!(deserialized.roles.contains(&"default".to_string()));
        assert_eq!(deserialized.attributes["org"], serde_json::json!("ecmwf"));
    }

    #[test]
    fn test_user_default_attributes() {
        let json = r#"{"username":"u","realm":"r","roles":[]}"#;
        let user: User = serde_json::from_str(json).unwrap();
        assert!(user.attributes.is_empty());
    }
}
