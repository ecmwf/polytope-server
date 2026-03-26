use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    pub url: String,
    #[serde(default)]
    pub secret: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    pub cache_ttl_secs: Option<u64>,
    pub cache_capacity: Option<u64>,
    #[serde(default)]
    pub allow_anonymous: bool,
}

impl AuthConfig {
    pub fn resolved_secret(&self) -> String {
        std::env::var("AUTH_SECRET")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.secret.clone())
    }
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("url", &self.url)
            .field("secret", &"[REDACTED]")
            .field("timeout_ms", &self.timeout_ms)
            .field("cache_ttl_secs", &self.cache_ttl_secs)
            .field("cache_capacity", &self.cache_capacity)
            .field("allow_anonymous", &self.allow_anonymous)
            .finish()
    }
}

fn default_timeout_ms() -> u64 {
    5000
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub server: HttpConfig,
    pub bits: serde_yaml::Value,
    pub edr: Option<serde_yaml::Value>,
    #[serde(default)]
    pub authentication: Option<AuthConfig>,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    3000
}

impl ServerConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(serde_yaml::from_str(&std::fs::read_to_string(path)?)?)
    }

    pub fn bits_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(&self.bits)
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_with_auth() {
        let yaml = r#"
server:
  host: "0.0.0.0"
  port: 3000
bits: {}
authentication:
  url: "http://auth-o-tron:8080"
  secret: "testsecret"
"#;
        let cfg: ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.url, "http://auth-o-tron:8080");
        assert_eq!(auth.secret, "testsecret");
        assert_eq!(auth.timeout_ms, 5000);
        assert!(!auth.allow_anonymous);
    }

    #[test]
    fn test_config_allow_anonymous() {
        let yaml = r#"
bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
  allow_anonymous: true
"#;
        let cfg: ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert!(auth.allow_anonymous);
    }

    #[test]
    fn test_config_without_auth() {
        let yaml = r#"
server:
  host: "0.0.0.0"
  port: 3000
bits: {}
"#;
        let cfg: ServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.authentication.is_none());
    }

    #[test]
    fn test_config_with_custom_timeout() {
        let yaml = r#"
server:
  host: "0.0.0.0"
  port: 3000
bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
  timeout_ms: 10000
"#;
        let cfg: ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.timeout_ms, 10000);
    }

    #[test]
    fn test_config_cache_defaults_to_none() {
        let yaml = r#"
bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
"#;
        let cfg: ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert!(auth.cache_ttl_secs.is_none());
        assert!(auth.cache_capacity.is_none());
    }

    #[test]
    fn test_config_with_cache_settings() {
        let yaml = r#"
bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
  cache_ttl_secs: 300
  cache_capacity: 50000
"#;
        let cfg: ServerConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.cache_ttl_secs, Some(300));
        assert_eq!(auth.cache_capacity, Some(50000));
    }
}
