use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub url: String,
    pub secret: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
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
}
