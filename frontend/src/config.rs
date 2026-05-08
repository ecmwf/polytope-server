use serde::de;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

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

#[derive(Debug)]
pub struct ServerConfig {
    pub polytope: PolytopeConfig,
    pub server: HttpConfig,
    pub bits: serde_yaml::Value,
    pub edr: Option<serde_yaml::Value>,
    pub authentication: Option<AuthConfig>,
    pub admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone)]
pub struct PolytopeConfig {
    pub site: String,
    pub env: String,
}

impl<'de> Deserialize<'de> for ServerConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawServerConfig {
            #[serde(default)]
            server: HttpConfig,
            polytope: Option<PolytopeConfig>,
            bits: serde_yaml::Value,
            #[serde(default)]
            edr: Option<serde_yaml::Value>,
            #[serde(default)]
            authentication: Option<AuthConfig>,
            #[serde(default)]
            admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
        }

        let raw = RawServerConfig::deserialize(deserializer)?;
        let polytope = raw
            .polytope
            .ok_or_else(|| de::Error::missing_field("polytope.site/polytope.env"))?;

        Ok(Self {
            polytope,
            server: raw.server,
            bits: raw.bits,
            edr: raw.edr,
            authentication: raw.authentication,
            admin_bypass_roles: raw.admin_bypass_roles,
        })
    }
}

impl<'de> Deserialize<'de> for PolytopeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawPolytopeConfig {
            site: Option<serde_yaml::Value>,
            env: Option<serde_yaml::Value>,
        }

        let raw = RawPolytopeConfig::deserialize(deserializer)?;
        Ok(Self {
            site: require_site_env_tag("polytope.site", raw.site.as_ref())
                .map_err(de::Error::custom)?,
            env: require_site_env_tag("polytope.env", raw.env.as_ref())
                .map_err(de::Error::custom)?,
        })
    }
}

fn require_site_env_tag(field: &str, value: Option<&serde_yaml::Value>) -> Result<String, String> {
    let value = value.ok_or_else(|| format!("missing field `{field}`"))?;
    let tag = match value {
        serde_yaml::Value::String(tag) => tag.clone(),
        serde_yaml::Value::Number(number) if number.as_u64().is_some() => number.to_string(),
        _ => return Err(format!("{field} must be a string or unsigned integer tag")),
    };

    bits::polytope_id::pack_tag(&tag)
        .map(|_| tag)
        .map_err(|err| format!("{field}: {err}"))
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
        let mut bits = self.bits.clone();
        let outer = bits.as_mapping_mut().ok_or_else(|| {
            <serde_yaml::Error as serde::ser::Error>::custom("bits must be a mapping")
        })?;

        // The chart's `bits:` block is the top-level YAML consumed by
        // `bits::parse_bootstrap`. The inner `bits.bits.*` mapping holds the
        // broker-only `BitsConfig` (persistence, slot, site/env, ...). Inject
        // site/env there, creating the inner mapping if absent.
        let bits_key = serde_yaml::Value::String("bits".to_string());
        if !outer.contains_key(&bits_key) {
            outer.insert(
                bits_key.clone(),
                serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
            );
        }
        let inner = outer
            .get_mut(&bits_key)
            .and_then(serde_yaml::Value::as_mapping_mut)
            .ok_or_else(|| {
                <serde_yaml::Error as serde::ser::Error>::custom("bits.bits must be a mapping")
            })?;

        inner.insert(
            serde_yaml::Value::String("site".to_string()),
            serde_yaml::Value::String(self.polytope.site.clone()),
        );
        inner.insert(
            serde_yaml::Value::String("env".to_string()),
            serde_yaml::Value::String(self.polytope.env.clone()),
        );

        serde_yaml::to_string(&bits)
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_polytope(body: &str) -> String {
        format!(
            r#"
polytope:
  site: bol
  env: dev
{body}"#
        )
    }

    #[test]
    fn request_id_config_requires_polytope_site_and_env() {
        let missing_polytope = r#"
bits:
  site: bol
  env: dev
"#;
        let err = serde_yaml::from_str::<ServerConfig>(missing_polytope)
            .expect_err("polytope.site and polytope.env should be required");
        assert!(
            err.to_string().contains("polytope.site") || err.to_string().contains("polytope.env"),
            "error should identify missing polytope site/env: {err}"
        );
    }

    #[test]
    fn request_id_config_rejects_invalid_site_env() {
        for (field, value) in [("site", "abcd"), ("env", "DEV"), ("site", "a-b")] {
            let (site, env) = if field == "site" {
                (value, "dev")
            } else {
                ("bol", value)
            };
            let yaml = format!(
                r#"
polytope:
  site: {site}
  env: {env}
bits:
  site: bol
  env: dev
"#
            );
            let err = serde_yaml::from_str::<ServerConfig>(&yaml)
                .expect_err("invalid polytope site/env should be rejected");
            assert!(
                err.to_string().contains(&format!("polytope.{field}")),
                "error should identify invalid polytope.{field}: {err}"
            );
        }
    }

    #[test]
    fn request_id_config_injects_bits_site_env() {
        let yaml = r#"
polytope:
  site: bol
  env: dev
bits: {}
"#;
        let cfg: ServerConfig =
            serde_yaml::from_str(yaml).expect("top-level polytope site/env config should parse");
        let bits_yaml = cfg
            .bits_yaml()
            .expect("injected BITS YAML should serialize successfully");
        let bits_cfg: serde_yaml::Value =
            serde_yaml::from_str(&bits_yaml).expect("injected BITS YAML should parse successfully");

        // bits_yaml() returns the chart's outer `bits:` block, in which
        // `bits.bits.*` is the BitsConfig consumed by parse_bootstrap. site/env
        // must land in that inner block.
        let inner = bits_cfg
            .get("bits")
            .expect("injected YAML should contain inner bits BitsConfig block");
        assert_eq!(inner.get("site").and_then(|v| v.as_str()), Some("bol"));
        assert_eq!(inner.get("env").and_then(|v| v.as_str()), Some("dev"));
    }

    #[test]
    fn test_config_with_auth() {
        let yaml = config_with_polytope(
            r#"server:
  host: "0.0.0.0"
  port: 3000
bits: {}
authentication:
  url: "http://auth-o-tron:8080"
  secret: "testsecret"
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.url, "http://auth-o-tron:8080");
        assert_eq!(auth.secret, "testsecret");
        assert_eq!(auth.timeout_ms, 5000);
        assert!(!auth.allow_anonymous);
    }

    #[test]
    fn test_config_allow_anonymous() {
        let yaml = config_with_polytope(
            r#"bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
  allow_anonymous: true
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert!(auth.allow_anonymous);
    }

    #[test]
    fn test_config_without_auth() {
        let yaml = config_with_polytope(
            r#"server:
  host: "0.0.0.0"
  port: 3000
bits: {}
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(cfg.authentication.is_none());
    }

    #[test]
    fn test_config_with_custom_timeout() {
        let yaml = config_with_polytope(
            r#"server:
  host: "0.0.0.0"
  port: 3000
bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
  timeout_ms: 10000
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.timeout_ms, 10000);
    }

    #[test]
    fn test_config_cache_defaults_to_none() {
        let yaml = config_with_polytope(
            r#"bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert!(auth.cache_ttl_secs.is_none());
        assert!(auth.cache_capacity.is_none());
    }

    #[test]
    fn test_config_with_cache_settings() {
        let yaml = config_with_polytope(
            r#"bits: {}
authentication:
  url: "http://auth:8080"
  secret: "s"
  cache_ttl_secs: 300
  cache_capacity: 50000
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.cache_ttl_secs, Some(300));
        assert_eq!(auth.cache_capacity, Some(50000));
    }
}
