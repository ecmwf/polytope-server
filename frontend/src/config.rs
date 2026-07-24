// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use serde::de;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

pub const DEFAULT_AUTH_AUDIENCE: &str = "polytope-server";

#[derive(Debug, Deserialize, Clone)]
pub struct JwtPublicKeyConfig {
    pub kid: String,
    pub public_key: String,
}

#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    pub url: String,
    /// Exact JWT issuer expected from auth-o-tron.
    pub issuer: String,
    /// Exact JWT audience expected from auth-o-tron.
    #[serde(default = "default_auth_audience")]
    pub audience: String,
    /// Overlapping public keys accepted by `kid` during signing-key rotation.
    pub public_keys: Vec<JwtPublicKeyConfig>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    pub cache_ttl_secs: Option<u64>,
    pub cache_capacity: Option<u64>,
    #[serde(default)]
    pub allow_anonymous: bool,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let public_key_ids: Vec<&str> = self
            .public_keys
            .iter()
            .map(|key| key.kid.as_str())
            .collect();
        f.debug_struct("AuthConfig")
            .field("url", &self.url)
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("public_key_ids", &public_key_ids)
            .field("timeout_ms", &self.timeout_ms)
            .field("cache_ttl_secs", &self.cache_ttl_secs)
            .field("cache_capacity", &self.cache_capacity)
            .field("allow_anonymous", &self.allow_anonymous)
            .finish()
    }
}

fn default_auth_audience() -> String {
    DEFAULT_AUTH_AUDIENCE.to_string()
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
    pub mcp: Option<McpConfig>,
    pub authentication: Option<AuthConfig>,
    pub admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
    pub metrics: Option<MetricsConfig>,
    pub support: SupportConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
    #[serde(default)]
    pub catalogue_url: Option<String>,
    #[serde(default = "default_mcp_inline_result_max_bytes")]
    pub inline_result_max_bytes: usize,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            catalogue_url: None,
            inline_result_max_bytes: default_mcp_inline_result_max_bytes(),
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
        }
    }
}

fn default_mcp_inline_result_max_bytes() -> usize {
    65_536
}

/// Where users are told to raise a support ticket when an error reaches them.
/// `default_url` is the deployment-level contact (used for every error, including
/// pre-auth ones where no realm is known). `realms` optionally overrides the
/// contact for authenticated users of a given auth realm, since a single
/// deployment can serve multiple communities (e.g. `ecmwf` and `desp`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SupportConfig {
    #[serde(default)]
    pub default_url: Option<String>,
    #[serde(default)]
    pub realms: HashMap<String, String>,
}

impl SupportConfig {
    /// Resolve the support URL for an error: the authenticated user's realm
    /// override when known and mapped, otherwise the deployment default.
    pub fn resolve(&self, realm: Option<&str>) -> Option<&str> {
        realm
            .and_then(|r| self.realms.get(r))
            .map(String::as_str)
            .or(self.default_url.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    /// Custom histogram bucket boundaries (in seconds) for job-duration metrics
    /// (`polytope.broker.request.duration.seconds` and the per-collection
    /// variant). Falls back to [`MetricsConfig::resolved_duration_buckets`]
    /// defaults when absent.
    #[serde(default)]
    pub duration_buckets: Option<Vec<f64>>,
    /// Custom histogram bucket boundaries (in seconds) for the dispatcher
    /// queue-wait metric (`polytope.broker.dispatcher.queue_wait.seconds`).
    /// Falls back to [`MetricsConfig::resolved_queue_wait_buckets`] defaults
    /// when absent.
    #[serde(default)]
    pub queue_wait_buckets: Option<Vec<f64>>,
}

fn default_metrics_port() -> u16 {
    9090
}

/// Validates a user-supplied histogram bucket list:
/// - must not be empty
/// - all values must be finite (no NaN or ±∞)
/// - all values must be ≥ 0
/// - values must be strictly increasing (duplicates are rejected)
fn validate_buckets(field: &str, buckets: &[f64]) -> Result<(), String> {
    if buckets.is_empty() {
        return Err(format!("{field}: must not be empty"));
    }
    let mut prev: Option<f64> = None;
    for &b in buckets {
        if !b.is_finite() {
            return Err(format!("{field}: must be finite (no NaN or infinity)"));
        }
        if b < 0.0 {
            return Err(format!("{field}: must not be negative"));
        }
        if let Some(p) = prev
            && b <= p
        {
            return Err(format!("{field}: must be strictly increasing"));
        }
        prev = Some(b);
    }
    Ok(())
}

impl MetricsConfig {
    /// Validates any user-supplied bucket lists.  Called by
    /// [`ServerConfig::from_file`] at startup; can also be called in tests.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(ref b) = self.duration_buckets {
            validate_buckets("metrics.duration_buckets", b)?;
        }
        if let Some(ref b) = self.queue_wait_buckets {
            validate_buckets("metrics.queue_wait_buckets", b)?;
        }
        Ok(())
    }

    /// Returns the configured duration-histogram bucket boundaries, or the
    /// built-in defaults if none were specified in config.
    pub fn resolved_duration_buckets(&self) -> Vec<f64> {
        self.duration_buckets.clone().unwrap_or_else(|| {
            vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0, 120.0,
            ]
        })
    }

    /// Returns the configured queue-wait-histogram bucket boundaries, or the
    /// built-in defaults if none were specified in config.
    pub fn resolved_queue_wait_buckets(&self) -> Vec<f64> {
        self.queue_wait_buckets.clone().unwrap_or_else(|| {
            vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]
        })
    }
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
            mcp: Option<McpConfig>,
            #[serde(default)]
            authentication: Option<AuthConfig>,
            #[serde(default)]
            admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
            #[serde(default)]
            metrics: Option<MetricsConfig>,
            #[serde(default)]
            support: SupportConfig,
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
            mcp: raw.mcp,
            authentication: raw.authentication,
            admin_bypass_roles: raw.admin_bypass_roles,
            metrics: raw.metrics,
            support: raw.support,
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

    bits::request_id::pack_tag(&tag)
        .map(|_| tag)
        .map_err(|err| format!("{field}: {err}"))
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub internal_poll_port: Option<u16>,
    #[serde(default = "default_completed_redirect_ttl_secs")]
    pub completed_redirect_ttl_secs: u64,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            internal_poll_port: None,
            completed_redirect_ttl_secs: default_completed_redirect_ttl_secs(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    3000
}

const fn default_completed_redirect_ttl_secs() -> u64 {
    600
}

impl ServerConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let cfg: Self = serde_yaml::from_str(&std::fs::read_to_string(path)?)?;
        if cfg.server.completed_redirect_ttl_secs == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "server.completed_redirect_ttl_secs must be greater than 0",
            )
            .into());
        }
        if let Some(ref m) = cfg.metrics {
            m.validate()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        }
        Ok(cfg)
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

        // A `worker_server` block means this broker dispatches to remote-pool
        // workers, which must send completion/heartbeat callbacks back to THIS
        // specific broker instance. bits derives that direct callback address
        // from `advertised_addr`, which we populate from POD_IP. If POD_IP is
        // missing, bits' `callback_url` is `None` and workers silently fall back
        // to the load-balanced broker URL — completion callbacks then land on a
        // random broker, are dropped, and the job is stranded in-progress with
        // an un-reclaimable durable record. Fail loud rather than silently
        // misroute (a silent fallback previously masked exactly this bug).
        let worker_server_key = serde_yaml::Value::String("worker_server".to_string());
        if inner.contains_key(&worker_server_key) {
            let pod_ip = std::env::var("POD_IP").unwrap_or_default();
            let pod_ip = pod_ip.trim();
            if pod_ip.is_empty() {
                return Err(<serde_yaml::Error as serde::ser::Error>::custom(
                    "bits.worker_server is configured but POD_IP is empty or unset: cannot \
                     advertise a direct worker-callback address. Refusing to start to avoid \
                     load-balanced callback misrouting that strands jobs. Ensure the frontend \
                     pod sets POD_IP from status.podIP (downward API).",
                ));
            }
            let worker_server = inner
                .get_mut(&worker_server_key)
                .and_then(serde_yaml::Value::as_mapping_mut)
                .ok_or_else(|| {
                    <serde_yaml::Error as serde::ser::Error>::custom(
                        "bits.worker_server must be a mapping",
                    )
                })?;
            let port = worker_server
                .get(serde_yaml::Value::String("port".to_string()))
                .and_then(serde_yaml::Value::as_u64)
                .and_then(|port| u16::try_from(port).ok())
                .unwrap_or(9001);
            let advertised_addr = if pod_ip.contains(':') && !pod_ip.starts_with('[') {
                format!("[{pod_ip}]:{port}")
            } else {
                format!("{pod_ip}:{port}")
            };
            worker_server.insert(
                serde_yaml::Value::String("advertised_addr".to_string()),
                serde_yaml::Value::String(advertised_addr),
            );
        }

        serde_yaml::to_string(&bits)
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    pub fn internal_poll_bind_addr(&self) -> Option<String> {
        self.server
            .internal_poll_port
            .map(|port| format!("{}:{}", self.server.host, port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn set_pod_ip(value: &str) {
        unsafe { std::env::set_var("POD_IP", value) };
    }

    fn remove_pod_ip() {
        unsafe { std::env::remove_var("POD_IP") };
    }

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
    fn bits_yaml_injects_worker_server_advertised_addr_from_pod_ip() {
        let _guard = env_lock();
        set_pod_ip("10.1.2.3");

        let yaml = config_with_polytope(
            r#"bits:
  bits:
    worker_server:
      host: "0.0.0.0"
      port: 9001
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let bits_yaml = cfg.bits_yaml().unwrap();
        remove_pod_ip();

        let bits_cfg: serde_yaml::Value = serde_yaml::from_str(&bits_yaml).unwrap();
        let worker_server = bits_cfg
            .get("bits")
            .and_then(|bits| bits.get("worker_server"))
            .expect("inner worker_server should exist");
        assert_eq!(
            worker_server
                .get("advertised_addr")
                .and_then(|v| v.as_str()),
            Some("10.1.2.3:9001")
        );
    }

    #[test]
    fn bits_yaml_errors_when_worker_server_present_but_pod_ip_missing() {
        let _guard = env_lock();
        remove_pod_ip();

        let yaml = config_with_polytope(
            r#"bits:
  bits:
    worker_server:
      host: "0.0.0.0"
      port: 9001
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let err = cfg
            .bits_yaml()
            .expect_err("worker_server without POD_IP must hard-fail");
        assert!(
            err.to_string().contains("POD_IP"),
            "error should mention POD_IP: {err}"
        );
    }

    #[test]
    fn bits_yaml_does_not_create_worker_server_for_pod_ip() {
        let _guard = env_lock();
        set_pod_ip("10.1.2.3");

        let yaml = config_with_polytope("bits: {}\n");
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let bits_yaml = cfg.bits_yaml().unwrap();
        remove_pod_ip();

        let bits_cfg: serde_yaml::Value = serde_yaml::from_str(&bits_yaml).unwrap();
        assert!(
            bits_cfg
                .get("bits")
                .and_then(|bits| bits.get("worker_server"))
                .is_none()
        );
    }

    #[test]
    fn test_config_with_auth_keyset_and_default_audience() {
        let yaml = config_with_polytope(
            r#"server:
  host: "0.0.0.0"
  port: 3000
bits: {}
authentication:
  url: "http://auth-o-tron:8080"
  issuer: "https://auth-o-tron.example.com"
  public_keys:
    - kid: "key-2026-01"
      public_key: "first-public-key"
    - kid: "key-2026-02"
      public_key: "second-public-key"
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.url, "http://auth-o-tron:8080");
        assert_eq!(auth.issuer, "https://auth-o-tron.example.com");
        assert_eq!(auth.audience, DEFAULT_AUTH_AUDIENCE);
        assert_eq!(auth.public_keys.len(), 2);
        assert_eq!(auth.public_keys[0].kid, "key-2026-01");
        assert_eq!(auth.public_keys[1].kid, "key-2026-02");
        assert_eq!(auth.timeout_ms, 5000);
        assert!(!auth.allow_anonymous);
    }

    #[test]
    fn internal_poll_config_parses_port_and_bind_addr() {
        let yaml = config_with_polytope(
            r#"server:
  host: "127.0.0.1"
  port: 3000
  internal_poll_port: 9002
bits: {}
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(cfg.server.internal_poll_port, Some(9002));
        assert_eq!(cfg.bind_addr(), "127.0.0.1:3000");
        assert_eq!(
            cfg.internal_poll_bind_addr().as_deref(),
            Some("127.0.0.1:9002")
        );
    }

    #[test]
    fn completed_redirect_ttl_defaults_to_ten_minutes() {
        let yaml = config_with_polytope("bits: {}\n");
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(cfg.server.completed_redirect_ttl_secs, 600);
    }

    #[test]
    fn completed_redirect_ttl_parses_override() {
        let yaml =
            config_with_polytope("server:\n  completed_redirect_ttl_secs: 21600\nbits: {}\n");
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(cfg.server.completed_redirect_ttl_secs, 21_600);
    }

    #[test]
    fn test_config_allow_anonymous() {
        let yaml = config_with_polytope(
            r#"bits: {}
authentication:
  url: "http://auth:8080"
  issuer: "https://auth.example"
  public_keys:
    - kid: "test"
      public_key: "not-used"
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
    fn mcp_config_parses_when_present() {
        let yaml = config_with_polytope(
            r#"bits: {}
mcp:
  catalogue_url: "https://catalogue.example/"
  inline_result_max_bytes: 1234
  allowed_hosts:
    - polytope.example
  allowed_origins:
    - https://client.example
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let mcp = cfg.mcp.unwrap();
        assert_eq!(
            mcp.catalogue_url.as_deref(),
            Some("https://catalogue.example/")
        );
        assert_eq!(mcp.inline_result_max_bytes, 1234);
        assert_eq!(mcp.allowed_hosts, vec!["polytope.example"]);
        assert_eq!(mcp.allowed_origins, vec!["https://client.example"]);
    }

    #[test]
    fn mcp_config_defaults_inline_limit() {
        let yaml = config_with_polytope("bits: {}\nmcp: {}\n");
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let mcp = cfg.mcp.unwrap();
        assert_eq!(mcp.inline_result_max_bytes, 65_536);
        assert!(mcp.catalogue_url.is_none());
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
  issuer: "https://auth.example"
  public_keys:
    - kid: "test"
      public_key: "not-used"
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
  issuer: "https://auth.example"
  public_keys:
    - kid: "test"
      public_key: "not-used"
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
  issuer: "https://auth.example"
  audience: "custom-audience"
  public_keys:
    - kid: "test"
      public_key: "not-used"
  cache_ttl_secs: 300
  cache_capacity: 50000
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let auth = cfg.authentication.unwrap();
        assert_eq!(auth.cache_ttl_secs, Some(300));
        assert_eq!(auth.cache_capacity, Some(50000));
    }

    #[test]
    fn legacy_shared_secret_config_is_rejected() {
        let yaml = config_with_polytope(
            r#"bits: {}
authentication:
  url: "http://auth:8080"
  secret: "legacy-shared-secret"
"#,
        );
        let error = serde_yaml::from_str::<ServerConfig>(&yaml)
            .expect_err("RS256 issuer and public keyset must be required");
        assert!(
            error.to_string().contains("issuer") || error.to_string().contains("public_keys"),
            "error should identify the missing RS256 contract: {error}"
        );
    }

    #[test]
    fn metrics_config_parses_when_present() {
        let yaml = config_with_polytope(
            r#"bits: {}
metrics:
  enabled: true
  port: 9090
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let m = cfg.metrics.unwrap();
        assert!(m.enabled);
        assert_eq!(m.port, 9090);
    }

    #[test]
    fn metrics_config_optional() {
        let yaml = config_with_polytope("bits: {}\n");
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(cfg.metrics.is_none());
    }

    #[test]
    fn metrics_config_defaults() {
        let yaml = config_with_polytope(
            r#"bits: {}
metrics:
  enabled: true
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let m = cfg.metrics.unwrap();
        assert!(m.enabled);
        assert_eq!(m.port, 9090);
    }

    #[test]
    fn metrics_custom_buckets_parse() {
        let yaml = config_with_polytope(
            r#"bits: {}
metrics:
  enabled: true
  duration_buckets: [0.1, 0.5, 1.0, 5.0]
  queue_wait_buckets: [0.01, 0.1, 1.0]
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let m = cfg.metrics.unwrap();
        assert_eq!(m.resolved_duration_buckets(), vec![0.1, 0.5, 1.0, 5.0]);
        assert_eq!(m.resolved_queue_wait_buckets(), vec![0.01, 0.1, 1.0]);
        assert!(m.validate().is_ok());
    }

    #[test]
    fn metrics_default_buckets_when_omitted() {
        let yaml = config_with_polytope("bits: {}\nmetrics:\n  enabled: true\n");
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let m = cfg.metrics.unwrap();
        let dur = m.resolved_duration_buckets();
        assert!(!dur.is_empty());
        assert!(
            dur.windows(2).all(|w| w[0] < w[1]),
            "default duration buckets must be strictly increasing"
        );
        let qw = m.resolved_queue_wait_buckets();
        assert!(!qw.is_empty());
        assert!(
            qw.windows(2).all(|w| w[0] < w[1]),
            "default queue_wait buckets must be strictly increasing"
        );
    }

    #[test]
    fn metrics_partial_buckets_other_uses_default() {
        let yaml = config_with_polytope(
            r#"bits: {}
metrics:
  duration_buckets: [1.0, 2.0, 5.0]
"#,
        );
        let cfg: ServerConfig = serde_yaml::from_str(&yaml).unwrap();
        let m = cfg.metrics.unwrap();
        assert_eq!(m.resolved_duration_buckets(), vec![1.0, 2.0, 5.0]);
        // queue_wait not set — falls back to non-empty default
        assert!(!m.resolved_queue_wait_buckets().is_empty());
    }

    #[test]
    fn metrics_invalid_buckets_rejected() {
        // empty
        let m = MetricsConfig {
            duration_buckets: Some(vec![]),
            ..MetricsConfig::default()
        };
        assert!(m.validate().is_err(), "empty list should fail");

        // not strictly increasing
        let m = MetricsConfig {
            duration_buckets: Some(vec![2.0, 1.0]),
            ..MetricsConfig::default()
        };
        assert!(m.validate().is_err(), "decreasing values should fail");

        // duplicate values
        let m = MetricsConfig {
            duration_buckets: Some(vec![1.0, 1.0, 2.0]),
            ..MetricsConfig::default()
        };
        assert!(m.validate().is_err(), "duplicate values should fail");

        // negative
        let m = MetricsConfig {
            queue_wait_buckets: Some(vec![-0.1, 1.0]),
            ..MetricsConfig::default()
        };
        assert!(m.validate().is_err(), "negative values should fail");

        // non-finite
        let m = MetricsConfig {
            duration_buckets: Some(vec![f64::INFINITY]),
            ..MetricsConfig::default()
        };
        assert!(m.validate().is_err(), "infinity should fail");

        let m = MetricsConfig {
            duration_buckets: Some(vec![f64::NAN]),
            ..MetricsConfig::default()
        };
        assert!(m.validate().is_err(), "NaN should fail");
    }
}
