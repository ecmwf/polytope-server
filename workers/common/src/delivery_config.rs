use serde::{Deserialize, Serialize};

/// How the worker should deliver its result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryType {
    /// Stream result directly through the broker (default behavior).
    Direct,
    /// Push to BOBS and return a redirect URL to the reader endpoint.
    Bobs,
    /// Upload to S3 and return a redirect URL to a presigned GET URL.
    S3,
}

/// Per-worker-pool delivery configuration, loaded from a YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    pub delivery_type: DeliveryType,

    /// BOBS service URL. Required when delivery_type = bobs.
    pub bobs_url: Option<String>,

    /// S3 bucket name. Required when delivery_type = s3.
    pub s3_bucket: Option<String>,

    /// AWS region. Required when delivery_type = s3.
    #[serde(default)]
    pub s3_region: Option<String>,

    /// Object storage endpoint URL. Required when delivery_type = s3 for self-hosted S3.
    #[serde(default)]
    pub s3_endpoint_url: Option<String>,

    /// Use path-style S3 URLs. Set to true for self-hosted S3 (MinIO, Ceph, etc.)
    #[serde(default)]
    pub s3_force_path_style: Option<bool>,

    #[serde(default)]
    pub s3_access_key_id: Option<String>,

    #[serde(default)]
    pub s3_secret_access_key: Option<String>,

    #[serde(default)]
    pub s3_presigned_url_expiry_secs: Option<u64>,

    #[serde(default)]
    pub s3_public_url: Option<String>,

    /// S3 key prefix (optional). Defaults to empty string.
    #[serde(default)]
    pub s3_key_prefix: String,

    /// Encoding codec to apply before delivery.
    #[serde(default)]
    pub encoding: Codec,

    /// Skip compression for payloads smaller than this many bytes.
    #[serde(default = "default_encoding_threshold")]
    pub encoding_threshold_bytes: usize,
}

/// Content encoding codec.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    #[default]
    Identity,
    Zstd,
    Gzip,
}

impl Codec {
    /// Returns the HTTP Content-Encoding header value for this codec.
    pub fn content_encoding_header(&self) -> Option<&'static str> {
        match self {
            Codec::Identity => None,
            Codec::Zstd => Some("zstd"),
            Codec::Gzip => Some("gzip"),
        }
    }
}

fn default_encoding_threshold() -> usize {
    1024
}

impl DeliveryConfig {
    pub fn from_file(path: &str) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        serde_yml::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_config_deserializes_from_yaml() {
        let yaml = r#"
delivery_type: bobs
bobs_url: "http://bobs.example.com"
s3_bucket: null
s3_region: null
encoding: gzip
encoding_threshold_bytes: 4096
"#;
        let config: DeliveryConfig = serde_yml::from_str(yaml).unwrap();
        assert!(matches!(config.delivery_type, DeliveryType::Bobs));
        assert_eq!(config.bobs_url.as_deref(), Some("http://bobs.example.com"));
        assert_eq!(config.encoding, Codec::Gzip);
        assert_eq!(config.encoding_threshold_bytes, 4096);
        assert_eq!(config.s3_key_prefix, "");
    }

    #[test]
    fn delivery_config_defaults() {
        let yaml = "delivery_type: direct\n";
        let config: DeliveryConfig = serde_yml::from_str(yaml).unwrap();
        assert!(matches!(config.delivery_type, DeliveryType::Direct));
        assert_eq!(config.encoding, Codec::Identity);
        assert_eq!(config.encoding_threshold_bytes, 1024);
        assert_eq!(config.s3_key_prefix, "");
    }

    #[test]
    fn delivery_config_deserializes_self_hosted_s3_fields() {
        let yaml = r#"
delivery_type: s3
s3_bucket: "example-bucket"
s3_region: "us-east-1"
s3_endpoint_url: "http://minio:9000"
s3_force_path_style: true
"#;
        let config: DeliveryConfig = serde_yml::from_str(yaml).unwrap();

        assert!(matches!(config.delivery_type, DeliveryType::S3));
        assert_eq!(config.s3_bucket.as_deref(), Some("example-bucket"));
        assert_eq!(config.s3_region.as_deref(), Some("us-east-1"));
        assert_eq!(config.s3_endpoint_url.as_deref(), Some("http://minio:9000"));
        assert_eq!(config.s3_force_path_style, Some(true));
    }

    #[test]
    fn delivery_config_deserializes_s3_credentials_and_url() {
        let yaml = r#"
delivery_type: s3
s3_bucket: "example-bucket"
s3_region: "us-east-1"
s3_access_key_id: "my-access-key"
s3_secret_access_key: "my-secret-key"
s3_presigned_url_expiry_secs: 3600
s3_public_url: "https://cdn.example.com"
"#;
        let config: DeliveryConfig = serde_yml::from_str(yaml).unwrap();

        assert_eq!(config.s3_access_key_id.as_deref(), Some("my-access-key"));
        assert_eq!(
            config.s3_secret_access_key.as_deref(),
            Some("my-secret-key")
        );
        assert_eq!(config.s3_presigned_url_expiry_secs, Some(3600));
        assert_eq!(
            config.s3_public_url.as_deref(),
            Some("https://cdn.example.com")
        );
    }

    #[test]
    fn codec_content_encoding_header() {
        assert_eq!(Codec::Identity.content_encoding_header(), None);
        assert_eq!(Codec::Zstd.content_encoding_header(), Some("zstd"));
        assert_eq!(Codec::Gzip.content_encoding_header(), Some("gzip"));
    }
}
