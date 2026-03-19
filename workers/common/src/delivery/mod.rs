use async_trait::async_trait;
use aws_config::BehaviorVersion;

use crate::delivery_config::{DeliveryConfig, DeliveryType};
use crate::Completion;

mod bobs;
mod s3;

use bobs::BobsPush;
use s3::S3Push;

#[async_trait]
pub trait ResultDelivery: Send + Sync {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
    ) -> Completion;
}

pub async fn make_delivery(
    config: &DeliveryConfig,
    client: reqwest::Client,
) -> Box<dyn ResultDelivery> {
    match config.delivery_type {
        DeliveryType::Direct => Box::new(DirectDelivery),
        DeliveryType::Bobs => {
            let host = config
                .bobs_url
                .as_deref()
                .expect("bobs_url required for delivery_type=bobs")
                .trim_start_matches("http://")
                .trim_start_matches("https://");
            let api_base = format!("http://{host}");
            Box::new(BobsPush { api_base, client })
        }
        DeliveryType::S3 => {
            let shared_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let mut s3_builder = aws_sdk_s3::config::Builder::from(&shared_config)
                .region(aws_sdk_s3::config::Region::new(
                    config
                        .s3_region
                        .as_deref()
                        .unwrap_or("us-east-1")
                        .to_string(),
                ));

            if let Some(endpoint_url) = config.s3_endpoint_url.as_deref() {
                s3_builder = s3_builder.endpoint_url(endpoint_url);
            }

            if matches!(config.s3_force_path_style, Some(true)) {
                s3_builder = s3_builder.force_path_style(true);
            }

            if let (Some(key_id), Some(secret)) = (
                config.s3_access_key_id.as_deref(),
                config.s3_secret_access_key.as_deref(),
            ) {
                s3_builder = s3_builder.credentials_provider(aws_sdk_s3::config::Credentials::new(
                    key_id, secret, None, None, "config",
                ));
            }

            let s3_config = s3_builder.build();

            Box::new(S3Push {
                bucket: config
                    .s3_bucket
                    .clone()
                    .expect("s3_bucket required for delivery_type=s3"),
                key_prefix: config.s3_key_prefix.clone(),
                presigned_url_expiry_secs: config.s3_presigned_url_expiry_secs.unwrap_or(86400).min(604800),
                public_url: config.s3_public_url.clone(),
                s3_client: aws_sdk_s3::Client::from_conf(s3_config),
            })
        }
    }
}

struct DirectDelivery;

#[async_trait]
impl ResultDelivery for DirectDelivery {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
    ) -> Completion {
        Completion::Complete {
            content_type: content_type.to_string(),
            content_encoding: content_encoding.map(str::to_string),
            content_length: None,
            body,
        }
    }
}
