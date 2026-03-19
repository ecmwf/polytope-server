use async_trait::async_trait;

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
        DeliveryType::Bobs => Box::new(BobsPush {
            bobs_url: config
                .bobs_url
                .clone()
                .expect("bobs_url required for delivery_type=bobs"),
            client,
        }),
        DeliveryType::S3 => {
            let mut s3_builder = aws_sdk_s3::config::Builder::new()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
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

            let s3_config = s3_builder.build();

            Box::new(S3Push {
                bucket: config
                    .s3_bucket
                    .clone()
                    .expect("s3_bucket required for delivery_type=s3"),
                key_prefix: config.s3_key_prefix.clone(),
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
