// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use aws_config::BehaviorVersion;

use crate::delivery_config::{DeliveryConfig, DeliveryType};
use crate::{Completion, SourceError};

#[derive(Debug, Clone)]
pub struct DeliveryContext<'a> {
    pub job_id: &'a str,
    pub user: &'a serde_json::Value,
    pub source_error: Option<SourceError>,
}

impl DeliveryContext<'_> {
    pub fn source_error_message(&self) -> Option<String> {
        self.source_error.as_ref().and_then(SourceError::message)
    }
}

pub(crate) fn enduser_fields(user: &serde_json::Value) -> (Option<&str>, Option<&str>) {
    (
        user.get("auth")
            .and_then(|auth| auth.get("username"))
            .and_then(|value| value.as_str()),
        user.get("auth")
            .and_then(|auth| auth.get("realm"))
            .and_then(|value| value.as_str()),
    )
}

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
        buffered_length: Option<u64>,
        metadata: &serde_json::Value,
        context: DeliveryContext<'_>,
    ) -> Completion;
}

pub async fn make_delivery(config: &DeliveryConfig) -> Box<dyn ResultDelivery> {
    let configured: Box<dyn ResultDelivery> = match &config.delivery_type {
        DeliveryType::Direct => Box::new(DirectDelivery),
        DeliveryType::Bobs => {
            let host = config
                .bobs_url
                .as_deref()
                .expect("bobs_url required for delivery_type=bobs")
                .trim_start_matches("http://")
                .trim_start_matches("https://");
            let api_base = format!("http://{host}/api/v1");
            // create_client deliberately keeps NO idle connections
            // (pool_max_idle_per_host(0)). Each create opens a fresh connection to
            // the BOBS *service*, so kube-proxy re-load-balances it across BOBS
            // pods and spools spread evenly. A warm pooled h2 connection would pin
            // ALL of a worker's creates to one BOBS pod, concentrating load and
            // memory (and OOM risk) on that pod. The body write/complete then follow
            // the per-pod write_url returned by create, so body_client correctly
            // tracks whichever pod owns each spool.
            let create_client = reqwest::Client::builder()
                .http2_prior_knowledge()
                .pool_max_idle_per_host(0)
                .build()
                .expect("build BOBS create_client");
            let body_client = reqwest::Client::builder()
                .http2_prior_knowledge()
                .build()
                .expect("build BOBS body_client");
            Box::new(BobsPush {
                api_base,
                create_client,
                body_client,
                early_release: config.bobs_early_release,
            })
        }
        DeliveryType::S3 => {
            let shared_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let mut s3_builder = aws_sdk_s3::config::Builder::from(&shared_config).region(
                aws_sdk_s3::config::Region::new(
                    config
                        .s3_region
                        .as_deref()
                        .unwrap_or("us-east-1")
                        .to_string(),
                ),
            );

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
                presigned_url_expiry_secs: config
                    .s3_presigned_url_expiry_secs
                    .unwrap_or(86400)
                    .min(604800),
                public_url: config.s3_public_url.clone(),
                s3_client: aws_sdk_s3::Client::from_conf(s3_config),
            })
        }
    };

    if config.inline_max_bytes == 0 || matches!(&config.delivery_type, DeliveryType::Direct) {
        configured
    } else {
        Box::new(SizeGatedDelivery {
            inline_max_bytes: config.inline_max_bytes,
            fallback: configured,
        })
    }
}

struct SizeGatedDelivery {
    inline_max_bytes: u64,
    fallback: Box<dyn ResultDelivery>,
}

impl SizeGatedDelivery {
    fn should_inline(
        &self,
        content_type: &str,
        buffered_length: Option<u64>,
        metadata: &serde_json::Value,
    ) -> bool {
        let media_type = content_type.split(';').next().unwrap_or_default().trim();
        self.inline_max_bytes != 0
            && buffered_length.is_some_and(|length| length <= self.inline_max_bytes)
            && (media_type.eq_ignore_ascii_case("application/octet-stream")
                || media_type.eq_ignore_ascii_case("application/x-polytope-multichunk"))
            && metadata
                .get("buffer_full_output")
                .and_then(|value| value.as_bool())
                != Some(true)
    }
}

#[async_trait]
impl ResultDelivery for SizeGatedDelivery {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
        buffered_length: Option<u64>,
        metadata: &serde_json::Value,
        context: DeliveryContext<'_>,
    ) -> Completion {
        if self.should_inline(content_type, buffered_length, metadata) {
            tracing::info!(
                "event.name" = "worker.delivery.selected",
                outcome = "success",
                delivery = "inline",
                request.id = %context.job_id,
                payload_bytes = buffered_length.expect("inline delivery requires a length"),
                inline_max_bytes = self.inline_max_bytes,
                "selected inline result delivery"
            );
            DirectDelivery
                .deliver(
                    content_type,
                    content_encoding,
                    body,
                    buffered_length,
                    metadata,
                    context,
                )
                .await
        } else {
            self.fallback
                .deliver(
                    content_type,
                    content_encoding,
                    body,
                    buffered_length,
                    metadata,
                    context,
                )
                .await
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
        buffered_length: Option<u64>,
        metadata: &serde_json::Value,
        context: DeliveryContext<'_>,
    ) -> Completion {
        if metadata.get("buffer_full_output").and_then(|v| v.as_bool()) == Some(true) {
            return Completion::Error {
                message: "buffer_full_output requires BOBS delivery; direct streaming does not support it".to_string(),
            };
        }
        Completion::Complete {
            content_type: content_type.to_string(),
            content_encoding: content_encoding.map(str::to_string),
            content_length: buffered_length,
            body,
            source_error: context.source_error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct CountingFallback {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ResultDelivery for CountingFallback {
        async fn deliver(
            &self,
            _content_type: &str,
            _content_encoding: Option<&str>,
            _body: reqwest::Body,
            _buffered_length: Option<u64>,
            _metadata: &serde_json::Value,
            _context: DeliveryContext<'_>,
        ) -> Completion {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Completion::Error {
                message: "fallback called".to_string(),
            }
        }
    }

    fn delivery(limit: u64, calls: &Arc<AtomicUsize>) -> SizeGatedDelivery {
        SizeGatedDelivery {
            inline_max_bytes: limit,
            fallback: Box::new(CountingFallback {
                calls: Arc::clone(calls),
            }),
        }
    }

    async fn deliver(
        delivery: &SizeGatedDelivery,
        content_type: &str,
        length: Option<u64>,
    ) -> Completion {
        let user = serde_json::json!({});
        let metadata = serde_json::json!({});
        delivery
            .deliver(
                content_type,
                None,
                reqwest::Body::from("payload"),
                length,
                &metadata,
                DeliveryContext {
                    job_id: "job-1",
                    user: &user,
                    source_error: None,
                },
            )
            .await
    }

    #[tokio::test]
    async fn small_octet_stream_is_inline_without_fallback_calls() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = deliver(
            &delivery(128 * 1024, &calls),
            "application/octet-stream",
            Some(63 * 1024),
        )
        .await;

        assert!(matches!(result, Completion::Complete { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn octet_stream_at_threshold_is_inline() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = deliver(
            &delivery(128, &calls),
            "application/octet-stream",
            Some(128),
        )
        .await;

        assert!(matches!(result, Completion::Complete { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn octet_stream_above_threshold_uses_configured_delivery() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = deliver(
            &delivery(128, &calls),
            "application/octet-stream",
            Some(129),
        )
        .await;

        assert!(matches!(result, Completion::Error { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn covjson_uses_configured_delivery_even_when_small() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = deliver(
            &delivery(128, &calls),
            "application/prs.coverage+json",
            Some(64),
        )
        .await;

        assert!(matches!(result, Completion::Error { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_limit_and_unknown_lengths_preserve_fallback_failures() {
        for (limit, length) in [(0, Some(1)), (128, None)] {
            let calls = Arc::new(AtomicUsize::new(0));
            let result =
                deliver(&delivery(limit, &calls), "application/octet-stream", length).await;

            match result {
                Completion::Error { message } => assert_eq!(message, "fallback called"),
                other => panic!("expected fallback error, got {other:?}"),
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
}
