use async_trait::async_trait;
use aws_sdk_s3::{
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use bytes::BytesMut;

use super::ResultDelivery;
use crate::Completion;

const S3_PART_SIZE_BYTES: usize = 5 * 1024 * 1024;
const S3_PRESIGNED_GET_TTL_SECS: u64 = 604800;

pub(super) struct S3Push {
    pub(super) bucket: String,
    pub(super) key_prefix: String,
    pub(super) s3_client: aws_sdk_s3::Client,
}

#[async_trait]
impl ResultDelivery for S3Push {
    async fn deliver(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
    ) -> Completion {
        match self.push(content_type, content_encoding, body).await {
            Ok(location) => Completion::Redirect {
                location,
                message: "result available for download".to_string(),
            },
            Err(e) => Completion::Error {
                message: format!("delivery failed: {e}"),
            },
        }
    }
}

impl S3Push {
    async fn push(
        &self,
        content_type: &str,
        content_encoding: Option<&str>,
        body: reqwest::Body,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let s3_key = self.next_key();

        let mut create_req = self
            .s3_client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&s3_key)
            .content_type(content_type);
        if let Some(encoding) = content_encoding {
            create_req = create_req.content_encoding(encoding);
        }
        let create_out = create_req.send().await?;

        let upload_id = create_out
            .upload_id()
            .ok_or("missing upload_id from CreateMultipartUpload")?
            .to_string();

        let parts = match self.upload_stream_parts(&s3_key, &upload_id, body).await {
            Ok(parts) => parts,
            Err(err) => {
                let _ = self
                    .s3_client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&s3_key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                return Err(err);
            }
        };

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.s3_client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&s3_key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        self.presigned_get_url(&s3_key).await
    }

    async fn upload_stream_parts(
        &self,
        s3_key: &str,
        upload_id: &str,
        body: reqwest::Body,
    ) -> Result<Vec<CompletedPart>, Box<dyn std::error::Error + Send + Sync>> {
        let mut completed_parts = Vec::new();
        let mut stream = ByteStream::from_body_1_x(body);
        let mut buffer = BytesMut::new();
        let mut part_number: i32 = 1;

        while let Some(chunk) = stream.try_next().await? {
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= S3_PART_SIZE_BYTES {
                let part = buffer.split_to(S3_PART_SIZE_BYTES).freeze();
                completed_parts.push(self.upload_part(s3_key, upload_id, part_number, part).await?);
                part_number += 1;
            }
        }

        if !buffer.is_empty() || completed_parts.is_empty() {
            let final_part = buffer.split().freeze();
            completed_parts.push(
                self.upload_part(s3_key, upload_id, part_number, final_part)
                    .await?,
            );
        }

        Ok(completed_parts)
    }

    async fn upload_part(
        &self,
        s3_key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: bytes::Bytes,
    ) -> Result<CompletedPart, Box<dyn std::error::Error + Send + Sync>> {
        let uploaded = self
            .s3_client
            .upload_part()
            .bucket(&self.bucket)
            .key(s3_key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(bytes))
            .send()
            .await?;

        let e_tag = uploaded
            .e_tag()
            .ok_or("missing ETag from UploadPart response")?
            .to_string();

        Ok(CompletedPart::builder()
            .set_part_number(Some(part_number))
            .set_e_tag(Some(e_tag))
            .build())
    }

    fn next_key(&self) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        if self.key_prefix.is_empty() {
            id
        } else {
            format!("{}/{}", self.key_prefix.trim_end_matches('/'), id)
        }
    }

    async fn presigned_get_url(
        &self,
        s3_key: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let presigned = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .presigned(
                PresigningConfig::builder()
                    .expires_in(std::time::Duration::from_secs(S3_PRESIGNED_GET_TTL_SECS))
                    .build()?,
            )
            .await?;

        Ok(presigned.uri().to_string())
    }
}

#[cfg(test)]
impl S3Push {
    fn for_test_with_endpoint(bucket: &str, key_prefix: &str, endpoint_url: &str) -> Self {
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(endpoint_url)
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "test-credentials",
            ))
            .build();

        Self {
            bucket: bucket.to_string(),
            key_prefix: key_prefix.to_string(),
            s3_client: aws_sdk_s3::Client::from_conf(conf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn s3_push_returns_error_when_unreachable() {
        let push = S3Push::for_test_with_endpoint("example-bucket", "results", "http://127.0.0.1:1");

        let result = push
            .deliver(
                "application/octet-stream",
                Some("gzip"),
                reqwest::Body::from(vec![1, 2, 3]),
            )
            .await;

        match result {
            Completion::Error { message } => {
                assert!(message.starts_with("delivery failed:"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
