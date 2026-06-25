use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetMetadata {
    pub key: String,
    pub value: Value,
}

#[async_trait]
impl TransformAction for SetMetadata {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let metadata = job.metadata_mut();

        // Ensure metadata is an object; if not, replace with empty object
        if !metadata.is_object() {
            *metadata = serde_json::json!({});
        }

        // Write or overwrite only the configured key; preserve all other keys
        metadata
            .as_object_mut()
            .expect("metadata is object after check")
            .insert(self.key.clone(), self.value.clone());

        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "set_metadata", SetMetadata);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn writes_new_metadata_key() {
        let action: SetMetadata = serde_json::from_value(json!({
            "key": "polytope_mars",
            "value": {"datacube": "test", "options": {"foo": "bar"}}
        }))
        .unwrap();

        let mut job = Job::new(json!({}));
        let result = action.execute(&mut job).await.unwrap();

        assert!(matches!(result, TransformResult::Continue));
        assert_eq!(
            job.metadata["polytope_mars"],
            json!({"datacube": "test", "options": {"foo": "bar"}})
        );
    }

    #[tokio::test]
    async fn overwrites_existing_key() {
        let action: SetMetadata = serde_json::from_value(json!({
            "key": "polytope_mars",
            "value": {"datacube": "new", "options": {"baz": "qux"}}
        }))
        .unwrap();

        let mut job = Job::new(json!({}));
        job.metadata_mut()["polytope_mars"] =
            json!({"datacube": "old", "options": {"old": "data"}});

        let result = action.execute(&mut job).await.unwrap();

        assert!(matches!(result, TransformResult::Continue));
        assert_eq!(
            job.metadata["polytope_mars"],
            json!({"datacube": "new", "options": {"baz": "qux"}})
        );
    }

    #[tokio::test]
    async fn preserves_unrelated_trusted_metadata() {
        let action: SetMetadata = serde_json::from_value(json!({
            "key": "polytope_mars",
            "value": {"datacube": "test"}
        }))
        .unwrap();

        let mut job = Job::new(json!({}));
        job.metadata_mut()["cost"] = json!(123);
        job.metadata_mut()["admin_overrides"] = json!({"mock_now_rfc3339": "2040-05-06T07:08:09Z"});
        job.metadata_mut()["accept_encoding"] = json!("gzip");
        job.metadata_mut()["buffer_full_output"] = json!(true);

        let result = action.execute(&mut job).await.unwrap();

        assert!(matches!(result, TransformResult::Continue));
        assert_eq!(job.metadata["polytope_mars"], json!({"datacube": "test"}));
        assert_eq!(job.metadata["cost"], json!(123));
        assert_eq!(
            job.metadata["admin_overrides"],
            json!({"mock_now_rfc3339": "2040-05-06T07:08:09Z"})
        );
        assert_eq!(job.metadata["accept_encoding"], json!("gzip"));
        assert_eq!(job.metadata["buffer_full_output"], json!(true));
    }

    #[tokio::test]
    async fn handles_non_object_metadata() {
        let action: SetMetadata = serde_json::from_value(json!({
            "key": "polytope_mars",
            "value": {"datacube": "test"}
        }))
        .unwrap();

        let mut job = Job::new(json!({}));
        *job.metadata_mut() = json!("not an object");

        let result = action.execute(&mut job).await.unwrap();

        assert!(matches!(result, TransformResult::Continue));
        assert_eq!(job.metadata["polytope_mars"], json!({"datacube": "test"}));
        assert!(job.metadata.is_object());
    }

    #[tokio::test]
    async fn does_not_read_request_fields() {
        // This test proves that the action only reads from its static config value,
        // never from client request fields like metadata, polytope_mars, pre_path, or use_catalogue.
        let action: SetMetadata = serde_json::from_value(json!({
            "key": "polytope_mars",
            "value": {"datacube": "config-supplied", "options": {"source": "trusted-routing"}}
        }))
        .unwrap();

        let mut job = Job::new(json!({
            "metadata": {"client": "should-not-appear"},
            "polytope_mars": {"client": "should-not-appear"},
            "pre_path": "client-controlled",
            "use_catalogue": true
        }));

        let result = action.execute(&mut job).await.unwrap();

        assert!(matches!(result, TransformResult::Continue));
        // Only the config value appears in metadata, not the request fields
        assert_eq!(
            job.metadata["polytope_mars"],
            json!({"datacube": "config-supplied", "options": {"source": "trusted-routing"}})
        );
        // Request fields are unchanged
        assert_eq!(
            job.request["metadata"],
            json!({"client": "should-not-appear"})
        );
        assert_eq!(
            job.request["polytope_mars"],
            json!({"client": "should-not-appear"})
        );
        assert_eq!(job.request["pre_path"], json!("client-controlled"));
        assert_eq!(job.request["use_catalogue"], json!(true));
    }

    #[tokio::test]
    async fn preserves_metadata_when_request_contains_similar_keys() {
        // Prove that even when the request contains keys with similar names,
        // only the config value is written to metadata
        let action: SetMetadata = serde_json::from_value(json!({
            "key": "cost",
            "value": 999
        }))
        .unwrap();

        let mut job = Job::new(json!({
            "cost": 42,
            "admin_overrides": {"client": "controlled"}
        }));
        job.metadata_mut()["existing_cost"] = json!(555);

        let result = action.execute(&mut job).await.unwrap();

        assert!(matches!(result, TransformResult::Continue));
        // The config value overwrites the metadata key
        assert_eq!(job.metadata["cost"], json!(999));
        // But the request fields are untouched
        assert_eq!(job.request["cost"], json!(42));
        assert_eq!(
            job.request["admin_overrides"],
            json!({"client": "controlled"})
        );
        // Other metadata is preserved
        assert_eq!(job.metadata["existing_cost"], json!(555));
    }
}
