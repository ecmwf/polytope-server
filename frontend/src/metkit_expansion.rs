use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MetkitExpansion {}

#[async_trait]
impl TransformAction for MetkitExpansion {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let obj = job
            .request
            .as_object_mut()
            .ok_or_else(|| ActionError::ConfigError("request is not an object".into()))?;
        let had_verb = obj.contains_key("verb");
        obj.entry("verb")
            .or_insert_with(|| serde_json::json!("retrieve"));

        let mut expanded = metkit::expand_json(&job.request)
            .map_err(|e| ActionError::ConfigError(format!("metkit expansion failed: {e}")))?;

        if !had_verb {
            if let Some(obj) = expanded.as_object_mut() {
                obj.remove("verb");
            }
        }

        job.request = expanded;
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "metkit_expansion", MetkitExpansion);
