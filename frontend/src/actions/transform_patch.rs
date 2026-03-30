use std::collections::HashMap;

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchRequest {
    pub set: HashMap<String, Value>,
}

#[async_trait]
impl TransformAction for PatchRequest {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let obj = job
            .request
            .as_object_mut()
            .ok_or_else(|| ActionError::ConfigError("request is not an object".into()))?;
        for (k, v) in &self.set {
            obj.insert(k.clone(), v.clone());
        }
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "patch_request", PatchRequest);
