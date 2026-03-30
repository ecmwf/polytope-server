use std::collections::HashMap;

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetDefaults {
    pub set: HashMap<String, Value>,
}

#[async_trait]
impl TransformAction for SetDefaults {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let obj = job
            .request
            .as_object_mut()
            .ok_or_else(|| ActionError::ConfigError("request is not an object".into()))?;
        for (k, v) in &self.set {
            if !obj.contains_key(k) {
                obj.insert(k.clone(), v.clone());
            }
        }
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "set_defaults", SetDefaults);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn fills_only_missing_keys() {
        let action: SetDefaults = serde_json::from_value(json!({
            "set": {
                "class": "od",
                "stream": "oper",
                "type": "fc"
            }
        }))
        .unwrap();

        let mut job = Job::new(json!({
            "class": "ai",
            "stream": "wave"
        }));

        let result = action.execute(&mut job).await.unwrap();
        assert!(matches!(result, TransformResult::Continue));
        assert_eq!(job.request["class"], json!("ai"));
        assert_eq!(job.request["stream"], json!("wave"));
        assert_eq!(job.request["type"], json!("fc"));
    }
}
