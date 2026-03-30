use std::collections::HashMap;

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Match {
    #[serde(flatten)]
    pub fields: HashMap<String, Value>,
}

fn value_matches_any(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Array(actuals), Value::Array(expecteds)) => {
            actuals.iter().any(|a| expecteds.iter().any(|e| a == e))
        }
        (Value::Array(actuals), expected_scalar) => actuals.iter().any(|a| a == expected_scalar),
        (actual_scalar, Value::Array(expecteds)) => expecteds.iter().any(|e| actual_scalar == e),
        (actual_scalar, expected_scalar) => actual_scalar == expected_scalar,
    }
}

#[async_trait]
impl CheckAction for Match {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        for (key, expected) in &self.fields {
            let Some(actual) = job.request.get(key) else {
                return Ok(CheckResult::Reject {
                    reason: format!("request missing key '{key}'"),
                    silent: true,
                });
            };

            if !value_matches_any(actual, expected) {
                return Ok(CheckResult::Reject {
                    reason: format!("{key}: '{}' does not match required '{}'", actual, expected),
                    silent: true,
                });
            }
        }
        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "match", Match);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mk(fields: Value) -> Match {
        serde_json::from_value(fields).unwrap()
    }

    #[tokio::test]
    async fn scalar_equals_scalar_passes() {
        let check = mk(json!({"class": "od"}));
        let job = Job::new(json!({"class": "od"}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Pass
        ));
    }

    #[tokio::test]
    async fn scalar_not_equal_scalar_rejects() {
        let check = mk(json!({"class": "od"}));
        let job = Job::new(json!({"class": "ai"}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Reject { .. }
        ));
    }

    #[tokio::test]
    async fn config_array_matches_scalar_request() {
        let check = mk(json!({"class": ["od", "ai"]}));
        let job = Job::new(json!({"class": "ai"}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Pass
        ));
    }

    #[tokio::test]
    async fn config_scalar_matches_array_request() {
        let check = mk(json!({"class": "ai"}));
        let job = Job::new(json!({"class": ["od", "ai"]}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Pass
        ));
    }

    #[tokio::test]
    async fn array_matches_array_on_any_overlap() {
        let check = mk(json!({"class": ["od", "ai"]}));
        let job = Job::new(json!({"class": ["xx", "ai"]}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Pass
        ));
    }

    #[tokio::test]
    async fn array_rejects_when_no_overlap() {
        let check = mk(json!({"class": ["od", "ai"]}));
        let job = Job::new(json!({"class": ["xx", "yy"]}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Reject { .. }
        ));
    }

    #[tokio::test]
    async fn multiple_fields_require_all_to_match() {
        let check = mk(json!({"class": ["od", "ai"], "stream": ["oper", "enfo"]}));
        let passing_job = Job::new(json!({"class": "od", "stream": "enfo"}));
        assert!(matches!(
            check.evaluate(&passing_job).await.unwrap(),
            CheckResult::Pass
        ));

        let failing_job = Job::new(json!({"class": "od", "stream": "wave"}));
        assert!(matches!(
            check.evaluate(&failing_job).await.unwrap(),
            CheckResult::Reject { .. }
        ));
    }

    #[tokio::test]
    async fn missing_field_rejects() {
        let check = mk(json!({"class": ["od", "ai"], "stream": ["oper", "enfo"]}));
        let job = Job::new(json!({"class": "od"}));
        assert!(matches!(
            check.evaluate(&job).await.unwrap(),
            CheckResult::Reject { .. }
        ));
    }
}
