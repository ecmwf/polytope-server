use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HasKey {
    pub key: String,
}

#[async_trait]
impl CheckAction for HasKey {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if job.request.get(&self.key).is_some() {
            Ok(CheckResult::Pass)
        } else {
            Ok(CheckResult::Reject {
                reason: format!("request does not contain key '{}'", self.key),
                silent: true,
            })
        }
    }
}

bits::register_action!(check, "has_key", HasKey);
