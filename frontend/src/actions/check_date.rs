use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

use crate::actions::date_check::date_check;

#[derive(Debug, Serialize, Deserialize)]
pub struct DateChecker {
    #[serde(default = "default_date_key")]
    pub key: String,
    pub allowed_values: Vec<String>,
}

fn default_date_key() -> String {
    "date".into()
}

#[async_trait]
impl CheckAction for DateChecker {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let Some(value) = job.request.get(&self.key) else {
            return Ok(CheckResult::Reject {
                reason: format!("request does not contain expected key '{}'", self.key),
                silent: false,
            });
        };
        match date_check(value, &self.allowed_values) {
            Ok(()) => Ok(CheckResult::Pass),
            Err(err) => Ok(CheckResult::Reject {
                reason: err.to_string(),
                silent: false,
            }),
        }
    }
}

bits::register_action!(check, "date_checker", DateChecker);
