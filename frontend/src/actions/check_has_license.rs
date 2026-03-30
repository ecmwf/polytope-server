use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HasLicense {
    pub license: String,
}

#[async_trait]
impl CheckAction for HasLicense {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match job.metadata.get("license").and_then(|v| v.as_str()) {
            Some(l) if l == self.license => Ok(CheckResult::Pass),
            Some(l) => Ok(CheckResult::Reject {
                reason: format!("license '{}' does not match required '{}'", l, self.license),
                silent: true,
            }),
            None => Ok(CheckResult::Reject {
                reason: "no license field found".to_string(),
                silent: true,
            }),
        }
    }
}

bits::register_action!(check, "has_license", HasLicense);
