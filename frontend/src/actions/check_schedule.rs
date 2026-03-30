use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};

use crate::actions::schedule::{ScheduleCatalog, ScheduleReleased};

#[async_trait]
impl CheckAction for ScheduleReleased {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let catalog = ScheduleCatalog::from_path(&self.path)?;
        match catalog.assert_request_released(&job.request, self.current_time()?) {
            Ok(()) => Ok(CheckResult::Pass),
            Err(ActionError::ResourceError(reason)) => Ok(CheckResult::Reject {
                reason,
                silent: false,
            }),
            Err(ActionError::ConfigError(reason)) => Ok(CheckResult::Reject {
                reason,
                silent: false,
            }),
            Err(err) => Err(err),
        }
    }
}

bits::register_action!(check, "schedule_released", ScheduleReleased);
