use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct RateLimit {
    #[serde(default)]
    pub total: Option<u32>,
    #[serde(default)]
    pub per_user: Option<u32>,
}

impl<'de> Deserialize<'de> for RateLimit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Inner {
            #[serde(default)]
            total: Option<u32>,
            #[serde(default)]
            per_user: Option<u32>,
        }

        let inner = Inner::deserialize(deserializer)?;
        tracing::warn!(
            "rate_limit action is not yet implemented; requests will not be rate-limited"
        );
        Ok(Self {
            total: inner.total,
            per_user: inner.per_user,
        })
    }
}

#[async_trait]
impl CheckAction for RateLimit {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "rate_limit", RateLimit);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserializes_config() {
        let parsed: RateLimit = serde_json::from_value(json!({"total": 100, "per_user": 5}))
            .expect("rate_limit should deserialize");
        assert_eq!(parsed.total, Some(100));
        assert_eq!(parsed.per_user, Some(5));
    }

    #[tokio::test]
    async fn always_passes() {
        let action = RateLimit {
            total: Some(10),
            per_user: Some(1),
        };
        let job = Job::new(json!({"class": "od"}));
        assert!(matches!(
            action.evaluate(&job).await.unwrap(),
            CheckResult::Pass
        ));
    }
}
