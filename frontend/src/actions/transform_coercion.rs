use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};

use crate::actions::coercion::{CoercionConfig, coerce_request};

#[derive(Debug, Clone, Serialize, Default)]
pub struct RequestCoercion {
    #[serde(flatten)]
    pub config: CoercionConfig,
}

impl<'de> Deserialize<'de> for RequestCoercion {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<CoercionConfig>::deserialize(deserializer).map(|opt| Self {
            config: opt.unwrap_or_default(),
        })
    }
}

#[async_trait]
impl TransformAction for RequestCoercion {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        job.request = coerce_request(&job.request, &self.config)?;
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "coerce_request", RequestCoercion);
