use async_trait::async_trait;
use polytope_worker_common::{ProcessResult, Processor, WorkItem};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TestConfig {
    pub behaviour: Behaviour,
    #[serde(default = "default_content_type")]
    pub content_type: String,
}

pub fn default_content_type() -> String {
    "application/json".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Behaviour {
    Reject,
    Wait {
        duration_ms: u64,
    },
    Error,
    Echo,
    Dummy {
        #[serde(default = "default_dummy_count")]
        count: u64,
    },
}

pub fn default_dummy_count() -> u64 {
    10
}

pub struct BehaviourProcessor {
    pub config: TestConfig,
}

impl BehaviourProcessor {
    pub fn new(config: TestConfig) -> Self {
        Self { config }
    }
}

pub fn json_success(content_type: &str, payload: Vec<u8>) -> ProcessResult {
    let body = bytes::Bytes::from(payload);
    let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(body)));
    ProcessResult::success(content_type, Box::new(stream))
}

#[async_trait]
impl Processor for BehaviourProcessor {
    async fn process(&self, work: WorkItem) -> ProcessResult {
        match &self.config.behaviour {
            Behaviour::Reject => ProcessResult::reject("rejected by test worker"),

            Behaviour::Wait { duration_ms } => {
                tokio::time::sleep(std::time::Duration::from_millis(*duration_ms)).await;
                json_success(&self.config.content_type, b"{}".to_vec())
            }

            Behaviour::Error => ProcessResult::error("test error"),

            Behaviour::Echo => {
                let payload = serde_json::to_vec(&work.request).unwrap_or_default();
                json_success(&self.config.content_type, payload)
            }

            Behaviour::Dummy { count } => {
                let data: Vec<u64> = (1..=*count).collect();
                let payload = serde_json::to_vec(&data).unwrap_or_default();
                json_success(&self.config.content_type, payload)
            }
        }
    }
}
