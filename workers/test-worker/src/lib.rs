use async_trait::async_trait;
use polytope_worker_common::{ProcessResult, Processor, WorkItem};
use serde::Deserialize;

const DEFAULT_STRESS_DELAY_MS: u64 = 0;
const DEFAULT_STRESS_RESPONSE_BYTES: usize = 1024;
const DEFAULT_STRESS_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_STRESS_MAX_DELAY_MS: u64 = 10_000;
const DEFAULT_STRESS_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

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
    Stress {
        #[serde(default = "default_stress_delay_ms")]
        default_delay_ms: u64,
        #[serde(default = "default_stress_response_bytes")]
        default_response_bytes: usize,
        #[serde(default = "default_stress_chunk_bytes")]
        default_chunk_bytes: usize,
        #[serde(default = "default_stress_max_delay_ms")]
        max_delay_ms: u64,
        // response_bytes is streamed lazily (StressStream), so there is no
        // allocation to bound -- no max_response_bytes cap. chunk_bytes is a
        // real per-chunk allocation, so it keeps a cap.
        #[serde(default = "default_stress_max_chunk_bytes")]
        max_chunk_bytes: usize,
    },
}

pub fn default_dummy_count() -> u64 {
    10
}

pub fn default_stress_delay_ms() -> u64 {
    DEFAULT_STRESS_DELAY_MS
}

pub fn default_stress_response_bytes() -> usize {
    DEFAULT_STRESS_RESPONSE_BYTES
}

pub fn default_stress_chunk_bytes() -> usize {
    DEFAULT_STRESS_CHUNK_BYTES
}

pub fn default_stress_max_delay_ms() -> u64 {
    DEFAULT_STRESS_MAX_DELAY_MS
}

pub fn default_stress_max_chunk_bytes() -> usize {
    DEFAULT_STRESS_MAX_CHUNK_BYTES
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

fn nested_stress_config(request: &serde_json::Value) -> Option<&serde_json::Value> {
    request
        .get("stress")
        .or_else(|| request.get("request").and_then(|inner| inner.get("stress")))
}

fn stress_u64(request: &serde_json::Value, key: &str) -> Option<u64> {
    nested_stress_config(request)?.get(key)?.as_u64()
}

fn stress_usize(request: &serde_json::Value, key: &str) -> Option<usize> {
    stress_u64(request, key).and_then(|value| usize::try_from(value).ok())
}

struct StressStream {
    remaining: usize,
    chunk: bytes::Bytes,
}

impl futures::Stream for StressStream {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return std::task::Poll::Ready(None);
        }

        let bytes_to_send = this.remaining.min(this.chunk.len());
        this.remaining -= bytes_to_send;
        let bytes = if bytes_to_send == this.chunk.len() {
            this.chunk.clone()
        } else {
            this.chunk.slice(..bytes_to_send)
        };
        std::task::Poll::Ready(Some(Ok(bytes)))
    }
}

/// Build a lazily-evaluated stream of byte chunks totalling `total` bytes.
///
/// The stream reuses a fixed byte buffer for full-sized chunks so synthetic
/// data generation does not become the throughput bottleneck being measured.
/// The last chunk may be smaller than `chunk` bytes when `total` is not evenly
/// divisible.
fn stress_stream(
    total: usize,
    chunk: usize,
) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin + 'static {
    let chunk_len = chunk.max(1).min(total).max(1);
    StressStream {
        remaining: total,
        chunk: bytes::Bytes::from(vec![b'x'; chunk_len]),
    }
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

            Behaviour::Stress {
                default_delay_ms,
                default_response_bytes,
                default_chunk_bytes,
                max_delay_ms,
                max_chunk_bytes,
            } => {
                let delay_ms = stress_u64(&work.request, "delay_ms")
                    .unwrap_or(*default_delay_ms)
                    .min(*max_delay_ms);
                let response_bytes = stress_usize(&work.request, "response_bytes")
                    .unwrap_or(*default_response_bytes);
                let chunk_bytes = stress_usize(&work.request, "chunk_bytes")
                    .unwrap_or(*default_chunk_bytes)
                    .min(*max_chunk_bytes)
                    .max(1);

                if delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }

                let stream = stress_stream(response_bytes, chunk_bytes);
                ProcessResult::success(&self.config.content_type, Box::new(stream))
            }
        }
    }
}
