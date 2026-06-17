use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::env;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Config {
    pub frontend_url: String,
    pub collection: String,
    pub auth: String,
    pub payload_json: Value,
    pub mock_realm: Option<String>,
    pub mock_role: String,
    pub mock_user_prefix: String,
    pub warmup_iters: usize,
    pub concurrency: usize,
    pub total_iters: usize,
    pub ramp_seconds: u64,
    pub poll_interval: Duration,
    pub poll_timeout: Duration,
    pub bobs_svc_template: String,
    pub max_error_rate: f64,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let frontend_url = required_env("LOADGEN_FRONTEND_URL")?;
        let collection = required_env("LOADGEN_COLLECTION")?;
        let auth = required_env("LOADGEN_AUTH")?;
        let payload_json = serde_json::from_str(&required_env("LOADGEN_PAYLOAD_JSON")?)
            .map_err(|err| format!("LOADGEN_PAYLOAD_JSON is not valid JSON: {err}"))?;
        let bobs_svc_template = required_env("LOADGEN_BOBS_SVC_TEMPLATE")?;
        let mock_realm = optional_non_empty("LOADGEN_MOCK_REALM");
        let mock_role = env_or("LOADGEN_MOCK_ROLE", "default");
        let mock_user_prefix = env_or("LOADGEN_MOCK_USER_PREFIX", "mock-");
        let warmup_iters = parse_env("LOADGEN_WARMUP_ITERS", 5)?;
        let concurrency = parse_env("LOADGEN_CONCURRENCY", 64)?;
        let total_iters = parse_env("LOADGEN_TOTAL_ITERS", 512)?;
        let ramp_seconds = parse_env("LOADGEN_RAMP_SECONDS", 30)?;
        let poll_interval = Duration::from_millis(parse_env("LOADGEN_POLL_INTERVAL_MS", 250)?);
        let poll_timeout = Duration::from_secs(parse_env("LOADGEN_POLL_TIMEOUT_S", 600)?);
        let max_error_rate = parse_env("LOADGEN_MAX_ERROR_RATE", 0.01)?;
        if frontend_url.ends_with('/') {
            return Err("LOADGEN_FRONTEND_URL must not have a trailing slash".to_string());
        }
        if concurrency == 0 {
            return Err("LOADGEN_CONCURRENCY must be at least 1".to_string());
        }
        Ok(Self {
            frontend_url,
            collection,
            auth,
            payload_json,
            mock_realm,
            mock_role,
            mock_user_prefix,
            warmup_iters,
            concurrency,
            total_iters,
            ramp_seconds,
            poll_interval,
            poll_timeout,
            bobs_svc_template,
            max_error_rate,
        })
    }

    pub fn request_body(&self) -> Value {
        json!({"verb": "retrieve", "request": self.payload_json})
    }

    pub fn summary_config(&self) -> SummaryConfig {
        SummaryConfig {
            frontend_url: self.frontend_url.clone(),
            collection: self.collection.clone(),
            mock_realm: self.mock_realm.clone(),
            mock_role: self.mock_role.clone(),
            mock_user_prefix: self.mock_user_prefix.clone(),
            warmup_iters: self.warmup_iters,
            concurrency: self.concurrency,
            total_iters: self.total_iters,
            ramp_seconds: self.ramp_seconds,
            poll_interval_ms: self.poll_interval.as_millis() as u64,
            poll_timeout_s: self.poll_timeout.as_secs(),
            bobs_svc_template: self.bobs_svc_template.clone(),
            max_error_rate: self.max_error_rate,
        }
    }
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name)
        .map_err(|_| format!("missing required environment variable {name}"))
        .and_then(|value| {
            if value.trim().is_empty() {
                Err(format!("required environment variable {name} is empty"))
            } else {
                Ok(value)
            }
        })
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn optional_non_empty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn parse_env<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .parse()
            .map_err(|err| format!("{name} is invalid: {err}")),
        _ => Ok(default),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SummaryConfig {
    pub frontend_url: String,
    pub collection: String,
    pub mock_realm: Option<String>,
    pub mock_role: String,
    pub mock_user_prefix: String,
    pub warmup_iters: usize,
    pub concurrency: usize,
    pub total_iters: usize,
    pub ramp_seconds: u64,
    pub poll_interval_ms: u64,
    pub poll_timeout_s: u64,
    pub bobs_svc_template: String,
    pub max_error_rate: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Downloaded,
    SubmitFailed,
    PollFailed,
    TimedOut,
    DownloadFailed,
}

#[derive(Debug, Clone)]
pub struct IterationResult {
    pub outcome: Outcome,
    pub submit_ms: Option<f64>,
    pub ready_ms: Option<f64>,
    pub read_ms: Option<f64>,
    pub bytes: u64,
    pub status: Option<u16>,
    pub read_start: Option<Instant>,
    pub read_end: Option<Instant>,
}

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct Counts {
    pub submitted: usize,
    pub ready: usize,
    pub downloaded: usize,
    pub submit_failed: usize,
    pub poll_failed: usize,
    pub timed_out: usize,
    pub download_failed: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Percentiles {
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StreamPercentiles {
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub phase: String,
    pub config: SummaryConfig,
    pub counts: Counts,
    pub errors_by_status: BTreeMap<String, usize>,
    pub percentiles: BTreeMap<String, Percentiles>,
    pub bytes_total: u64,
    pub per_stream_mibps: StreamPercentiles,
    pub aggregate_mibps: f64,
    pub throughput_rps: f64,
    pub success_rate: f64,
    pub error_rate: f64,
    pub duration_s: f64,
}

pub fn to_bobs_internal(
    location_url: &str,
    bobs_svc_template: &str,
) -> Result<(String, String), String> {
    let parsed = reqwest::Url::parse(location_url)
        .map_err(|_| format!("Cannot parse BOBS location URL: {location_url:?}"))?;
    let parts: Vec<String> = parsed
        .path_segments()
        .ok_or_else(|| format!("Cannot parse BOBS location URL: {location_url:?}"))?
        .filter(|part| !part.is_empty())
        .map(percent_decode)
        .collect::<Result<_, _>>()?;

    let (route_segment, key, key_must_be_uuid_like) = match parts.as_slice() {
        [route, key] => (route, key, true),
        [route, api, v1, key] if api == "api" && v1 == "v1" => (route, key, true),
        [route, api, v1, read, key] if api == "api" && v1 == "v1" && read == "read" => {
            (route, key, false)
        }
        _ => return Err(format!("Cannot parse BOBS location URL: {location_url:?}")),
    };

    if !is_route_segment(route_segment) || key.is_empty() || key.contains('/') {
        return Err(format!("Cannot parse BOBS location URL: {location_url:?}"));
    }
    if key_must_be_uuid_like && !is_uuid_like_key(key) {
        return Err(format!("Cannot parse BOBS location URL: {location_url:?}"));
    }

    let ordinal = route_segment
        .rsplit_once('-')
        .map(|(_, ordinal)| ordinal)
        .unwrap_or_default();
    let base = bobs_svc_template.replace("{ordinal}", ordinal);
    Ok((format!("{base}/api/v1/read/{key}"), ordinal.to_string()))
}

fn is_route_segment(value: &str) -> bool {
    let Some((prefix, ordinal)) = value.rsplit_once('-') else {
        return false;
    };
    !prefix.is_empty()
        && prefix.bytes().all(|byte| byte.is_ascii_lowercase())
        && !ordinal.is_empty()
        && ordinal.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_uuid_like_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
}

fn percent_decode(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'%' {
            if idx + 2 >= bytes.len() {
                return Err("invalid percent encoding in BOBS location URL".to_string());
            }
            let hex = std::str::from_utf8(&bytes[idx + 1..idx + 3])
                .map_err(|_| "invalid percent encoding in BOBS location URL".to_string())?;
            let value = u8::from_str_radix(hex, 16)
                .map_err(|_| "invalid percent encoding in BOBS location URL".to_string())?;
            out.push(value);
            idx += 3;
        } else {
            out.push(bytes[idx]);
            idx += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "invalid UTF-8 in BOBS location URL".to_string())
}

pub fn summarize(
    phase: &str,
    config: SummaryConfig,
    results: &[IterationResult],
    duration_s: f64,
) -> Summary {
    let mut counts = Counts::default();
    let mut errors_by_status = BTreeMap::new();
    let mut submit_ms = Vec::new();
    let mut ready_ms = Vec::new();
    let mut read_ms = Vec::new();
    let mut per_stream_mibps = Vec::new();
    let mut bytes_total = 0u64;
    let mut first_read_start: Option<Instant> = None;
    let mut last_read_end: Option<Instant> = None;

    for result in results {
        if result.submit_ms.is_some() {
            counts.submitted += 1;
        }
        if let Some(value) = result.submit_ms {
            submit_ms.push(value);
        }
        if let Some(value) = result.ready_ms {
            counts.ready += 1;
            ready_ms.push(value);
        }
        if let Some(value) = result.read_ms {
            read_ms.push(value);
            if value > 0.0 {
                per_stream_mibps.push((result.bytes as f64 / 1_048_576.0) / (value / 1_000.0));
            }
        }
        bytes_total += result.bytes;
        match result.outcome {
            Outcome::Downloaded => counts.downloaded += 1,
            Outcome::SubmitFailed => counts.submit_failed += 1,
            Outcome::PollFailed => counts.poll_failed += 1,
            Outcome::TimedOut => counts.timed_out += 1,
            Outcome::DownloadFailed => counts.download_failed += 1,
        }
        if let Some(status) = result.status {
            *errors_by_status.entry(status.to_string()).or_insert(0) += 1;
        }
        if let Some(start) = result.read_start {
            first_read_start = Some(first_read_start.map_or(start, |current| current.min(start)));
        }
        if let Some(end) = result.read_end {
            last_read_end = Some(last_read_end.map_or(end, |current| current.max(end)));
        }
    }

    let total = results.len();
    let success_rate = if total == 0 {
        1.0
    } else {
        counts.downloaded as f64 / total as f64
    };
    let error_rate = 1.0 - success_rate;
    let read_span_s = match (first_read_start, last_read_end) {
        (Some(start), Some(end)) => (end - start).as_secs_f64(),
        _ => 0.0,
    };
    let aggregate_mibps = if read_span_s > 0.0 {
        (bytes_total as f64 / 1_048_576.0) / read_span_s
    } else {
        0.0
    };
    let throughput_rps = if duration_s > 0.0 {
        counts.downloaded as f64 / duration_s
    } else {
        0.0
    };

    let mut metric_percentiles = BTreeMap::new();
    metric_percentiles.insert("submit_ms".to_string(), percentiles(&submit_ms));
    metric_percentiles.insert("ready_ms".to_string(), percentiles(&ready_ms));
    metric_percentiles.insert("read_ms".to_string(), percentiles(&read_ms));

    Summary {
        phase: phase.to_string(),
        config,
        counts,
        errors_by_status,
        percentiles: metric_percentiles,
        bytes_total,
        per_stream_mibps: stream_percentiles(&per_stream_mibps),
        aggregate_mibps,
        throughput_rps,
        success_rate,
        error_rate,
        duration_s,
    }
}

fn percentiles(values: &[f64]) -> Percentiles {
    Percentiles {
        p50: percentile(values, 50.0),
        p90: percentile(values, 90.0),
        p95: percentile(values, 95.0),
        p99: percentile(values, 99.0),
        max: values.iter().copied().reduce(f64::max),
        mean: mean(values),
    }
}

fn stream_percentiles(values: &[f64]) -> StreamPercentiles {
    StreamPercentiles {
        p50: percentile(values, 50.0),
        p95: percentile(values, 95.0),
        max: values.iter().copied().reduce(f64::max),
        mean: mean(values),
    }
}

fn percentile(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let index = ((percentile / 100.0) * (ordered.len() as f64 - 1.0)).round() as usize;
    ordered.get(index).copied()
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

pub fn warmup_failed_summary(config: SummaryConfig) -> Value {
    json!({
        "phase": "warmup_failed",
        "config": config,
    })
}

pub fn redact_auth_for_logs(_: &Config) -> Map<String, Value> {
    Map::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bobs_location_translation_matches_python_runner() {
        let template = "http://rel-bobs-{ordinal}:3000";
        assert_eq!(
            to_bobs_internal("https://host/download-3/0123-abcd", template).unwrap(),
            (
                "http://rel-bobs-3:3000/api/v1/read/0123-abcd".to_string(),
                "3".to_string()
            )
        );
        assert_eq!(
            to_bobs_internal("https://host/download-12/api/v1/abcdef", template)
                .unwrap()
                .0,
            "http://rel-bobs-12:3000/api/v1/read/abcdef"
        );
        assert_eq!(
            to_bobs_internal("https://host/download-4/api/v1/read/object-key", template)
                .unwrap()
                .0,
            "http://rel-bobs-4:3000/api/v1/read/object-key"
        );
        assert!(to_bobs_internal("https://host/download-4/not-api/object-key", template).is_err());
        assert!(to_bobs_internal("https://host/download-4/not_uuid", template).is_err());
    }

    #[test]
    fn summary_aggregation_uses_known_percentiles_and_rates() {
        let now = Instant::now();
        let cfg = SummaryConfig {
            frontend_url: "http://frontend:3000".to_string(),
            collection: "c".to_string(),
            mock_realm: None,
            mock_role: "default".to_string(),
            mock_user_prefix: "mock-".to_string(),
            warmup_iters: 5,
            concurrency: 2,
            total_iters: 4,
            ramp_seconds: 1,
            poll_interval_ms: 250,
            poll_timeout_s: 600,
            bobs_svc_template: "http://bobs-{ordinal}:3000".to_string(),
            max_error_rate: 0.01,
        };
        let results = vec![
            IterationResult {
                outcome: Outcome::Downloaded,
                submit_ms: Some(10.0),
                ready_ms: Some(100.0),
                read_ms: Some(1000.0),
                bytes: 1_048_576,
                status: None,
                read_start: Some(now),
                read_end: Some(now + Duration::from_secs(1)),
            },
            IterationResult {
                outcome: Outcome::Downloaded,
                submit_ms: Some(20.0),
                ready_ms: Some(200.0),
                read_ms: Some(2000.0),
                bytes: 2_097_152,
                status: None,
                read_start: Some(now + Duration::from_secs(1)),
                read_end: Some(now + Duration::from_secs(3)),
            },
            IterationResult {
                outcome: Outcome::SubmitFailed,
                submit_ms: Some(30.0),
                ready_ms: None,
                read_ms: None,
                bytes: 0,
                status: Some(500),
                read_start: None,
                read_end: None,
            },
        ];
        let summary = summarize("measured", cfg, &results, 6.0);
        assert_eq!(summary.counts.downloaded, 2);
        assert_eq!(summary.counts.submit_failed, 1);
        assert_eq!(summary.errors_by_status.get("500"), Some(&1));
        assert_eq!(summary.percentiles["submit_ms"].p50, Some(20.0));
        assert_eq!(summary.percentiles["submit_ms"].p90, Some(30.0));
        assert_eq!(summary.percentiles["ready_ms"].mean, Some(150.0));
        assert_eq!(summary.bytes_total, 3_145_728);
        assert_eq!(summary.aggregate_mibps, 1.0);
        assert_eq!(summary.throughput_rps, 2.0 / 6.0);
        assert!((summary.error_rate - (1.0 / 3.0)).abs() < f64::EPSILON);
        assert_eq!(summary.per_stream_mibps.mean, Some(1.0));
    }
}
