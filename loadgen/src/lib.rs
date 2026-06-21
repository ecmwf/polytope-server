use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::env;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum RunLimit {
    Iterations,
    Duration {
        duration: Duration,
        rps: Option<f64>,
    },
}

impl RunLimit {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Iterations => "iterations",
            Self::Duration { .. } => "duration",
        }
    }

    pub fn target_duration_s(&self) -> Option<f64> {
        match self {
            Self::Iterations => None,
            Self::Duration { duration, .. } => Some(duration.as_secs_f64()),
        }
    }

    pub fn target_rps(&self) -> Option<f64> {
        match self {
            Self::Iterations => None,
            Self::Duration { rps, .. } => *rps,
        }
    }
}

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
    pub run_limit: RunLimit,
    pub ramp_seconds: u64,
    pub poll_interval: Duration,
    pub poll_timeout: Duration,
    pub bobs_svc_template: String,
    pub max_error_rate: f64,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    pub fn from_lookup<F>(lookup: F) -> Result<Self, String>
    where
        F: Fn(&str) -> Option<String>,
    {
        let frontend_url = required_lookup(&lookup, "LOADGEN_FRONTEND_URL")?;
        let collection = required_lookup(&lookup, "LOADGEN_COLLECTION")?;
        let auth = required_lookup(&lookup, "LOADGEN_AUTH")?;
        let payload_json = serde_json::from_str(&required_lookup(&lookup, "LOADGEN_PAYLOAD_JSON")?)
            .map_err(|err| format!("LOADGEN_PAYLOAD_JSON is not valid JSON: {err}"))?;
        let bobs_svc_template = required_lookup(&lookup, "LOADGEN_BOBS_SVC_TEMPLATE")?;
        let mock_realm = optional_non_empty_lookup(&lookup, "LOADGEN_MOCK_REALM");
        let mock_role = lookup_or(&lookup, "LOADGEN_MOCK_ROLE", "default");
        let mock_user_prefix = lookup_or(&lookup, "LOADGEN_MOCK_USER_PREFIX", "mock-");
        let warmup_iters = parse_lookup(&lookup, "LOADGEN_WARMUP_ITERS", 5)?;
        let concurrency = parse_lookup(&lookup, "LOADGEN_CONCURRENCY", 64)?;
        let total_iters = parse_lookup(&lookup, "LOADGEN_TOTAL_ITERS", 512)?;
        let ramp_seconds = parse_lookup(&lookup, "LOADGEN_RAMP_SECONDS", 30)?;
        let poll_interval =
            Duration::from_millis(parse_lookup(&lookup, "LOADGEN_POLL_INTERVAL_MS", 250)?);
        let poll_timeout =
            Duration::from_secs(parse_lookup(&lookup, "LOADGEN_POLL_TIMEOUT_S", 600)?);
        let max_error_rate = parse_lookup(&lookup, "LOADGEN_MAX_ERROR_RATE", 0.01)?;
        let duration_s = parse_optional_f64(&lookup, "LOADGEN_DURATION_S")?;
        let rps = parse_optional_f64(&lookup, "LOADGEN_RPS")?;
        if frontend_url.ends_with('/') {
            return Err("LOADGEN_FRONTEND_URL must not have a trailing slash".to_string());
        }
        if concurrency == 0 {
            return Err("LOADGEN_CONCURRENCY must be at least 1".to_string());
        }
        let run_limit = match (duration_s, rps) {
            (Some(duration_s), rps) => {
                if duration_s <= 0.0 || !duration_s.is_finite() {
                    return Err("LOADGEN_DURATION_S must be a positive finite number".to_string());
                }
                if let Some(rps) = rps {
                    if rps <= 0.0 || !rps.is_finite() {
                        return Err("LOADGEN_RPS must be a positive finite number".to_string());
                    }
                }
                RunLimit::Duration {
                    duration: Duration::from_secs_f64(duration_s),
                    rps,
                }
            }
            (None, Some(_)) => {
                return Err("LOADGEN_RPS requires LOADGEN_DURATION_S".to_string());
            }
            (None, None) => RunLimit::Iterations,
        };
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
            run_limit,
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
            run_limit: self.run_limit.name().to_string(),
            target_duration_s: self.run_limit.target_duration_s(),
            target_rps: self.run_limit.target_rps(),
            ramp_seconds: self.ramp_seconds,
            poll_interval_ms: self.poll_interval.as_millis() as u64,
            poll_timeout_s: self.poll_timeout.as_secs(),
            bobs_svc_template: self.bobs_svc_template.clone(),
            max_error_rate: self.max_error_rate,
        }
    }
}

fn required_lookup<F>(lookup: &F, name: &str) -> Result<String, String>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name)
        .ok_or_else(|| format!("missing required environment variable {name}"))
        .and_then(|value| {
            if value.trim().is_empty() {
                Err(format!("required environment variable {name} is empty"))
            } else {
                Ok(value)
            }
        })
}

fn lookup_or<F>(lookup: &F, name: &str, default: &str) -> String
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn optional_non_empty_lookup<F>(lookup: &F, name: &str) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name).filter(|value| !value.is_empty())
}

fn parse_lookup<F, T>(lookup: &F, name: &str, default: T) -> Result<T, String>
where
    F: Fn(&str) -> Option<String>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match lookup(name) {
        Some(value) if !value.trim().is_empty() => value
            .parse()
            .map_err(|err| format!("{name} is invalid: {err}")),
        _ => Ok(default),
    }
}

fn parse_optional_f64<F>(lookup: &F, name: &str) -> Result<Option<f64>, String>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(value) if !value.trim().is_empty() => value
            .parse()
            .map(Some)
            .map_err(|err| format!("{name} is invalid: {err}")),
        _ => Ok(None),
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
    pub run_limit: String,
    pub target_duration_s: Option<f64>,
    pub target_rps: Option<f64>,
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
    pub completed_at: Instant,
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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TimeBucketMetrics {
    pub bucket_start_s: f64,
    pub bucket_end_s: f64,
    pub downloaded: usize,
    pub error: usize,
    pub bytes: u64,
    pub throughput_rps: f64,
    pub error_rate: f64,
    pub ready_ms_p95: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct SummaryMetadata {
    pub run_limit: String,
    pub target_duration_s: Option<f64>,
    pub submission_duration_s: f64,
    pub drain_duration_s: f64,
    pub scheduled: usize,
    pub missed_starts: usize,
    pub measured_start: Option<Instant>,
    pub bucket_width_s: f64,
}

impl SummaryMetadata {
    pub fn iteration(duration_s: f64, scheduled: usize) -> Self {
        Self {
            run_limit: "iterations".to_string(),
            target_duration_s: None,
            submission_duration_s: duration_s,
            drain_duration_s: 0.0,
            scheduled,
            missed_starts: 0,
            measured_start: None,
            bucket_width_s: DEFAULT_BUCKET_WIDTH_S,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub phase: String,
    pub config: SummaryConfig,
    pub run_limit: String,
    pub target_duration_s: Option<f64>,
    pub submission_duration_s: f64,
    pub drain_duration_s: f64,
    pub scheduled: usize,
    pub missed_starts: usize,
    pub counts: Counts,
    pub errors_by_status: BTreeMap<String, usize>,
    pub percentiles: BTreeMap<String, Percentiles>,
    pub time_buckets: Vec<TimeBucketMetrics>,
    pub bytes_total: u64,
    pub per_stream_mibps: StreamPercentiles,
    pub aggregate_mibps: f64,
    /// Peak sustained read throughput over any [`PEAK_WINDOW_S`]-wide window.
    ///
    /// Unlike `aggregate_mibps` (total bytes / first-read-to-last-read span),
    /// this is not diluted by ramp-up or a heavy-tailed single-stream drain: a
    /// mixed-size workload whose largest object reads back alone for minutes
    /// stretches the `aggregate_mibps` denominator, whereas this captures the
    /// dense concurrent-read plateau. See [`compute_peak_windowed_mibps`].
    pub peak_windowed_mibps: f64,
    /// Window width (seconds) used for `peak_windowed_mibps`.
    pub peak_window_s: f64,
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

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct ProgressCounts {
    pub started: usize,
    pub scheduled: usize,
    pub missed_starts: usize,
    pub submitted: usize,
    pub ready: usize,
    pub downloaded: usize,
    pub error: usize,
    pub submit_failed: usize,
    pub poll_failed: usize,
    pub timed_out: usize,
    pub download_failed: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProgressRates {
    pub window_rps: f64,
    pub window_mibps: f64,
}

/// JSON payload emitted as `LOADGEN_PROGRESS:{json}` during measured runs.
///
/// The progress stream is intentionally redaction-safe: it contains only
/// counters, byte totals, rates, elapsed time, and raw ready-latency samples.
/// It never includes request configuration, URLs, headers, authorization values,
/// payloads, mock identities, or precomputed ready-latency percentiles.
///
/// `window_s` is the time-based rolling-rate window width in seconds that live
/// consumers should use when aggregating progress. It defaults to `60`.
/// `ready_ms_since_last` contains raw ready-latency samples observed since the
/// previous emitted progress snapshot; consumers compute rolling percentiles
/// from these raw samples across pods.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProgressSnapshot {
    pub schema_version: u32,
    pub seq: u64,
    pub elapsed_s: f64,
    pub window_s: u64,
    pub counts: ProgressCounts,
    pub bytes_total: u64,
    pub rates: ProgressRates,
    pub ready_ms_since_last: Vec<f64>,
}

#[derive(Debug)]
pub struct ProgressTracker {
    start: Instant,
    window_s: u64,
    state: Mutex<ProgressState>,
}

#[derive(Debug, Default)]
struct ProgressState {
    seq: u64,
    counts: ProgressCounts,
    bytes_total: u64,
    ready_ms: Vec<f64>,
    last_snapshot_at: Option<Instant>,
    last_snapshot_downloaded: usize,
    last_snapshot_bytes: u64,
    last_snapshot_ready_index: usize,
}

impl ProgressTracker {
    pub fn new(start: Instant) -> Self {
        Self::with_window(start, 60)
    }

    pub fn with_window(start: Instant, window_s: u64) -> Self {
        Self {
            start,
            window_s,
            state: Mutex::new(ProgressState::default()),
        }
    }

    pub fn record_started(&self) {
        let mut state = self.state.lock().expect("progress mutex poisoned");
        state.counts.started += 1;
        state.counts.scheduled += 1;
    }

    pub fn record_missed_start(&self) {
        self.state
            .lock()
            .expect("progress mutex poisoned")
            .counts
            .missed_starts += 1;
    }

    pub fn record_submitted(&self) {
        self.state
            .lock()
            .expect("progress mutex poisoned")
            .counts
            .submitted += 1;
    }

    pub fn record_ready(&self, ready_ms: f64) {
        let mut state = self.state.lock().expect("progress mutex poisoned");
        state.counts.ready += 1;
        state.ready_ms.push(ready_ms);
    }

    pub fn record_finished(&self, outcome: Outcome, bytes: u64) {
        let mut state = self.state.lock().expect("progress mutex poisoned");
        state.bytes_total += bytes;
        match outcome {
            Outcome::Downloaded => state.counts.downloaded += 1,
            Outcome::SubmitFailed => {
                state.counts.error += 1;
                state.counts.submit_failed += 1;
            }
            Outcome::PollFailed => {
                state.counts.error += 1;
                state.counts.poll_failed += 1;
            }
            Outcome::TimedOut => {
                state.counts.error += 1;
                state.counts.timed_out += 1;
            }
            Outcome::DownloadFailed => {
                state.counts.error += 1;
                state.counts.download_failed += 1;
            }
        }
    }

    pub fn snapshot(&self, now: Instant) -> ProgressSnapshot {
        let mut state = self.state.lock().expect("progress mutex poisoned");
        let previous = state.last_snapshot_at.unwrap_or(self.start);
        let interval_s = (now - previous).as_secs_f64();
        let downloaded_delta = state.counts.downloaded - state.last_snapshot_downloaded;
        let bytes_delta = state.bytes_total - state.last_snapshot_bytes;
        let ready_ms_since_last = state.ready_ms[state.last_snapshot_ready_index..].to_vec();
        state.seq += 1;
        state.last_snapshot_at = Some(now);
        state.last_snapshot_downloaded = state.counts.downloaded;
        state.last_snapshot_bytes = state.bytes_total;
        state.last_snapshot_ready_index = state.ready_ms.len();
        ProgressSnapshot {
            schema_version: 1,
            seq: state.seq,
            elapsed_s: (now - self.start).as_secs_f64(),
            window_s: self.window_s,
            counts: state.counts.clone(),
            bytes_total: state.bytes_total,
            rates: ProgressRates {
                window_rps: if interval_s > 0.0 {
                    downloaded_delta as f64 / interval_s
                } else {
                    0.0
                },
                window_mibps: if interval_s > 0.0 {
                    (bytes_delta as f64 / 1_048_576.0) / interval_s
                } else {
                    0.0
                },
            },
            ready_ms_since_last,
        }
    }
}

pub fn loadgen_progress_interval_from_env() -> Result<Option<Duration>, String> {
    loadgen_progress_interval_from_value(env::var("LOADGEN_PROGRESS_INTERVAL_MS").ok().as_deref())
}

pub fn loadgen_progress_interval_from_value(
    value: Option<&str>,
) -> Result<Option<Duration>, String> {
    let millis: u64 = match value.filter(|value| !value.trim().is_empty()) {
        Some(value) => value
            .parse()
            .map_err(|err| format!("LOADGEN_PROGRESS_INTERVAL_MS is invalid: {err}"))?,
        None => 1000,
    };
    if millis == 0 {
        Ok(None)
    } else {
        Ok(Some(Duration::from_millis(millis)))
    }
}

pub fn summarize(
    phase: &str,
    config: SummaryConfig,
    results: &[IterationResult],
    duration_s: f64,
) -> Summary {
    summarize_with_metadata(
        phase,
        config,
        results,
        duration_s,
        SummaryMetadata::iteration(duration_s, results.len()),
    )
}

pub fn summarize_with_metadata(
    phase: &str,
    config: SummaryConfig,
    results: &[IterationResult],
    duration_s: f64,
    metadata: SummaryMetadata,
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
    let mut read_intervals: Vec<(Instant, Instant, u64)> = Vec::new();

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
        if let (Some(start), Some(end)) = (result.read_start, result.read_end) {
            read_intervals.push((start, end, result.bytes));
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
    let peak_windowed_mibps = match first_read_start {
        Some(origin) => {
            let relative: Vec<(f64, f64, u64)> = read_intervals
                .iter()
                .map(|&(start, end, bytes)| {
                    (
                        (start - origin).as_secs_f64(),
                        (end - origin).as_secs_f64(),
                        bytes,
                    )
                })
                .collect();
            compute_peak_windowed_mibps(&relative, PEAK_WINDOW_S)
        }
        None => 0.0,
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

    let time_buckets = time_buckets(results, metadata.measured_start, metadata.bucket_width_s);

    Summary {
        phase: phase.to_string(),
        config,
        run_limit: metadata.run_limit,
        target_duration_s: metadata.target_duration_s,
        submission_duration_s: metadata.submission_duration_s,
        drain_duration_s: metadata.drain_duration_s,
        scheduled: metadata.scheduled,
        missed_starts: metadata.missed_starts,
        counts,
        errors_by_status,
        percentiles: metric_percentiles,
        time_buckets,
        bytes_total,
        per_stream_mibps: stream_percentiles(&per_stream_mibps),
        aggregate_mibps,
        peak_windowed_mibps,
        peak_window_s: PEAK_WINDOW_S,
        throughput_rps,
        success_rate,
        error_rate,
        duration_s,
    }
}

const DEFAULT_BUCKET_WIDTH_S: f64 = 60.0;

/// Window width (seconds) for the peak sustained-throughput metric. A 10s
/// window smooths per-request jitter while still isolating the dense
/// concurrent-read plateau from ramp-up and the heavy-tailed drain.
const PEAK_WINDOW_S: f64 = 10.0;

#[derive(Default)]
struct BucketAccumulator {
    downloaded: usize,
    error: usize,
    bytes: u64,
    ready_ms: Vec<f64>,
}

fn time_buckets(
    results: &[IterationResult],
    measured_start: Option<Instant>,
    bucket_width_s: f64,
) -> Vec<TimeBucketMetrics> {
    if results.is_empty() || bucket_width_s <= 0.0 || !bucket_width_s.is_finite() {
        return Vec::new();
    }
    let origin = measured_start.unwrap_or_else(|| {
        results
            .iter()
            .map(|result| result.completed_at)
            .min()
            .unwrap_or_else(Instant::now)
    });
    let mut buckets: BTreeMap<usize, BucketAccumulator> = BTreeMap::new();
    for result in results {
        let elapsed_s = result
            .completed_at
            .checked_duration_since(origin)
            .unwrap_or_default()
            .as_secs_f64();
        let bucket_index = (elapsed_s / bucket_width_s).floor() as usize;
        let bucket = buckets.entry(bucket_index).or_default();
        if result.outcome == Outcome::Downloaded {
            bucket.downloaded += 1;
            bucket.bytes += result.bytes;
        } else {
            bucket.error += 1;
        }
        if let Some(ready_ms) = result.ready_ms {
            bucket.ready_ms.push(ready_ms);
        }
    }
    buckets
        .into_iter()
        .map(|(idx, bucket)| {
            let bucket_start_s = idx as f64 * bucket_width_s;
            let bucket_end_s = bucket_start_s + bucket_width_s;
            let total = bucket.downloaded + bucket.error;
            TimeBucketMetrics {
                bucket_start_s,
                bucket_end_s,
                downloaded: bucket.downloaded,
                error: bucket.error,
                bytes: bucket.bytes,
                throughput_rps: bucket.downloaded as f64 / bucket_width_s,
                error_rate: if total == 0 {
                    0.0
                } else {
                    bucket.error as f64 / total as f64
                },
                ready_ms_p95: percentile(&bucket.ready_ms, 95.0),
            }
        })
        .collect()
}

/// Peak sustained read throughput (MiB/s) over any `window_s`-wide window.
///
/// Each download is modeled as delivering its bytes at a constant rate across
/// its `[start, end]` interval; the instantaneous aggregate rate is the sum
/// over concurrently-active downloads. This returns the maximum average
/// throughput over any window of width `window_s`.
///
/// `intervals` are `(start_s, end_s, bytes)` relative to a common origin. The
/// constant-rate-per-read assumption is an approximation (no per-chunk
/// timestamps), but the windowed maximum reliably captures the plateau where
/// reads overlap densely, which `aggregate_mibps` understates whenever object
/// sizes (and therefore read durations) are skewed.
fn compute_peak_windowed_mibps(intervals: &[(f64, f64, u64)], window_s: f64) -> f64 {
    if intervals.is_empty() || window_s <= 0.0 {
        return 0.0;
    }

    // Rate-change events: +rate at start, -rate at end. Zero/negative-duration
    // reads are spread over a tiny epsilon so the modeled rate stays finite.
    let mut events: Vec<(f64, f64)> = Vec::with_capacity(intervals.len() * 2);
    let mut min_t = f64::INFINITY;
    let mut max_t = f64::NEG_INFINITY;
    for &(start, end, bytes) in intervals {
        let end = if end > start { end } else { start + 1e-6 };
        let rate = bytes as f64 / (end - start);
        events.push((start, rate));
        events.push((end, -rate));
        min_t = min_t.min(start);
        max_t = max_t.max(end);
    }
    events.sort_by(|a, b| a.0.total_cmp(&b.0));

    // Collapse to unique breakpoint times with the active rate on each segment
    // [times[k], times[k+1]) and the cumulative bytes delivered at times[k].
    let mut times: Vec<f64> = Vec::new();
    let mut seg_rate: Vec<f64> = Vec::new();
    let mut cur_rate = 0.0;
    let mut i = 0;
    while i < events.len() {
        let t = events[i].0;
        while i < events.len() && events[i].0 == t {
            cur_rate += events[i].1;
            i += 1;
        }
        times.push(t);
        seg_rate.push(cur_rate.max(0.0));
    }
    let mut cum: Vec<f64> = Vec::with_capacity(times.len());
    cum.push(0.0);
    for k in 0..times.len().saturating_sub(1) {
        cum.push(cum[k] + seg_rate[k] * (times[k + 1] - times[k]));
    }

    // C(t): cumulative bytes delivered by time t (piecewise linear).
    let cumulative = |t: f64| -> f64 {
        if t <= times[0] {
            return 0.0;
        }
        if t >= *times.last().unwrap() {
            return *cum.last().unwrap();
        }
        let k = match times.binary_search_by(|x| x.total_cmp(&t)) {
            Ok(k) => k,
            Err(k) => k - 1,
        };
        cum[k] + seg_rate[k] * (t - times[k])
    };

    let span = max_t - min_t;
    if span <= window_s {
        // Whole run fits in one window: the windowed peak is just the overall
        // read-span average (no tail to exclude).
        let bytes: f64 = intervals.iter().map(|&(_, _, b)| b as f64).sum();
        let denom = if span > 0.0 { span } else { window_s };
        return (bytes / 1_048_576.0) / denom;
    }

    // (C(t+W) - C(t)) is piecewise linear in t with breakpoints where t or
    // t+W crosses a segment boundary, so its maximum is at one of those
    // candidate positions.
    let lo = min_t;
    let hi = max_t - window_s;
    let mut best = 0.0;
    for &bp in &times {
        for cand in [bp, bp - window_s] {
            let t = cand.clamp(lo, hi);
            let delivered = cumulative(t + window_s) - cumulative(t);
            let mibps = (delivered / 1_048_576.0) / window_s;
            if mibps > best {
                best = mibps;
            }
        }
    }
    best
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

    fn base_env() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "LOADGEN_FRONTEND_URL".to_string(),
                "http://frontend:3000".to_string(),
            ),
            ("LOADGEN_COLLECTION".to_string(), "c".to_string()),
            (
                "LOADGEN_AUTH".to_string(),
                "EmailKey redaction-auth-secret".to_string(),
            ),
            (
                "LOADGEN_PAYLOAD_JSON".to_string(),
                json!({"a": 1}).to_string(),
            ),
            (
                "LOADGEN_BOBS_SVC_TEMPLATE".to_string(),
                "http://bobs-{ordinal}:3000".to_string(),
            ),
        ])
    }

    fn config_from_map(values: &BTreeMap<String, String>) -> Result<Config, String> {
        Config::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn config_defaults_to_iteration_mode_and_parses_duration_modes() {
        let mut env = base_env();
        let config = config_from_map(&env).unwrap();
        assert_eq!(config.run_limit, RunLimit::Iterations);
        assert_eq!(config.summary_config().run_limit, "iterations");
        assert_eq!(config.summary_config().target_duration_s, None);
        assert_eq!(config.summary_config().target_rps, None);
        assert_eq!(config.total_iters, 512);

        env.insert("LOADGEN_DURATION_S".to_string(), "900".to_string());
        let config = config_from_map(&env).unwrap();
        assert_eq!(config.run_limit.name(), "duration");
        assert_eq!(config.run_limit.target_duration_s(), Some(900.0));
        assert_eq!(config.run_limit.target_rps(), None);

        env.insert("LOADGEN_RPS".to_string(), "0.5".to_string());
        let config = config_from_map(&env).unwrap();
        assert_eq!(config.run_limit.name(), "duration");
        assert_eq!(config.run_limit.target_duration_s(), Some(900.0));
        assert_eq!(config.run_limit.target_rps(), Some(0.5));
    }

    #[test]
    fn config_rejects_invalid_duration_rps_and_concurrency_combinations() {
        let mut env = base_env();
        env.insert("LOADGEN_CONCURRENCY".to_string(), "0".to_string());
        assert!(config_from_map(&env).unwrap_err().contains("CONCURRENCY"));

        let mut env = base_env();
        env.insert("LOADGEN_DURATION_S".to_string(), "0".to_string());
        assert!(config_from_map(&env).unwrap_err().contains("DURATION"));

        let mut env = base_env();
        env.insert("LOADGEN_DURATION_S".to_string(), "60".to_string());
        env.insert("LOADGEN_RPS".to_string(), "0".to_string());
        assert!(config_from_map(&env).unwrap_err().contains("RPS"));

        let mut env = base_env();
        env.insert("LOADGEN_RPS".to_string(), "1".to_string());
        assert!(config_from_map(&env).unwrap_err().contains("requires"));
    }

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
            run_limit: "iterations".to_string(),
            target_duration_s: None,
            target_rps: None,
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
                completed_at: now + Duration::from_secs(1),
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
                completed_at: now + Duration::from_secs(3),
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
                completed_at: now + Duration::from_secs(4),
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
        // 3 MiB over a 3s read span (< 10s window) => falls back to span avg.
        assert_eq!(summary.peak_window_s, 10.0);
        assert!((summary.peak_windowed_mibps - 1.0).abs() < 1e-9);
    }

    #[test]
    fn summary_time_buckets_group_completions_and_ready_p95() {
        let now = Instant::now();
        let cfg = SummaryConfig {
            frontend_url: "http://frontend:3000".to_string(),
            collection: "c".to_string(),
            mock_realm: None,
            mock_role: "default".to_string(),
            mock_user_prefix: "mock-".to_string(),
            warmup_iters: 0,
            concurrency: 2,
            total_iters: 0,
            run_limit: "duration".to_string(),
            target_duration_s: Some(2.0),
            target_rps: Some(2.0),
            ramp_seconds: 0,
            poll_interval_ms: 250,
            poll_timeout_s: 600,
            bobs_svc_template: "http://bobs-{ordinal}:3000".to_string(),
            max_error_rate: 0.01,
        };
        let results = vec![
            IterationResult {
                outcome: Outcome::Downloaded,
                submit_ms: Some(1.0),
                ready_ms: Some(100.0),
                read_ms: Some(10.0),
                bytes: 10,
                status: None,
                read_start: Some(now),
                read_end: Some(now + Duration::from_millis(100)),
                completed_at: now + Duration::from_millis(200),
            },
            IterationResult {
                outcome: Outcome::Downloaded,
                submit_ms: Some(1.0),
                ready_ms: Some(300.0),
                read_ms: Some(10.0),
                bytes: 20,
                status: None,
                read_start: Some(now),
                read_end: Some(now + Duration::from_millis(100)),
                completed_at: now + Duration::from_millis(800),
            },
            IterationResult {
                outcome: Outcome::PollFailed,
                submit_ms: Some(1.0),
                ready_ms: None,
                read_ms: None,
                bytes: 0,
                status: Some(500),
                read_start: None,
                read_end: None,
                completed_at: now + Duration::from_millis(1200),
            },
        ];
        let summary = summarize_with_metadata(
            "measured",
            cfg,
            &results,
            2.0,
            SummaryMetadata {
                run_limit: "duration".to_string(),
                target_duration_s: Some(2.0),
                submission_duration_s: 2.0,
                drain_duration_s: 0.5,
                scheduled: 3,
                missed_starts: 4,
                measured_start: Some(now),
                bucket_width_s: 1.0,
            },
        );
        assert_eq!(summary.run_limit, "duration");
        assert_eq!(summary.scheduled, 3);
        assert_eq!(summary.missed_starts, 4);
        assert_eq!(summary.time_buckets.len(), 2);
        assert_eq!(summary.time_buckets[0].downloaded, 2);
        assert_eq!(summary.time_buckets[0].bytes, 30);
        assert_eq!(summary.time_buckets[0].throughput_rps, 2.0);
        assert_eq!(summary.time_buckets[0].ready_ms_p95, Some(300.0));
        assert_eq!(summary.time_buckets[1].error, 1);
        assert_eq!(summary.time_buckets[1].error_rate, 1.0);
    }

    #[test]
    fn summary_progress_and_config_serialization_exclude_auth_and_env_secret_values() {
        let mut env = base_env();
        env.insert(
            "LOADGEN_AUTH".to_string(),
            "EmailKey super-auth-token".to_string(),
        );
        env.insert(
            "POLYTOPE_EMAIL".to_string(),
            "private-user@example.test".to_string(),
        );
        env.insert(
            "POLYTOPE_KEY".to_string(),
            "private-polytope-key".to_string(),
        );
        env.insert("LOADGEN_DURATION_S".to_string(), "60".to_string());
        env.insert("LOADGEN_RPS".to_string(), "1".to_string());
        let config = config_from_map(&env).unwrap();
        let start = Instant::now();
        let summary = summarize_with_metadata(
            "measured",
            config.summary_config(),
            &[],
            0.0,
            SummaryMetadata {
                run_limit: config.run_limit.name().to_string(),
                target_duration_s: config.run_limit.target_duration_s(),
                submission_duration_s: 0.0,
                drain_duration_s: 0.0,
                scheduled: 0,
                missed_starts: 0,
                measured_start: Some(start),
                bucket_width_s: 60.0,
            },
        );
        let tracker = ProgressTracker::new(start);
        tracker.record_started();
        tracker.record_missed_start();
        tracker.record_finished(Outcome::DownloadFailed, 0);
        let text = format!(
            "{}\n{}\n{}",
            serde_json::to_string(&summary).unwrap(),
            serde_json::to_string(&tracker.snapshot(start + Duration::from_secs(1))).unwrap(),
            serde_json::to_string(&config.summary_config()).unwrap(),
        );
        for forbidden in [
            "EmailKey super-auth-token",
            "private-user@example.test",
            "private-polytope-key",
            "LOADGEN_AUTH",
            "Authorization",
            "POLYTOPE_EMAIL",
            "POLYTOPE_KEY",
        ] {
            assert!(
                !text.contains(forbidden),
                "found forbidden value {forbidden}: {text}"
            );
        }
    }

    #[test]
    fn peak_windowed_throughput_excludes_sparse_tail() {
        let mib = 1_048_576u64;
        // A fast 100 MiB read in [0,1]s alongside a slow 100 MiB read spread
        // over [0,100]s. Naive span-average = 200 MiB / 100 s = 2 MiB/s, badly
        // diluted by the tail. The busiest 10s window [0,10] delivers all
        // 100 MiB of the fast read plus 10 MiB of the slow one = 110 MiB/10s.
        let intervals = vec![(0.0, 1.0, 100 * mib), (0.0, 100.0, 100 * mib)];
        let peak = compute_peak_windowed_mibps(&intervals, 10.0);
        assert!((peak - 11.0).abs() < 0.01, "peak={peak}");
    }

    #[test]
    fn peak_windowed_throughput_short_run_falls_back_to_span_average() {
        let mib = 1_048_576u64;
        // 40 MiB over a 4s span, shorter than the 10s window.
        let intervals = vec![(0.0, 4.0, 40 * mib)];
        let peak = compute_peak_windowed_mibps(&intervals, 10.0);
        assert!((peak - 10.0).abs() < 0.01, "peak={peak}");
    }

    #[test]
    fn progress_snapshot_uses_interval_download_and_byte_deltas() {
        let start = Instant::now();
        let tracker = ProgressTracker::new(start);
        tracker.record_started();
        tracker.record_started();
        tracker.record_submitted();
        tracker.record_ready(120.0);
        tracker.record_finished(Outcome::Downloaded, 2 * 1_048_576);

        let first = tracker.snapshot(start + Duration::from_secs(2));
        assert_eq!(first.seq, 1);
        assert_eq!(first.counts.started, 2);
        assert_eq!(first.counts.submitted, 1);
        assert_eq!(first.counts.ready, 1);
        assert_eq!(first.counts.downloaded, 1);
        assert_eq!(first.bytes_total, 2 * 1_048_576);
        assert_eq!(first.rates.window_rps, 0.5);
        assert_eq!(first.rates.window_mibps, 1.0);
        assert_eq!(first.ready_ms_since_last, vec![120.0]);

        tracker.record_ready(240.0);
        tracker.record_finished(Outcome::Downloaded, 4 * 1_048_576);
        let second = tracker.snapshot(start + Duration::from_secs(4));
        assert_eq!(second.seq, 2);
        assert_eq!(second.counts.downloaded, 2);
        assert_eq!(second.bytes_total, 6 * 1_048_576);
        assert_eq!(second.rates.window_rps, 0.5);
        assert_eq!(second.rates.window_mibps, 2.0);
        assert_eq!(second.ready_ms_since_last, vec![240.0]);

        let third = tracker.snapshot(start + Duration::from_secs(5));
        assert_eq!(third.rates.window_rps, 0.0);
        assert_eq!(third.rates.window_mibps, 0.0);
        assert!(third.ready_ms_since_last.is_empty());
    }

    #[test]
    fn progress_snapshot_defaults_window_to_sixty_seconds_and_emits_raw_ready_samples() {
        let start = Instant::now();
        let tracker = ProgressTracker::new(start);
        tracker.record_ready(10.0);
        tracker.record_ready(30.0);
        let snapshot = tracker.snapshot(start + Duration::from_secs(1));
        assert_eq!(snapshot.window_s, 60);
        assert_eq!(snapshot.ready_ms_since_last, vec![10.0, 30.0]);
    }

    #[test]
    fn progress_snapshot_serialization_is_redaction_safe() {
        let start = Instant::now();
        let tracker = ProgressTracker::new(start);
        tracker.record_started();
        tracker.record_submitted();
        tracker.record_ready(42.0);
        tracker.record_finished(Outcome::PollFailed, 0);
        let value = serde_json::to_value(tracker.snapshot(start + Duration::from_secs(1))).unwrap();
        let text = value.to_string();

        assert_eq!(value["schema_version"], 1);
        assert!(value.get("counts").is_some());
        assert!(value.get("rates").is_some());
        assert_eq!(value["counts"]["error"], 1);
        assert_eq!(value["counts"]["poll_failed"], 1);
        assert_eq!(value["ready_ms_since_last"], json!([42.0]));
        for forbidden in [
            "auth",
            "authorization",
            "header",
            "config",
            "frontend_url",
            "collection",
            "payload",
            "EmailKey",
            "rolling_ready_ms",
            "p95",
        ] {
            assert!(
                !text.to_lowercase().contains(&forbidden.to_lowercase()),
                "progress snapshot contains forbidden field/value {forbidden}: {text}"
            );
        }
    }

    #[test]
    fn progress_interval_zero_disables_progress_without_changing_summary_shape() {
        assert_eq!(
            loadgen_progress_interval_from_value(Some("0")).unwrap(),
            None
        );
        assert_eq!(
            loadgen_progress_interval_from_value(None).unwrap(),
            Some(Duration::from_millis(1000))
        );
        assert_eq!(
            loadgen_progress_interval_from_value(Some("250")).unwrap(),
            Some(Duration::from_millis(250))
        );

        let cfg = SummaryConfig {
            frontend_url: "http://frontend:3000".to_string(),
            collection: "c".to_string(),
            mock_realm: None,
            mock_role: "default".to_string(),
            mock_user_prefix: "mock-".to_string(),
            warmup_iters: 5,
            concurrency: 1,
            total_iters: 0,
            run_limit: "iterations".to_string(),
            target_duration_s: None,
            target_rps: None,
            ramp_seconds: 0,
            poll_interval_ms: 250,
            poll_timeout_s: 600,
            bobs_svc_template: "http://bobs-{ordinal}:3000".to_string(),
            max_error_rate: 0.01,
        };
        let summary = serde_json::to_value(summarize("measured", cfg, &[], 0.0)).unwrap();
        assert_eq!(summary["phase"], "measured");
        assert!(summary.get("config").is_some());
        assert!(summary.get("counts").is_some());
        assert!(summary.get("rates").is_none());
        assert!(summary.get("ready_ms_since_last").is_none());
    }

    #[test]
    fn peak_windowed_throughput_empty_is_zero() {
        assert_eq!(compute_peak_windowed_mibps(&[], 10.0), 0.0);
    }
}
