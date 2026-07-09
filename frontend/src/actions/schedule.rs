// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::fs;

use bits::Job;
use bits::actions::ActionError;
use chrono::{DateTime, Duration, NaiveTime, Utc};
use roxmltree::Document;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actions::coercion::{
    as_object, max_request_date, max_request_u32, parse_request_class_like, parse_request_time,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleReleased {
    pub path: String,
    #[serde(default)]
    pub now_rfc3339: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScheduleCatalog {
    products: Vec<Product>,
}

#[derive(Debug, Clone)]
struct Product {
    class: String,
    stream: Option<String>,
    domain: Option<String>,
    time: Option<String>,
    step: Option<u32>,
    type_name: Option<String>,
    diss_step: Option<u32>,
    diss_domain: Option<String>,
    diss_type: Option<String>,
    release_time: NaiveTime,
    release_delta_day: i64,
}

impl ScheduleCatalog {
    pub fn from_path(path: &str) -> Result<Self, ActionError> {
        let raw = fs::read_to_string(path).map_err(|err| {
            ActionError::ConfigError(format!("failed to read schedule XML: {err}"))
        })?;
        Self::from_raw_xml(&raw)
    }

    pub fn from_raw_xml(raw: &str) -> Result<Self, ActionError> {
        let start = raw.find("<schedule").ok_or_else(|| {
            ActionError::ConfigError("schedule XML must contain a <schedule> root".into())
        })?;
        let xml = &raw[start..];
        let doc = Document::parse(xml).map_err(|err| {
            ActionError::ConfigError(format!("failed to parse schedule XML: {err}"))
        })?;

        let mut products = Vec::new();
        for node in doc
            .descendants()
            .filter(|node| node.has_tag_name("product"))
        {
            let text = |name: &str| {
                node.children()
                    .find(|child| child.has_tag_name(name))
                    .and_then(|child| child.text())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
            };

            let release_time = text("release_time").ok_or_else(|| {
                ActionError::ConfigError("schedule product missing release_time".into())
            })?;
            let release_time = NaiveTime::parse_from_str(&release_time, "%H:%M:%S")
                .map_err(|err| ActionError::ConfigError(format!("invalid release_time: {err}")))?;

            products.push(Product {
                class: text("class").unwrap_or_default().to_lowercase(),
                stream: text("stream").map(|value| value.to_lowercase()),
                domain: text("domain").map(|value| value.to_lowercase()),
                time: text("time"),
                step: text("step").and_then(|value| value.parse::<u32>().ok()),
                type_name: text("type").map(|value| value.to_lowercase()),
                diss_step: text("diss_step").and_then(|value| value.parse::<u32>().ok()),
                diss_domain: text("diss_domain").map(|value| value.to_lowercase()),
                diss_type: text("diss_type").map(|value| value.to_lowercase()),
                release_time,
                release_delta_day: text("release_delta_day")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0),
            });
        }

        Ok(Self { products })
    }

    pub fn assert_request_released(
        &self,
        request: &Value,
        now: DateTime<Utc>,
    ) -> Result<(), ActionError> {
        let request = as_object(request)?;
        let date = max_request_date(request.get("date").ok_or_else(|| missing_key("date"))?)?;
        let time = parse_request_time(request.get("time").ok_or_else(|| missing_key("time"))?)?;
        let step = resolve_step(request)?;
        let classes =
            parse_request_class_like(request.get("class").ok_or_else(|| missing_key("class"))?);
        let streams =
            parse_request_class_like(request.get("stream").ok_or_else(|| missing_key("stream"))?);
        let domains = request
            .get("domain")
            .map(parse_request_class_like)
            .unwrap_or_else(|| vec!["g".into()]);
        let types =
            parse_request_class_like(request.get("type").ok_or_else(|| missing_key("type"))?);

        for class in &classes {
            for stream in &streams {
                for domain in &domains {
                    for type_name in &types {
                        let product = self
                            .release_product(class, stream, domain, &time, step, type_name)
                            .ok_or_else(|| {
                                ActionError::ConfigError(format!(
                                    "No matching schedule entry for class={class}, stream={stream}, type={type_name}, time={time}, step={step:04}"
                                ))
                            })?;

                        let release_date = date.and_time(product.release_time)
                            + Duration::days(product.release_delta_day);
                        let release_date = release_date.and_utc();
                        if now < release_date {
                            return Err(ActionError::ResourceError(format!(
                                "Data not released yet. Release time is {release_date}."
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn release_product(
        &self,
        class: &str,
        stream: &str,
        domain: &str,
        time: &str,
        step: u32,
        type_name: &str,
    ) -> Option<&Product> {
        let mut matching = self
            .products
            .iter()
            .filter(|product| product.matches(class, stream, domain, time, type_name))
            .collect::<Vec<_>>();
        matching.sort_by_key(|product| product.step.or(product.diss_step).unwrap_or(0));
        matching
            .into_iter()
            .filter(|product| product.step.or(product.diss_step).unwrap_or(360) <= step)
            .max_by_key(|product| product.step.or(product.diss_step).unwrap_or(0))
            .or_else(|| {
                self.products.iter().find(|product| {
                    product.matches(class, stream, domain, time, type_name)
                        && product.step.is_none()
                })
            })
    }
}

impl Product {
    fn matches(
        &self,
        class: &str,
        stream: &str,
        domain: &str,
        time: &str,
        type_name: &str,
    ) -> bool {
        self.class == class
            && self
                .stream
                .as_ref()
                .is_none_or(|candidate| contains_token(candidate, stream))
            && self.time.as_ref().is_none_or(|candidate| candidate == time)
            && self
                .domain
                .as_ref()
                .or(self.diss_domain.as_ref())
                .is_none_or(|candidate| candidate == domain)
            && self
                .type_name
                .as_ref()
                .or(self.diss_type.as_ref())
                .is_none_or(|candidate| contains_token(candidate, type_name))
    }
}

fn contains_token(candidate: &str, needle: &str) -> bool {
    candidate
        .split('/')
        .any(|token| token.eq_ignore_ascii_case(needle))
}

/// Resolve the largest step the request asks for, considering BOTH the
/// top-level `step` field and any feature-derived step bound, and returning
/// their maximum.
///
/// Both sources must be considered because `transform::metkit_expansion`
/// runs before this check and injects a default top-level `step` (typically
/// `"0"`) into requests that originally only carried a `feature`. Looking at
/// only the top-level `step` would silently gate every feature retrieval on
/// step 0's release time — effectively bypassing the schedule check for any
/// request that asks for longer steps via `feature.range` or trajectory
/// points. The schedule check must reflect the largest step the request
/// will actually pull, which is the max of whatever metkit produced and
/// whatever the feature carries.
fn resolve_step(request: &serde_json::Map<String, Value>) -> Result<u32, ActionError> {
    let from_top_level = request.get("step").map(max_request_u32).transpose()?;
    let from_feature = step_from_feature(request)?;

    match (from_top_level, from_feature) {
        (Some(a), Some(b)) => Ok(a.max(b)),
        (Some(s), None) | (None, Some(s)) => Ok(s),
        (None, None) => Err(missing_key("step")),
    }
}

/// Extract the largest step indicated by a `feature` block, if present and
/// meaningful. Returns `None` when no `feature` is present, when the feature
/// type does not carry a step bound (e.g. `verticalprofile`), or when the
/// expected step-bearing fields are missing. Returns an error only for
/// malformed numeric values that the request author clearly intended as a
/// step bound.
fn step_from_feature(request: &serde_json::Map<String, Value>) -> Result<Option<u32>, ActionError> {
    let Some(feature) = request.get("feature").and_then(|v| v.as_object()) else {
        return Ok(None);
    };
    let feature_type = feature.get("type").and_then(|v| v.as_str()).unwrap_or("");

    match feature_type {
        "timeseries" => Ok(feature
            .get("range")
            .and_then(|r| r.get("end"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)),
        "trajectory" => {
            let Some(axes) = feature.get("axes").and_then(|v| v.as_array()) else {
                return Ok(None);
            };
            let Some(step_idx) = axes.iter().position(|v| v.as_str() == Some("step")) else {
                return Ok(None);
            };
            let Some(points) = feature.get("points").and_then(|v| v.as_array()) else {
                return Ok(None);
            };
            Ok(points
                .iter()
                .filter_map(|p| p.as_array()?.get(step_idx)?.as_u64())
                .max()
                .map(|v| v as u32))
        }
        _ => Ok(None),
    }
}

fn missing_key(key: &str) -> ActionError {
    ActionError::ConfigError(format!(
        "Cannot check data availability: request does not contain '{key}'"
    ))
}

impl ScheduleReleased {
    pub fn current_time(&self, job: &Job) -> Result<DateTime<Utc>, ActionError> {
        // Trust boundary: this admin override carrier is trusted only because submit helpers
        // write it from authenticated request extensions; client request JSON must never be
        // merged into job metadata.
        if let Some(admin_overrides) = job.metadata.get("admin_overrides") {
            let admin_overrides = admin_overrides.as_object().ok_or_else(|| {
                ActionError::ConfigError("invalid admin_overrides metadata: expected object".into())
            })?;
            if let Some(mock_now) = admin_overrides.get("mock_now_rfc3339") {
                let mock_now = mock_now.as_str().ok_or_else(|| {
                    ActionError::ConfigError(
                        "invalid admin_overrides.mock_now_rfc3339 metadata: expected string".into(),
                    )
                })?;
                return parse_current_time_override(
                    mock_now,
                    "invalid admin_overrides.mock_now_rfc3339 metadata",
                );
            }
        }

        match &self.now_rfc3339 {
            Some(value) => parse_current_time_override(value, "invalid now_rfc3339 override"),
            None => Ok(Utc::now()),
        }
    }
}

fn parse_current_time_override(
    value: &str,
    message_prefix: &str,
) -> Result<DateTime<Utc>, ActionError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| ActionError::ConfigError(format!("{message_prefix}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bits::db::PersistentJobRecord;
    use chrono::TimeZone;
    use serde_json::json;

    fn schedule(now_rfc3339: Option<&str>) -> ScheduleReleased {
        ScheduleReleased {
            path: "unused.xml".into(),
            now_rfc3339: now_rfc3339.map(str::to_string),
        }
    }

    fn job_with_metadata(metadata: Value) -> Job {
        let mut job = Job::new(json!({}));
        *job.metadata_mut() = metadata;
        job
    }

    fn parse_utc(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn config_error_message(result: Result<DateTime<Utc>, ActionError>) -> String {
        match result {
            Err(ActionError::ConfigError(message)) => message,
            other => panic!("expected config error, got {other:?}"),
        }
    }

    fn schedule_request() -> Value {
        json!({
            "class": "od",
            "stream": "oper",
            "domain": "g",
            "type": "fc",
            "date": "20240101",
            "time": "0000",
            "step": 0
        })
    }

    fn schedule_catalog() -> ScheduleCatalog {
        ScheduleCatalog::from_raw_xml(
            r#"
            <schedule>
                <product>
                    <class>od</class>
                    <stream>oper</stream>
                    <domain>g</domain>
                    <type>fc</type>
                    <time>00:00</time>
                    <step>0</step>
                    <release_time>12:00:00</release_time>
                    <release_delta_day>0</release_delta_day>
                </product>
            </schedule>
            "#,
        )
        .unwrap()
    }

    #[test]
    fn current_time_prefers_namespaced_metadata_over_static_config() {
        let job = job_with_metadata(json!({
            "admin_overrides": {
                "mock_now_rfc3339": "2030-01-01T12:34:56+02:00"
            }
        }));

        let current_time = schedule(Some("2020-01-01T00:00:00Z"))
            .current_time(&job)
            .unwrap();

        assert_eq!(current_time, parse_utc("2030-01-01T10:34:56Z"));
    }

    #[test]
    fn current_time_uses_static_config_before_wall_clock() {
        let job = job_with_metadata(json!({}));

        let current_time = schedule(Some("2020-01-01T00:00:00Z"))
            .current_time(&job)
            .unwrap();

        assert_eq!(current_time, parse_utc("2020-01-01T00:00:00Z"));
    }

    #[test]
    fn current_time_uses_wall_clock_without_overrides() {
        let job = job_with_metadata(json!({}));
        let before = Utc::now();

        let current_time = schedule(None).current_time(&job).unwrap();

        let after = Utc::now();
        assert!(current_time >= before);
        assert!(current_time <= after);
    }

    #[test]
    fn invalid_static_now_rfc3339_is_config_error() {
        let message = config_error_message(
            schedule(Some("not-rfc3339")).current_time(&job_with_metadata(json!({}))),
        );

        assert!(message.contains("invalid now_rfc3339 override"));
    }

    #[test]
    fn invalid_namespaced_metadata_is_config_error() {
        let message =
            config_error_message(schedule(None).current_time(&job_with_metadata(json!({
                "admin_overrides": {
                    "mock_now_rfc3339": "not-rfc3339"
                }
            }))));

        assert!(message.contains("invalid admin_overrides.mock_now_rfc3339 metadata"));
    }

    #[test]
    fn non_string_namespaced_metadata_is_config_error() {
        let message =
            config_error_message(schedule(None).current_time(&job_with_metadata(json!({
                "admin_overrides": {
                    "mock_now_rfc3339": 123
                }
            }))));

        assert!(message.contains("expected string"));
    }

    #[test]
    fn flat_mock_now_metadata_is_ignored() {
        let job = job_with_metadata(json!({
            "mock_now_rfc3339": "2030-01-01T00:00:00Z"
        }));

        let current_time = schedule(Some("2020-01-01T00:00:00Z"))
            .current_time(&job)
            .unwrap();

        assert_eq!(current_time, parse_utc("2020-01-01T00:00:00Z"));
    }

    #[test]
    fn schedule_fixture_rejects_before_and_passes_after_release_from_namespaced_metadata() {
        let catalog = schedule_catalog();
        let request = schedule_request();

        let before_release = parse_utc("2024-01-01T11:59:59Z");
        let before = catalog.assert_request_released(&request, before_release);
        assert!(matches!(before, Err(ActionError::ResourceError(_))));

        let after_release = parse_utc("2024-01-01T12:00:00Z");
        catalog
            .assert_request_released(&request, after_release)
            .unwrap();

        let before_job = job_with_metadata(json!({
            "admin_overrides": {
                "mock_now_rfc3339": "2024-01-01T11:59:59Z"
            }
        }));
        let before = catalog
            .assert_request_released(&request, schedule(None).current_time(&before_job).unwrap());
        assert!(matches!(before, Err(ActionError::ResourceError(_))));

        let after_job = job_with_metadata(json!({
            "admin_overrides": {
                "mock_now_rfc3339": "2024-01-01T12:00:00Z"
            }
        }));
        catalog
            .assert_request_released(&request, schedule(None).current_time(&after_job).unwrap())
            .unwrap();
    }

    #[test]
    fn request_body_mock_now_shapes_do_not_influence_current_time() {
        let job = Job::new(json!({
            "mock_now_rfc3339": "2030-01-01T00:00:00Z",
            "metadata": {
                "admin_overrides": {
                    "mock_now_rfc3339": "2030-01-01T00:00:00Z"
                }
            }
        }));

        let current_time = schedule(Some("2020-01-01T00:00:00Z"))
            .current_time(&job)
            .unwrap();

        assert_eq!(current_time, parse_utc("2020-01-01T00:00:00Z"));
    }

    #[test]
    fn restored_jobs_retain_namespaced_mock_time_metadata() {
        let record = PersistentJobRecord {
            job_id: "job-id".into(),
            broker_id: "broker-id".into(),
            original_request: json!({}),
            user: json!({}),
            metadata: json!({
                "admin_overrides": {
                    "mock_now_rfc3339": "2030-01-01T00:00:00Z"
                }
            }),
            created_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        };

        let restored = Job::restore(record);
        let current_time = schedule(Some("2020-01-01T00:00:00Z"))
            .current_time(&restored)
            .unwrap();

        assert_eq!(current_time, parse_utc("2030-01-01T00:00:00Z"));
    }

    // ----------------------- resolve_step ---------------------------
    //
    // resolve_step must reflect the largest step the request will actually
    // pull from MARS, regardless of whether that step came from a top-level
    // `step` field, a `feature.range`, or trajectory points. This matters
    // because metkit_expansion runs *before* schedule_released and injects a
    // default top-level step for feature requests; if resolve_step only
    // looked at the top-level step it would silently gate every feature
    // retrieval on step 0's release time, defeating the schedule check.

    fn resolve_step_from(request: Value) -> Result<u32, ActionError> {
        let map = request.as_object().expect("test request must be object");
        super::resolve_step(map)
    }

    #[test]
    fn resolve_step_uses_top_level_when_only_top_level_present() {
        let request = json!({ "step": "24" });
        assert_eq!(resolve_step_from(request).unwrap(), 24);
    }

    #[test]
    fn resolve_step_uses_timeseries_range_end_when_only_feature_present() {
        let request = json!({
            "feature": {
                "type": "timeseries",
                "range": {"start": 0, "end": 240}
            }
        });
        assert_eq!(resolve_step_from(request).unwrap(), 240);
    }

    #[test]
    fn resolve_step_takes_max_when_top_level_step_is_smaller_than_feature_range() {
        // The metkit_expansion case: top-level step="0" was injected as a
        // default; the user's intent (range to 240) lives only in the feature.
        let request = json!({
            "step": "0",
            "feature": {
                "type": "timeseries",
                "range": {"start": 0, "end": 240}
            }
        });
        assert_eq!(resolve_step_from(request).unwrap(), 240);
    }

    #[test]
    fn resolve_step_takes_max_when_top_level_step_is_larger_than_feature_range() {
        // The reverse: an explicit top-level step beyond the feature's
        // declared range. Schedule must gate on the larger value because
        // that's what the worker will actually attempt to retrieve.
        let request = json!({
            "step": "360",
            "feature": {
                "type": "timeseries",
                "range": {"start": 0, "end": 24}
            }
        });
        assert_eq!(resolve_step_from(request).unwrap(), 360);
    }

    #[test]
    fn resolve_step_handles_top_level_range_via_max_request_u32() {
        // metkit may emit a slash-separated list (e.g. "0/6/12/240") after
        // expanding a `0/to/240/by/6` form. max_request_u32 picks 240.
        let request = json!({ "step": "0/6/12/240" });
        assert_eq!(resolve_step_from(request).unwrap(), 240);
    }

    #[test]
    fn resolve_step_uses_trajectory_step_axis_when_only_feature_present() {
        let request = json!({
            "feature": {
                "type": "trajectory",
                "axes": ["latitude", "longitude", "step"],
                "points": [[10.0, 20.0, 6], [10.0, 20.0, 240], [10.0, 20.0, 24]]
            }
        });
        assert_eq!(resolve_step_from(request).unwrap(), 240);
    }

    #[test]
    fn resolve_step_combines_trajectory_and_top_level_step() {
        let request = json!({
            "step": "0",
            "feature": {
                "type": "trajectory",
                "axes": ["latitude", "longitude", "step"],
                "points": [[10.0, 20.0, 144]]
            }
        });
        assert_eq!(resolve_step_from(request).unwrap(), 144);
    }

    #[test]
    fn resolve_step_treats_verticalprofile_feature_as_no_step_bound() {
        // verticalprofile carries a level range, not a step; the step must
        // come from the top-level field (which metkit will have injected).
        let request = json!({
            "step": "0",
            "feature": {
                "type": "verticalprofile",
                "points": [[51.5, 0.1]],
                "axes": "levelist",
                "range": {"start": 0, "end": 30}
            }
        });
        assert_eq!(resolve_step_from(request).unwrap(), 0);
    }

    #[test]
    fn resolve_step_errors_when_neither_source_yields_a_step() {
        let request = json!({});
        let err = resolve_step_from(request)
            .expect_err("request without step or feature must report missing step");
        assert!(
            matches!(err, ActionError::ConfigError(ref msg) if msg.contains("'step'")),
            "unexpected error variant: {err:?}",
        );
    }

    #[test]
    fn resolve_step_errors_when_top_level_step_is_malformed() {
        // Malformed top-level step must surface as a config error rather
        // than silently falling through to the feature path.
        let request = json!({ "step": "not-a-number" });
        let err = resolve_step_from(request).expect_err("malformed step must error");
        assert!(matches!(err, ActionError::ConfigError(_)));
    }
}
