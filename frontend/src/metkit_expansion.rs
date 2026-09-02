// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Serialize, Deserialize)]
pub struct MetkitExpansion {}

#[async_trait]
impl TransformAction for MetkitExpansion {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let obj = job
            .request
            .as_object_mut()
            .ok_or_else(|| ActionError::ConfigError("request is not an object".into()))?;

        // Unwrap v1-style {"request": {...}} wrapper if present
        if let Some(inner) = obj.remove("request") {
            if let Some(inner_obj) = inner.as_object() {
                for (k, v) in inner_obj {
                    obj.entry(k.clone()).or_insert(v.clone());
                }
            }
        }

        // Preserve fields metkit can't handle (e.g. "feature" objects)
        let non_mars_keys: Vec<String> = obj
            .iter()
            .filter(|(k, v)| *k != "verb" && v.is_object())
            .map(|(k, _)| k.clone())
            .collect();
        let preserved: Vec<(String, serde_json::Value)> = non_mars_keys
            .into_iter()
            .filter_map(|k| obj.remove(&k).map(|v| (k, v)))
            .collect();

        let had_verb = obj.contains_key("verb");
        obj.entry("verb".to_string())
            .or_insert_with(|| serde_json::json!("retrieve"));

        let original_keys: HashSet<String> = obj.keys().cloned().collect();

        // Detect MARS ranges (e.g. "1/to/100", "0/to/240/by/6") on the
        // fields about to be passed to metkit below, and run a single
        // combined probe to canonicalize their endpoints while keeping the
        // compact range shape -- for FE workers that want to do their own
        // availability-aware expansion instead of consuming a fully
        // enumerated list (see docs/2026-08-19-metkit-range-preservation-plan.md).
        //
        // This is done here -- after the v1-unwrap and non-MARS-key
        // preservation above, but before the real `expand_json` call below
        // -- so detection sees exactly the same field set/shape that metkit
        // itself is about to expand, with no interaction with the
        // preserved "feature" object handling.
        //
        // Purely additive and best-effort: nothing here can reject the job.
        // Any field that isn't a clean, single, well-formed range (or whose
        // probe result doesn't come back as expected) is simply left out of
        // `metkit_ranges` below; the ordinary full-expansion path a few
        // lines down is completely unaffected either way, so the request
        // is always still served correctly -- just without the compact
        // range in metadata for that key. An FE worker that requires a
        // compact range for a given key is responsible for rejecting that
        // case itself.
        let detected_ranges = detect_ranges(obj);
        let metkit_ranges = if detected_ranges.is_empty() {
            serde_json::Map::new()
        } else {
            probe_ranges(obj, &detected_ranges)
        };

        let mut expanded = match metkit::expand_json(&job.request) {
            Ok(v) => v,
            Err(e) => {
                return Ok(TransformResult::Reject {
                    reason: format!("request expansion failed: {e}"),
                    silent: false,
                });
            }
        };

        let covered = feature_covered_keys(&preserved);

        if let Some(exp_obj) = expanded.as_object_mut() {
            if !had_verb {
                exp_obj.remove("verb");
            }
            for key in &covered {
                if !original_keys.contains(key) {
                    exp_obj.remove(key);
                }
            }
            for (k, v) in preserved {
                exp_obj.insert(k, v);
            }
        }

        job.request = expanded;

        if !metkit_ranges.is_empty() {
            let metadata = job.metadata_mut();
            if !metadata.is_object() {
                *metadata = serde_json::json!({});
            }
            metadata
                .as_object_mut()
                .expect("metadata is object after check")
                .insert(
                    "metkit_ranges".to_string(),
                    serde_json::Value::Object(metkit_ranges),
                );
        }

        Ok(TransformResult::Continue)
    }
}

/// A MARS range detected on a single request field, e.g. `"1/to/100"` or
/// `"0/to/240/by/6"`.
struct DetectedRange {
    key: String,
    start: String,
    end: String,
    /// The raw `by` token (just the step value), if present. Already
    /// validated to parse as an integer -- see `detect_ranges`.
    by: Option<String>,
}

/// Render a scalar JSON value the same way metkit's own request builder
/// would (see `render_scalar`/`json_value_to_strings` in
/// `utils/metkit/src/lib.rs`; duplicated here in miniature to keep this
/// action self-contained rather than widening that crate's or
/// `actions::coercion`'s visibility).
fn render_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::Bool(boolean) => boolean.to_string(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Render a request field's value as the token sequence metkit itself would
/// see: a `/`-separated string is split on `/`, an array is taken
/// element-by-element, anything else is a single scalar token.
fn value_tokens(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Array(items) => items.iter().map(render_scalar).collect(),
        serde_json::Value::String(text) => text.split('/').map(|token| token.to_string()).collect(),
        other => vec![render_scalar(other)],
    }
}

/// Scan `obj` for fields matching the MARS range grammar (`a/to/b` or
/// `a/to/b/by/c`, case-insensitive `to`/`by`), whether submitted as a
/// slash-separated string or an explicit JSON array of the same tokens.
///
/// Only single, well-formed ranges are recognised (matching the existing
/// precedent in `actions/coercion.rs` and `actions/date_check.rs`): mixed
/// lists-and-ranges in one field (e.g. `"0/to/12/by/6/24/36"`) are *not*
/// parsed here and are left for the ordinary full-expansion path, exactly as
/// they are today. This is a deliberate scope limitation, not a verified
/// grammar restriction -- see open question 4 in the plan doc.
///
/// A malformed `by` suffix (anything that doesn't parse as an integer) is
/// also treated as "not a recognised range" for the same reason: it simply
/// falls through to full enumeration untouched, rather than being rejected.
fn detect_ranges(obj: &serde_json::Map<String, serde_json::Value>) -> Vec<DetectedRange> {
    let mut ranges = Vec::new();
    for (key, value) in obj {
        if key == "verb" {
            continue;
        }
        let tokens = value_tokens(value);
        let range = match tokens.as_slice() {
            [start, to, end] if to.eq_ignore_ascii_case("to") => {
                Some((start.clone(), end.clone(), None))
            }
            [start, to, end, by_kw, by_val]
                if to.eq_ignore_ascii_case("to") && by_kw.eq_ignore_ascii_case("by") =>
            {
                by_val
                    .parse::<i64>()
                    .ok()
                    .map(|_| (start.clone(), end.clone(), Some(by_val.clone())))
            }
            _ => None,
        };
        if let Some((start, end, by)) = range {
            ranges.push(DetectedRange {
                key: key.clone(),
                start,
                end,
                by,
            });
        }
    }
    ranges
}

/// Run a single combined metkit probe covering every detected range at
/// once: each ranged key's value is replaced by the 2-element list
/// `[start, end]`, all other fields are left exactly as submitted (so
/// cross-key expansion context, e.g. `param` depending on `class`/`stream`,
/// is preserved). This is one metkit call regardless of how many ranged
/// keys the request has.
///
/// Purely best-effort and additive, per the comment in `execute` above: any
/// failure -- the probe call itself erroring, or an individual key not
/// coming back with exactly the expected 1 or 2 canonicalized values --
/// simply omits that key (or all keys, if the whole probe call fails) from
/// the result, and never rejects the job.
fn probe_ranges(
    obj: &serde_json::Map<String, serde_json::Value>,
    detected: &[DetectedRange],
) -> serde_json::Map<String, serde_json::Value> {
    let mut probe_obj = obj.clone();
    for range in detected {
        probe_obj.insert(
            range.key.clone(),
            serde_json::json!([range.start, range.end]),
        );
    }

    let mut result = serde_json::Map::new();
    let probe_expanded = match metkit::expand_json(&serde_json::Value::Object(probe_obj)) {
        Ok(v) => v,
        Err(_) => return result,
    };
    let Some(probe_exp_obj) = probe_expanded.as_object() else {
        return result;
    };

    for range in detected {
        let Some(value) = probe_exp_obj.get(&range.key) else {
            continue;
        };
        let Some(text) = value.as_str() else {
            continue;
        };
        let (norm_start, norm_end) = match text.split('/').collect::<Vec<_>>().as_slice() {
            [a, b] => (a.to_string(), b.to_string()),
            // Degenerate case: start and end canonicalize to the same
            // value (e.g. a genuinely single-element range, or two
            // different spellings of the same value); use it for both
            // endpoints.
            [a] => (a.to_string(), a.to_string()),
            // Anything else is unexpected for a 2-value probe -- skip this
            // key rather than guess.
            _ => continue,
        };
        let by_suffix = range
            .by
            .as_ref()
            .map(|by| format!("/by/{by}"))
            .unwrap_or_default();
        result.insert(
            range.key.clone(),
            serde_json::Value::String(format!("{norm_start}/to/{norm_end}{by_suffix}")),
        );
    }

    result
}

fn feature_covered_keys(preserved: &[(String, serde_json::Value)]) -> HashSet<String> {
    let mut covered = HashSet::new();

    let feature = match preserved
        .iter()
        .find(|(k, _)| k == "feature")
        .and_then(|(_, v)| v.as_object())
    {
        Some(f) => f,
        None => return covered,
    };

    let feature_type = feature.get("type").and_then(|v| v.as_str());
    let has_range = feature.contains_key("range");

    match feature_type {
        Some("timeseries") if has_range => {
            let axis = feature
                .get("time_axis")
                .and_then(|v| v.as_str())
                .or_else(|| feature.get("axes").and_then(|v| v.as_str()));
            if let Some(axis) = axis {
                covered.insert(axis.to_string());
            }
        }
        Some("verticalprofile") if has_range => {
            let axis = feature
                .get("axes")
                .and_then(|v| v.as_str())
                .unwrap_or("levelist");
            covered.insert(axis.to_string());
        }
        Some("trajectory") => {
            for axis in feature_axes_list(feature) {
                if axis == "step" || axis == "levelist" {
                    covered.insert(axis);
                }
            }
        }
        _ => {}
    }

    covered
}

fn feature_axes_list(feature: &serde_json::Map<String, serde_json::Value>) -> Vec<String> {
    match feature.get("axes") {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        Some(serde_json::Value::String(s)) => s.split('/').map(|s| s.trim().to_string()).collect(),
        _ => vec!["latitude".to_string(), "longitude".to_string()],
    }
}

bits::register_action!(transform, "metkit_expansion", MetkitExpansion);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detect_ranges_finds_plain_range() {
        let obj = json!({
            "verb": "retrieve",
            "step": "0/to/240",
            "class": "od"
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].key, "step");
        assert_eq!(ranges[0].start, "0");
        assert_eq!(ranges[0].end, "240");
        assert_eq!(ranges[0].by, None);
    }

    #[test]
    fn detect_ranges_finds_range_with_by() {
        let obj = json!({
            "verb": "retrieve",
            "step": "0/to/240/by/6"
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, "0");
        assert_eq!(ranges[0].end, "240");
        assert_eq!(ranges[0].by.as_deref(), Some("6"));
    }

    #[test]
    fn detect_ranges_accepts_array_form() {
        let obj = json!({
            "verb": "retrieve",
            "levelist": ["1", "to", "100"]
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].key, "levelist");
        assert_eq!(ranges[0].start, "1");
        assert_eq!(ranges[0].end, "100");
    }

    #[test]
    fn detect_ranges_rejects_malformed_by_suffix() {
        // Non-integer "by" -- not a recognised range; left alone entirely.
        let obj = json!({
            "verb": "retrieve",
            "step": "0/to/240/by/six"
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert!(ranges.is_empty());
    }

    #[test]
    fn detect_ranges_ignores_plain_lists_and_scalars() {
        let obj = json!({
            "verb": "retrieve",
            "class": "od",
            "step": ["0", "6", "12", "24"]
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert!(ranges.is_empty());
    }

    #[test]
    fn detect_ranges_ignores_five_tokens_without_to_by_keywords() {
        // Not the "to"/"by" range shape -- just an unusual 5-value list.
        let obj = json!({
            "verb": "retrieve",
            "step": "1/2/3/4/5"
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert!(ranges.is_empty());
    }

    #[test]
    fn detect_ranges_skips_verb_key() {
        let obj = json!({
            "verb": "1/to/100"
        });
        let ranges = detect_ranges(obj.as_object().unwrap());
        assert!(ranges.is_empty());
    }

    // ---------------------------------------------------------------------
    // The tests below exercise the real `metkit::expand_json` C++ binding
    // (via `probe_ranges` and the full `execute()` action) rather than just
    // the pure-Rust `detect_ranges` logic above. They require the `metkit`
    // feature and real eckit/metkit native libraries at build+link time --
    // see docs/2026-08-19-metkit-range-preservation-plan.md for how these
    // were run in an environment without `mars-client-bundle` checked out
    // locally (extracting `eccr.ecmwf.int/polytope/cpp-metkit-libs` inside a
    // matching-glibc container).
    // ---------------------------------------------------------------------

    fn sample_retrieve_request(step: &str) -> serde_json::Map<String, serde_json::Value> {
        json!({
            "verb": "retrieve",
            "class": "od",
            "type": "fc",
            "stream": "oper",
            "expver": "1",
            "date": "20240101",
            "time": "0000",
            "levtype": "sfc",
            "param": "2t",
            "step": step,
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn probe_ranges_canonicalizes_endpoints_via_real_metkit() {
        let obj = sample_retrieve_request("0/to/12");
        let detected = detect_ranges(&obj);
        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].key, "step");

        let ranges = probe_ranges(&obj, &detected);
        assert_eq!(ranges.get("step").and_then(|v| v.as_str()), Some("0/to/12"));
    }

    #[test]
    fn probe_ranges_preserves_by_suffix_verbatim() {
        let obj = sample_retrieve_request("0/to/12/by/6");
        let detected = detect_ranges(&obj);
        assert_eq!(detected.len(), 1);

        let ranges = probe_ranges(&obj, &detected);
        assert_eq!(
            ranges.get("step").and_then(|v| v.as_str()),
            Some("0/to/12/by/6")
        );
    }

    #[tokio::test]
    async fn execute_writes_metkit_ranges_metadata_and_still_fully_expands_request() {
        let action = MetkitExpansion {};
        let mut job = Job::new(serde_json::Value::Object(sample_retrieve_request(
            "0/to/12/by/6",
        )));

        let result = action.execute(&mut job).await.unwrap();
        assert!(matches!(result, TransformResult::Continue));

        // job.request is unaffected: still fully expanded, exactly as before
        // this change -- every other action downstream keeps working off
        // this untouched flat form.
        assert_eq!(job.request.get("step"), Some(&json!("0/6/12")));

        // metadata additionally carries the compact, metkit-canonicalized
        // range for FE workers that want it.
        let ranges = job
            .metadata
            .get("metkit_ranges")
            .expect("metkit_ranges metadata should be present");
        assert_eq!(ranges.get("step"), Some(&json!("0/to/12/by/6")));
    }

    #[tokio::test]
    async fn execute_writes_no_metadata_when_request_has_no_ranges() {
        let action = MetkitExpansion {};
        let mut job = Job::new(serde_json::Value::Object(sample_retrieve_request("0/6/12")));

        action.execute(&mut job).await.unwrap();

        assert!(job.metadata.get("metkit_ranges").is_none());
    }
}
