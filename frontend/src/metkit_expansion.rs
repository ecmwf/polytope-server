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
        Ok(TransformResult::Continue)
    }
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
