// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Polytope-mars feature validation, worker job construction, and geometry cache.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use super::metadata::Node;

const SUPPORTED_TYPES: &[&str] = &[
    "polygon",
    "boundingbox",
    "timeseries",
    "verticalprofile",
    "circle",
    "position",
];

#[derive(Debug, Clone, PartialEq)]
pub struct FeatureRequest {
    feature_type: String,
    value: Value,
    canonical_json: String,
}

impl FeatureRequest {
    pub fn feature_type(&self) -> &str {
        &self.feature_type
    }

    fn value(&self) -> &Value {
        &self.value
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedFeature {
    #[serde(rename = "type")]
    pub feature_type: String,
    pub n_points: u64,
    pub ranges: Vec<[u64; 2]>,
    pub coords: FeatureCoords,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureCoords {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
}

pub fn parse_feature(value: Option<&Value>) -> Result<Option<FeatureRequest>, String> {
    value.map(parse_feature_value).transpose()
}

fn parse_feature_value(value: &Value) -> Result<FeatureRequest, String> {
    let object = value.as_object().ok_or("'feature' must be a JSON object")?;
    let feature_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or("feature.type must be a string")?;
    if matches!(feature_type, "trajectory" | "path") {
        return Err(format!(
            "unsupported feature type '{feature_type}'; trajectory/path features do not yet reduce to a fixed spatial point set"
        ));
    }
    if !SUPPORTED_TYPES.contains(&feature_type) {
        return Err(format!(
            "unsupported feature type '{feature_type}'; supported polytope-mars feature types are {}",
            SUPPORTED_TYPES.join(", ")
        ));
    }
    match feature_type {
        "polygon" => {
            let vertices = object
                .get("shape")
                .and_then(Value::as_array)
                .ok_or("polygon feature must contain a shape array")?;
            if vertices.len() < 3 {
                return Err("polygon feature.shape must contain at least 3 vertices".to_string());
            }
        }
        "boundingbox" => {
            if object
                .get("points")
                .and_then(Value::as_array)
                .is_none_or(|points| points.len() != 2)
            {
                return Err("boundingbox feature.points must contain two points".to_string());
            }
        }
        "timeseries" | "verticalprofile" | "position" => {
            if object
                .get("points")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty)
            {
                return Err(format!("{feature_type} feature.points must not be empty"));
            }
        }
        "circle" if (!object.contains_key("center") || !object.contains_key("radius")) => {
            return Err("circle feature must contain center and radius".to_string());
        }
        _ => {}
    }
    if matches!(feature_type, "timeseries" | "verticalprofile") && object.contains_key("range") {
        return Err(format!(
            "{feature_type} feature.range is not supported by /chunks/v1; select time/level values in the MARS request so they remain zarr dimensions"
        ));
    }
    let canonical_json = canonical_json(value)?;
    Ok(FeatureRequest {
        feature_type: feature_type.to_string(),
        value: value.clone(),
        canonical_json,
    })
}

fn canonical_json(value: &Value) -> Result<String, String> {
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut keys: Vec<_> = object.keys().collect();
                keys.sort();
                let mut sorted = Map::new();
                for key in keys {
                    sorted.insert(key.clone(), canonical(&object[key]));
                }
                Value::Object(sorted)
            }
            Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&canonical(value))
        .map_err(|error| format!("feature cannot be canonicalised: {error}"))
}

fn grid_cache_key(grid_hash: Option<&str>, feature: &FeatureRequest) -> String {
    format!(
        "{}\n{}",
        grid_hash.unwrap_or("null"),
        feature.canonical_json
    )
}

#[derive(Debug)]
struct CachedFeature {
    inserted: Instant,
    value: ResolvedFeature,
}

#[derive(Debug, Default)]
struct FeatureCacheInner {
    values: HashMap<String, CachedFeature>,
    lru: VecDeque<String>,
}

#[derive(Debug)]
pub struct FeatureCache {
    ttl: Duration,
    capacity: usize,
    inner: Mutex<FeatureCacheInner>,
}

impl FeatureCache {
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity,
            inner: Mutex::new(FeatureCacheInner::default()),
        }
    }

    pub fn get(&self, key: &str) -> Option<ResolvedFeature> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        inner
            .values
            .retain(|_, entry| now.duration_since(entry.inserted) <= self.ttl);
        let value = inner.values.get(key)?.value.clone();
        inner.lru.retain(|candidate| candidate != key);
        inner.lru.push_back(key.to_string());
        Some(value)
    }

    pub fn insert(&self, key: String, value: ResolvedFeature) {
        if self.capacity == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        inner
            .values
            .retain(|_, entry| now.duration_since(entry.inserted) <= self.ttl);
        inner.lru.retain(|candidate| candidate != &key);
        while inner.values.len() >= self.capacity {
            let Some(oldest) = inner.lru.pop_front() else {
                inner.values.clear();
                break;
            };
            inner.values.remove(&oldest);
        }
        inner.lru.push_back(key.clone());
        inner.values.insert(
            key,
            CachedFeature {
                inserted: now,
                value,
            },
        );
    }
}

#[derive(Debug)]
pub struct ResolveTarget {
    pub cache_key: String,
    pub grid_hash: Option<String>,
    pub count_values: u64,
    pub job: Value,
}

pub fn resolve_targets(
    tree: &Node,
    feature: &FeatureRequest,
    max_feature_points: u64,
) -> Result<Vec<ResolveTarget>, String> {
    fn visit(
        node: &Node,
        feature: &FeatureRequest,
        max_feature_points: u64,
        targets: &mut HashMap<String, ResolveTarget>,
    ) -> Result<(), String> {
        match node {
            Node::Group { children, .. } => {
                for child in children {
                    visit(child, feature, max_feature_points, targets)?;
                }
            }
            Node::ArraySet {
                name,
                base_request,
                axes,
                variables,
                grid,
                extract,
                ..
            } => {
                let set_name = if name.is_empty() { "<root>" } else { name };
                let variable = variables.first().ok_or_else(|| {
                    format!("array_set '{set_name}' has no representative variable")
                })?;
                let mut job = Map::new();
                for (key, value) in &base_request.0 {
                    job.insert(key.clone(), Value::String(value.clone()));
                }
                for axis in axes {
                    let value = axis.values.first().ok_or_else(|| {
                        format!("array_set '{set_name}' axis '{}' is empty", axis.key)
                    })?;
                    job.insert(axis.key.clone(), Value::String(value.clone()));
                }
                job.insert("param".to_string(), Value::String(variable.param.clone()));
                job.insert("feature".to_string(), feature.value().clone());
                job.insert(
                    "feature_resolve".to_string(),
                    json!({
                        "grid_hash": extract.grid_hash,
                        "count_values": grid.count_values,
                        "max_feature_points": max_feature_points,
                    }),
                );
                let cache_key = grid_cache_key(extract.grid_hash.as_deref(), feature);
                targets.entry(cache_key.clone()).or_insert(ResolveTarget {
                    cache_key,
                    grid_hash: extract.grid_hash.clone(),
                    count_values: grid.count_values,
                    job: Value::Object(job),
                });
            }
        }
        Ok(())
    }

    let mut targets = HashMap::new();
    visit(tree, feature, max_feature_points, &mut targets)?;
    let mut targets: Vec<_> = targets.into_values().collect();
    targets.sort_by(|left, right| left.cache_key.cmp(&right.cache_key));
    Ok(targets)
}

pub fn validate_resolved(
    feature: &FeatureRequest,
    resolved: &ResolvedFeature,
    count_values: u64,
    max_feature_points: u64,
) -> Result<(), String> {
    if resolved.feature_type != feature.feature_type() {
        return Err(format!(
            "feature worker returned type '{}' for requested type '{}'",
            resolved.feature_type,
            feature.feature_type()
        ));
    }
    if resolved.n_points == 0 {
        return Err(format!(
            "{} feature selects no grid points",
            feature.feature_type()
        ));
    }
    if resolved.n_points > max_feature_points {
        return Err(format!(
            "{} selects {} points, exceeding chunks.max_feature_points={max_feature_points}",
            feature.feature_type(),
            resolved.n_points
        ));
    }
    let mut total = 0_u64;
    let mut previous_hi = 0_u64;
    for (index, [lo, hi]) in resolved.ranges.iter().copied().enumerate() {
        if lo >= hi || hi > count_values || (index > 0 && lo <= previous_hi) {
            return Err(
                "feature worker returned invalid, overlapping, or unsorted ranges".to_string(),
            );
        }
        total = total
            .checked_add(hi - lo)
            .ok_or("feature worker point count overflow")?;
        previous_hi = hi;
    }
    if total != resolved.n_points {
        return Err(format!(
            "feature worker ranges select {total} points but n_points is {}",
            resolved.n_points
        ));
    }
    let expected = usize::try_from(resolved.n_points)
        .map_err(|_| "feature worker point count is too large for this server")?;
    if resolved.coords.lat.len() != expected || resolved.coords.lon.len() != expected {
        return Err("feature worker coordinate lengths do not equal n_points".to_string());
    }
    if resolved
        .coords
        .lat
        .iter()
        .chain(&resolved.coords.lon)
        .any(|coordinate| !coordinate.is_finite())
    {
        return Err("feature worker returned non-finite coordinates".to_string());
    }
    Ok(())
}

pub fn attach_resolved(
    node: &mut Node,
    feature: &FeatureRequest,
    resolved: &HashMap<String, ResolvedFeature>,
) -> Result<(), String> {
    match node {
        Node::Group { children, .. } => {
            for child in children {
                attach_resolved(child, feature, resolved)?;
            }
        }
        Node::ArraySet {
            extract,
            feature: slot,
            ..
        } => {
            let key = grid_cache_key(extract.grid_hash.as_deref(), feature);
            let value = resolved
                .get(&key)
                .ok_or("feature resolution missing for an array-set grid")?;
            *slot = Some(Box::new(value.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_supported_types_and_rejects_temporal_ranges_and_paths() {
        let cases = [
            json!({"type": "polygon", "shape": [[0, 0], [1, 0], [0, 1]]}),
            json!({"type": "boundingbox", "points": [[0, 0], [1, 1]]}),
            json!({"type": "timeseries", "points": [[0, 0]], "time_axis": "date"}),
            json!({"type": "verticalprofile", "points": [[0, 0]]}),
            json!({"type": "circle", "center": [[0, 0]], "radius": 1}),
            json!({"type": "position", "points": [[0, 0]]}),
        ];
        for value in &cases {
            let expected = value["type"].as_str().unwrap();
            let parsed = parse_feature(Some(value)).unwrap().unwrap();
            assert_eq!(parsed.feature_type(), expected);
        }
        let error = parse_feature(Some(&json!({"type": "trajectory", "points": []}))).unwrap_err();
        assert!(error.contains("do not yet reduce"));
        let error = parse_feature(Some(&json!({
            "type": "timeseries",
            "points": [[52.51, 13.46]],
            "time_axis": "date",
            "range": {"start": 1, "end": 2}
        })))
        .unwrap_err();
        assert!(error.contains("remain zarr dimensions"));
    }

    #[test]
    fn cache_is_canonical_bounded_and_expires() {
        let first = parse_feature(Some(&json!({
            "type": "polygon",
            "shape": [[0, 0], [1, 0], [0, 1]]
        })))
        .unwrap()
        .unwrap();
        let reordered = parse_feature(Some(&json!({
            "shape": [[0, 0], [1, 0], [0, 1]],
            "type": "polygon"
        })))
        .unwrap()
        .unwrap();
        assert_eq!(
            grid_cache_key(Some("abc"), &first),
            grid_cache_key(Some("abc"), &reordered)
        );
        let cache = FeatureCache::new(Duration::from_secs(60), 1);
        let value = ResolvedFeature {
            feature_type: "polygon".to_string(),
            n_points: 1,
            ranges: vec![[2, 3]],
            coords: FeatureCoords {
                lat: vec![1.0],
                lon: vec![2.0],
            },
        };
        cache.insert("one".to_string(), value.clone());
        assert_eq!(cache.get("one"), Some(value.clone()));
        cache.insert("two".to_string(), value);
        assert!(cache.get("one").is_none());
        assert!(cache.get("two").is_some());
    }

    #[test]
    fn validates_caps_ranges_and_coordinates() {
        let feature = parse_feature(Some(&json!({"type": "position", "points": [[1, 2]]})))
            .unwrap()
            .unwrap();
        let valid = ResolvedFeature {
            feature_type: "position".to_string(),
            n_points: 2,
            ranges: vec![[2, 4]],
            coords: FeatureCoords {
                lat: vec![1.0, 2.0],
                lon: vec![3.0, 4.0],
            },
        };
        validate_resolved(&feature, &valid, 10, 2).unwrap();
        assert!(validate_resolved(&feature, &valid, 10, 1)
            .unwrap_err()
            .contains("max_feature_points"));
    }
}
