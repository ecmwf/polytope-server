// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! `/chunks/v1/{collection}/metadata` request parsing and response building
//! (contract §1).

use serde::Serialize;
use serde::ser::SerializeMap;
use serde_json::{Map, Value};

use super::expand::CanonicalRequest;
use crate::config::{ChunksConfig, ChunksGridConfig};

pub const METADATA_VERSION: u32 = 1;

/// A JSON object whose keys serialise in insertion order regardless of
/// serde_json's `preserve_order` feature.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderedMap<V>(pub Vec<(String, V)>);

impl<V: Serialize> Serialize for OrderedMap<V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

#[derive(Debug, Serialize)]
pub struct MetadataResponse {
    pub version: u32,
    pub canonical_request: OrderedMap<Vec<String>>,
    pub axes: Vec<Axis>,
    pub grid: Grid,
    pub variables: Vec<Variable>,
    pub chunking: Chunking,
    pub extract: ExtractInfo,
}

#[derive(Debug, Serialize)]
pub struct Axis {
    pub dim: String,
    pub key: String,
    pub values: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Grid {
    pub kind: &'static str,
    pub count_values: u64,
    #[serde(rename = "md5GridSection")]
    pub md5_grid_section: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Variable {
    pub name: String,
    pub param: String,
}

#[derive(Debug, Serialize)]
pub struct Chunking {
    pub default: OrderedMap<u64>,
    pub max_chunk_cost: u64,
}

#[derive(Debug, Serialize)]
pub struct ExtractInfo {
    pub grid_hash: Option<String>,
    pub order: Vec<String>,
}

/// Validate a metadata body (`{"request": {...}}`) and return the flat MARS
/// request to expand. `verb` is dropped (expansion always uses `retrieve`).
pub fn parse_metadata_body(body: &Value) -> Result<Map<String, Value>, String> {
    let obj = body
        .as_object()
        .ok_or("request body must be a JSON object")?;
    if obj.contains_key("feature") {
        return Err(feature_unsupported());
    }
    let request = obj
        .get("request")
        .ok_or("request body must contain a 'request' object")?
        .as_object()
        .ok_or("'request' must be a JSON object")?;
    parse_flat_request(request)
}

pub(crate) fn feature_unsupported() -> String {
    "'feature' is not supported by /chunks/v1 (v0)".to_string()
}

/// Validate a flat MARS request object: non-empty, no `feature`, values are
/// strings, numbers or non-empty arrays of strings/numbers. Drops `verb`.
pub(crate) fn parse_flat_request(
    request: &Map<String, Value>,
) -> Result<Map<String, Value>, String> {
    if request.contains_key("feature") {
        return Err(feature_unsupported());
    }
    let mut out = Map::new();
    for (key, value) in request {
        if key == "verb" {
            continue;
        }
        let ok = match value {
            Value::String(s) => !s.is_empty(),
            Value::Number(_) => true,
            Value::Array(items) => {
                !items.is_empty()
                    && items
                        .iter()
                        .all(|v| matches!(v, Value::String(_) | Value::Number(_)))
            }
            _ => false,
        };
        if !ok {
            return Err(format!(
                "request key '{key}' must be a non-empty string, a number, or a non-empty list \
                 of strings/numbers"
            ));
        }
        out.insert(key.clone(), value.clone());
    }
    if out.is_empty() {
        return Err("request must not be empty".to_string());
    }
    Ok(out)
}

fn grid_matches(grid: &ChunksGridConfig, collection: &str, canonical: &CanonicalRequest) -> bool {
    grid.collection == collection
        && grid.match_keys.iter().all(|(key, expected)| {
            let Some(allowed) = ChunksGridConfig::allowed_values(expected) else {
                return false;
            };
            canonical
                .get(key)
                .is_some_and(|values| values.iter().all(|v| allowed.contains(v)))
        })
}

/// First grid-registry entry matching `collection` + `canonical` (contract §6).
pub fn find_grid<'a>(
    config: &'a ChunksConfig,
    collection: &str,
    canonical: &CanonicalRequest,
) -> Option<&'a ChunksGridConfig> {
    config
        .grids
        .iter()
        .find(|grid| grid_matches(grid, collection, canonical))
}

/// Build the contract §1 metadata response from a canonical request.
pub fn build_metadata(
    config: &ChunksConfig,
    collection: &str,
    canonical: &CanonicalRequest,
) -> Result<MetadataResponse, String> {
    let params = canonical
        .get("param")
        .filter(|p| !p.is_empty())
        .ok_or("request expanded to no 'param' value")?;

    let axes: Vec<Axis> = canonical
        .entries
        .iter()
        .filter(|(key, values)| key != "param" && values.len() > 1)
        .map(|(key, values)| Axis {
            dim: key.clone(),
            key: key.clone(),
            values: values.clone(),
        })
        .collect();

    if axes.iter().any(|a| a.dim == "values") {
        return Err("request key 'values' clashes with the grid dimension name".to_string());
    }

    let grid = find_grid(config, collection, canonical).ok_or_else(|| {
        format!(
            "no grid is registered for this request in collection '{collection}' \
             (chunks.grids)"
        )
    })?;

    let variables = params
        .iter()
        .map(|param| Variable {
            // v0: metkit's expansion does not expose shortnames through the
            // bridge; the canonical param id doubles as the variable name.
            name: param.clone(),
            param: param.clone(),
        })
        .collect();

    let mut default_chunks: Vec<(String, u64)> = axes.iter().map(|a| (a.dim.clone(), 1)).collect();
    default_chunks.push(("values".to_string(), grid.count_values));

    let order: Vec<String> = axes.iter().map(|a| a.dim.clone()).collect();

    Ok(MetadataResponse {
        version: METADATA_VERSION,
        canonical_request: OrderedMap(canonical.entries.clone()),
        axes,
        grid: Grid {
            kind: "unstructured",
            count_values: grid.count_values,
            md5_grid_section: grid.md5_grid_section.clone(),
        },
        variables,
        chunking: Chunking {
            default: OrderedMap(default_chunks),
            max_chunk_cost: config.max_chunk_cost,
        },
        extract: ExtractInfo {
            grid_hash: grid.md5_grid_section.clone(),
            order,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn canonical(entries: &[(&str, &[&str])]) -> CanonicalRequest {
        CanonicalRequest {
            entries: entries
                .iter()
                .map(|(k, vs)| (k.to_string(), vs.iter().map(|v| v.to_string()).collect()))
                .collect(),
        }
    }

    fn config(yaml: &str) -> ChunksConfig {
        let cfg: ChunksConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        cfg
    }

    #[test]
    fn grid_match_requires_every_request_value_to_be_allowed() {
        let cfg = config(
            r#"
grids:
  - collection: c
    match: { resolution: high, levtype: [sfc, pl] }
    count_values: 10
"#,
        );
        let ok = canonical(&[("levtype", &["sfc"]), ("resolution", &["high"])]);
        assert!(find_grid(&cfg, "c", &ok).is_some());
        assert!(find_grid(&cfg, "other", &ok).is_none());
        let mixed = canonical(&[("levtype", &["sfc"]), ("resolution", &["high", "standard"])]);
        assert!(find_grid(&cfg, "c", &mixed).is_none());
        let missing = canonical(&[("levtype", &["sfc"])]);
        assert!(find_grid(&cfg, "c", &missing).is_none());
    }

    #[test]
    fn first_matching_grid_wins_and_empty_match_is_catch_all() {
        let cfg = config(
            r#"
grids:
  - collection: c
    match: { resolution: high }
    count_values: 1
  - collection: c
    count_values: 2
"#,
        );
        let high = canonical(&[("resolution", &["high"])]);
        let std = canonical(&[("resolution", &["standard"])]);
        assert_eq!(find_grid(&cfg, "c", &high).unwrap().count_values, 1);
        assert_eq!(find_grid(&cfg, "c", &std).unwrap().count_values, 2);
    }

    #[test]
    fn numeric_match_values_compare_as_canonical_strings() {
        let cfg = config(
            r#"
grids:
  - collection: c
    match: { generation: 1 }
    count_values: 1
"#,
        );
        assert!(find_grid(&cfg, "c", &canonical(&[("generation", &["1"])])).is_some());
    }

    #[test]
    fn metadata_with_no_axes_is_single_field() {
        let cfg = config("grids: [{collection: c, count_values: 5}]");
        let md = build_metadata(
            &cfg,
            "c",
            &canonical(&[("class", &["od"]), ("param", &["167"])]),
        )
        .unwrap();
        let v = serde_json::to_value(&md).unwrap();
        assert_eq!(v["axes"], json!([]));
        assert_eq!(v["extract"]["order"], json!([]));
        assert_eq!(v["chunking"]["default"], json!({"values": 5}));
        assert_eq!(v["grid"]["md5GridSection"], Value::Null);
        assert_eq!(v["extract"]["grid_hash"], Value::Null);
        assert_eq!(v["chunking"]["max_chunk_cost"], json!(20_000_000));
    }

    #[test]
    fn parse_metadata_body_validation() {
        assert!(parse_metadata_body(&json!([])).is_err());
        assert!(parse_metadata_body(&json!({})).is_err());
        assert!(parse_metadata_body(&json!({"request": "class=od"})).is_err());
        assert!(parse_metadata_body(&json!({"request": {}})).is_err());
        assert!(parse_metadata_body(&json!({"request": {"verb": "retrieve"}})).is_err());
        assert!(parse_metadata_body(&json!({"request": {"a": {"x": 1}}})).is_err());
        assert!(parse_metadata_body(&json!({"request": {"a": []}})).is_err());
        assert!(parse_metadata_body(&json!({"request": {"a": [true]}})).is_err());
        let err = parse_metadata_body(&json!({"request": {"a": "1"}, "feature": {}})).unwrap_err();
        assert!(err.contains("feature"));
        let err =
            parse_metadata_body(&json!({"request": {"feature": {"type": "polygon"}}})).unwrap_err();
        assert!(err.contains("feature"));
        let ok =
            parse_metadata_body(&json!({"request": {"verb": "retrieve", "a": [1, "2"]}})).unwrap();
        assert_eq!(Value::Object(ok), json!({"a": [1, "2"]}));
    }
}
