// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! `/chunks/v1/{collection}/metadata` request parsing and response building.
//!
//! Contract v2 (`polytope-zarr-contract.md`): the response is a qube-derived
//! **structure tree** (`version: 2`). The request gains one optional field,
//! `structure.gaps` (`"exact"` | `"span"`).

use serde::ser::SerializeMap;
use serde::Serialize;
use serde_json::{Map, Value};

use super::catalogue::QubeHandle;
use super::expand::CanonicalRequest;
use super::feature::{PolygonRequest, ResolvedFeature};
use super::qube::Cube;
use super::tree;
use crate::config::{ChunksConfig, ChunksGridConfig};

pub const METADATA_VERSION: u32 = 2;

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

/// Gap policy for the `date` axis (Contract v2 §V2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gaps {
    /// Keep the catalogued (possibly non-contiguous) date values.
    Exact,
    /// Replace the `date` axis with the full daily calendar between its min
    /// and max, and set `fill_on_missing: true`.
    Span,
}

/// A parsed metadata request body.
#[derive(Debug)]
pub struct MetadataBody {
    pub request: Map<String, Value>,
    pub gaps: Gaps,
    pub feature: Option<PolygonRequest>,
}

// ---------------------------------------------------------------------------
// Response shape (Contract v2 §V2.3)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct MetadataResponse {
    pub version: u32,
    pub canonical_request: OrderedMap<Vec<String>>,
    pub tree: Node,
    pub chunking: Chunking,
    pub catalogue: CatalogueInfo,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Node {
    Group {
        name: String,
        attrs: GroupAttrs,
        children: Vec<Node>,
    },
    ArraySet {
        name: String,
        base_request: OrderedMap<String>,
        axes: Vec<Axis>,
        variables: Vec<Variable>,
        grid: Grid,
        #[serde(skip_serializing_if = "Option::is_none")]
        feature: Option<Box<ResolvedFeature>>,
        fill_on_missing: bool,
        extract: ExtractInfo,
    },
}

impl Node {
    /// Overwrite this node's `name` (used when a parent names its children).
    pub fn set_name(&mut self, new_name: String) {
        match self {
            Node::Group { name, .. } | Node::ArraySet { name, .. } => *name = new_name,
        }
    }

    /// Collect every distinct axis `dim` in the subtree, in canonical order.
    pub fn collect_axis_dims(&self, out: &mut Vec<String>) {
        match self {
            Node::Group { children, .. } => {
                for child in children {
                    child.collect_axis_dims(out);
                }
            }
            Node::ArraySet { axes, .. } => {
                for axis in axes {
                    if !out.contains(&axis.dim) {
                        out.push(axis.dim.clone());
                    }
                }
            }
        }
    }

    fn max_axis_len(&self, dim: &str) -> Option<u64> {
        match self {
            Node::Group { children, .. } => children
                .iter()
                .filter_map(|child| child.max_axis_len(dim))
                .max(),
            Node::ArraySet { axes, .. } => axes
                .iter()
                .find(|axis| axis.dim == dim)
                .map(|axis| axis.values.len() as u64),
        }
    }

    fn max_feature_points(&self) -> Option<u64> {
        match self {
            Node::Group { children, .. } => {
                children.iter().filter_map(Node::max_feature_points).max()
            }
            Node::ArraySet { feature, .. } => feature.as_ref().map(|value| value.n_points),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct GroupAttrs {
    pub defined_by: OrderedMap<Vec<String>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<&'static str>,
    pub count_values: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nside: Option<u32>,
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
    pub max_fields_per_job: u64,
    pub max_multi_chunks: usize,
}

#[derive(Debug, Serialize)]
pub struct ExtractInfo {
    pub grid_hash: Option<String>,
    pub order: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CatalogueInfo {
    pub source: String,
    pub etag: Option<String>,
    pub advisory: bool,
}

// ---------------------------------------------------------------------------
// Request parsing
// ---------------------------------------------------------------------------

/// Validate a Contract v2.1 metadata body and return the flat MARS request,
/// gap policy, and optional polygon feature. `verb` is dropped.
pub fn parse_metadata_body(body: &Value) -> Result<MetadataBody, String> {
    let obj = body
        .as_object()
        .ok_or("request body must be a JSON object")?;
    let request = obj
        .get("request")
        .ok_or("request body must contain a 'request' object")?
        .as_object()
        .ok_or("'request' must be a JSON object")?;
    let request = parse_flat_request(request)?;
    let gaps = parse_gaps(obj.get("structure"))?;
    let feature = super::feature::parse_feature(obj.get("feature"))?;
    Ok(MetadataBody {
        request,
        gaps,
        feature,
    })
}

fn parse_gaps(structure: Option<&Value>) -> Result<Gaps, String> {
    let Some(structure) = structure else {
        return Ok(Gaps::Exact);
    };
    let structure = structure
        .as_object()
        .ok_or("'structure' must be a JSON object")?;
    match structure.get("gaps") {
        None | Some(Value::Null) => Ok(Gaps::Exact),
        Some(Value::String(s)) if s == "exact" => Ok(Gaps::Exact),
        Some(Value::String(s)) if s == "span" => Ok(Gaps::Span),
        Some(_) => Err("structure.gaps must be \"exact\" or \"span\"".to_string()),
    }
}

pub(crate) fn feature_unsupported() -> String {
    "'feature' is supported only as a top-level metadata field".to_string()
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

// ---------------------------------------------------------------------------
// Grid registry (Contract §6, applied per cube in v2 §V2.2(7))
// ---------------------------------------------------------------------------

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

/// First grid-registry entry matching `collection` + `canonical` (Contract §6).
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

// ---------------------------------------------------------------------------
// Contract v2 derivation
// ---------------------------------------------------------------------------

/// Build the Contract v2 metadata response: intersect the canonical request
/// with the catalogue qube, factor it into dense cubes, and assemble the
/// structure tree.
pub fn build_metadata_v2(
    config: &ChunksConfig,
    collection: &str,
    canonical: &CanonicalRequest,
    user_keys: &std::collections::BTreeSet<String>,
    handle: &QubeHandle,
    gaps: Gaps,
    feature: Option<&PolygonRequest>,
) -> Result<MetadataResponse, String> {
    // metkit fills unsupplied keys with MARS defaults (e.g. a default `date`,
    // `time` or `param`). Those defaults must NOT narrow the catalogue
    // intersection: only keys the USER actually supplied may constrain the qube.
    // So drop metkit-default values for any key the qube branches on that the
    // user did not supply — the qube then contributes that key's full value set
    // as an axis (or as `param` variables). Metkit defaults for keys the qube
    // does NOT branch on are kept: they are genuine MARS pins needed for extract.
    let qube_dims = handle.qube.dimensions();
    let effective: Vec<(String, Vec<String>)> = canonical
        .entries
        .iter()
        .filter(|(k, _)| user_keys.contains(k.as_str()) || !qube_dims.contains(k.as_str()))
        .cloned()
        .collect();

    // Effective request as dim -> allowed value set (user-supplied keys only
    // constrain the qube; non-branch keys are ignored by select_datacubes).
    let request: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = effective
        .iter()
        .map(|(k, vs)| (k.clone(), vs.iter().cloned().collect()))
        .collect();

    // Intersect with the qube (prune semantics).
    let mut datacubes: Vec<Cube> = handle.qube.select_datacubes(&request);
    if datacubes.is_empty() {
        return Err(
            "the request selects nothing in this collection's catalogue (empty intersection)"
                .to_string(),
        );
    }

    // Keys the user constrained that the qube does not branch on: pin them into
    // every cube with their canonical values (single-valued -> base_request,
    // multi-valued -> axis). This lets the catalogue carry MORE structure than
    // the request without dropping request-only pins.
    let request_only: Vec<(String, Vec<String>)> = effective
        .iter()
        .filter(|(k, _)| !qube_dims.contains(k.as_str()))
        .map(|(k, vs)| {
            let mut vs = vs.clone();
            vs.sort();
            vs.dedup();
            (k.clone(), vs)
        })
        .collect();
    for cube in &mut datacubes {
        for (k, vs) in &request_only {
            cube.entry(k.clone()).or_insert_with(|| vs.clone());
        }
    }

    // The dense cubes are the qube's root-to-leaf datacubes (the catalogue is
    // already canonically factored by qubed). We do NOT re-factor: a pure
    // datacube re-factorisation would wrongly merge cubes across param-footprint
    // and grid boundaries that the catalogue deliberately keeps separate.
    // Determinism comes from the (qubed-canonical) qube plus ascending value
    // sorting and canonical-key-order tree divergence.
    let mut tree = tree::build_tree(&datacubes, config, collection, gaps)?;
    if let Some(polygon) = feature {
        tree::attach_feature(&mut tree, polygon, config.max_feature_points)?;
    }

    let default_chunks = default_chunking(
        &tree,
        config.default_max_fields_per_job,
        config.max_chunk_cost,
    );

    Ok(MetadataResponse {
        version: METADATA_VERSION,
        canonical_request: OrderedMap(effective.clone()),
        tree,
        chunking: Chunking {
            default: OrderedMap(default_chunks),
            max_chunk_cost: config.max_chunk_cost,
            max_fields_per_job: config.default_max_fields_per_job,
            max_multi_chunks: config.max_multi_chunks,
        },
        catalogue: CatalogueInfo {
            source: handle.source.clone(),
            etag: handle.etag.clone(),
            advisory: true,
        },
    })
}

fn default_chunking(
    tree: &Node,
    configured_max_fields: u64,
    max_chunk_cost: u64,
) -> Vec<(String, u64)> {
    let mut axis_dims = Vec::new();
    tree.collect_axis_dims(&mut axis_dims);
    let date_len = tree.max_axis_len("date");
    let time_len = tree.max_axis_len("time");
    let feature_points = tree.max_feature_points();
    let max_fields_per_job =
        feature_points.map(|points| configured_max_fields.min((max_chunk_cost / points).max(1)));
    let mut temporal_chunks = std::collections::BTreeMap::new();
    // Feature series pack the temporal axes up to the per-job field target, whether
    // or not the series exceeds it: a short series becomes one job, not one job per
    // field (a 90-day hourly point series was 2,160 jobs, enough to exhaust the
    // ingress's per-connection request limit). Field-shaped arrays (no feature) keep
    // one field per chunk.
    if let Some(max_fields_per_job) = max_fields_per_job {
        let mut remaining = max_fields_per_job;
        if let Some(len) = time_len {
            let chunk = len.min(remaining).max(1);
            temporal_chunks.insert("time", chunk);
            remaining = (remaining / chunk).max(1);
        }
        if let Some(len) = date_len {
            temporal_chunks.insert("date", len.min(remaining).max(1));
        }
    }

    let mut chunks = axis_dims
        .into_iter()
        .map(|dim| {
            let size = temporal_chunks.get(dim.as_str()).copied().unwrap_or(1);
            (dim, size)
        })
        .collect::<Vec<_>>();
    chunks.push((
        if feature_points.is_some() {
            "points"
        } else {
            "values"
        }
        .to_string(),
        0,
    ));
    chunks
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
        assert_eq!(Value::Object(ok.request), json!({"a": [1, "2"]}));
        assert_eq!(ok.gaps, Gaps::Exact);
        assert!(ok.feature.is_none());
        let with_feature = parse_metadata_body(&json!({
            "request": {"a": "1"},
            "feature": {"type": "polygon", "shape": [[0, 0], [1, 0], [0, 1]]},
        }))
        .unwrap();
        assert!(with_feature.feature.is_some());
    }

    #[test]
    fn parse_gaps_default_and_values() {
        let exact = parse_metadata_body(&json!({"request": {"a": "1"}})).unwrap();
        assert_eq!(exact.gaps, Gaps::Exact);
        let span =
            parse_metadata_body(&json!({"request": {"a": "1"}, "structure": {"gaps": "span"}}))
                .unwrap();
        assert_eq!(span.gaps, Gaps::Span);
        assert!(
            parse_metadata_body(&json!({"request": {"a": "1"}, "structure": {"gaps": "wat"}}))
                .is_err()
        );
        assert!(parse_metadata_body(&json!({"request": {"a": "1"}, "structure": []})).is_err());
    }
}
