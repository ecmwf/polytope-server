// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Minimal internal qube for `/chunks/v1/{collection}/metadata` (Contract v2).
//!
//! We deliberately do NOT depend on the `qubed` Rust crate here. Its
//! `Coordinates::to_string()` panics (`todo!()`) on `Mixed` coordinates and
//! its integer `RangeSet` paths are `unimplemented!()` — both reachable from
//! real catalogue data that mixes integer and string tokens under one node
//! (e.g. `time = 0000/1200`). It also has no notion of canonical MARS-string
//! normalisation, would pull a large git-pinned dependency tree (lasso,
//! slotmap, chrono, rayon, tiny-str, …) into the frontend, and its
//! factorisation is tied to ingest order. For a synchronous server endpoint
//! we want a small, panic-free, deterministic implementation instead.
//!
//! This module:
//!   * parses the catalogue arena JSON (the `{ "version", "qube": [...] }`
//!     envelope produced by qubed's `to_arena_json`, or a bare node array),
//!   * normalises every coordinate to a canonical MARS string (V2.2(3)),
//!   * prune-selects the qube against a canonical request (V2.2(4)),
//!   * factors the selection into maximal dense datacubes with a deterministic
//!     canonical-key-order factorisation (V2.2(5)).

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::expand::axis_rank;

/// A dense datacube: canonical key -> ascending, de-duplicated canonical
/// string values. The cube denotes the full cartesian product of its values.
pub type Cube = BTreeMap<String, Vec<String>>;

/// One arena node with canonical string coordinates.
#[derive(Debug, Clone)]
struct Node {
    dim: String,
    /// Canonical string coordinates. Empty for the root node.
    values: Vec<String>,
    children: Vec<usize>,
}

/// A parsed catalogue qube.
#[derive(Debug, Clone)]
pub struct Qube {
    nodes: Vec<Node>,
    root: usize,
}

/// Normalise a single coordinate token to its canonical MARS string form
/// (V2.2(3)). `time` is zero-padded to `HHMM`; everything else is passed
/// through verbatim (integers already arrive stringified, dates as
/// `YYYYMMDD`).
fn canonical_value(dim: &str, token: &str) -> String {
    if dim == "time" && !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
        // Catalogue time values are HHMM (0, 600, 1200, …); left-pad to 4.
        if token.len() < 4 {
            return format!("{token:0>4}");
        }
    }
    token.to_string()
}

/// Extract the raw string tokens from an arena `coords` value. Handles the
/// typed-object form (`{"ints": [...]}`, `{"strings": [...]}`,
/// `{"ints_text": "a/b"}`, `{"floats": [...]}`, `{"datetimes": [...]}`, and
/// the `Mixed` multi-key object), a bare array, a bare `a/b/c` string, and
/// `null`/absent (the root).
fn coord_tokens(coords: Option<&Value>) -> Vec<String> {
    fn from_scalar(v: &Value) -> Option<String> {
        match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
    fn from_value(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Array(items) => {
                for item in items {
                    from_value(item, out);
                }
            }
            Value::String(s) => {
                for part in s.split('/') {
                    if !part.is_empty() {
                        out.push(part.to_string());
                    }
                }
            }
            Value::Object(map) => {
                // Typed coords object: pull the known typed keys in a fixed
                // order so a Mixed node is handled without panicking.
                for key in ["ints", "ints_text", "strings", "floats", "datetimes"] {
                    if let Some(inner) = map.get(key) {
                        from_value(inner, out);
                    }
                }
            }
            other => {
                if let Some(s) = from_scalar(other) {
                    out.push(s);
                }
            }
        }
    }
    let mut out = Vec::new();
    if let Some(v) = coords {
        from_value(v, &mut out);
    }
    out
}

impl Qube {
    /// Parse a catalogue arena JSON document (bytes).
    pub fn from_arena_json_bytes(bytes: &[u8]) -> Result<Qube, String> {
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|e| format!("catalogue is not valid JSON: {e}"))?;
        Qube::from_arena_json(value)
    }

    /// Parse a catalogue arena JSON [`Value`].
    pub fn from_arena_json(value: Value) -> Result<Qube, String> {
        // Accept both the versioned envelope { "version", "qube": [...] } and
        // a bare array of node records.
        let arr = match value {
            Value::Object(mut map) => match map.remove("qube") {
                Some(Value::Array(a)) => a,
                _ => return Err("catalogue arena JSON missing 'qube' array".to_string()),
            },
            Value::Array(a) => a,
            _ => return Err("catalogue arena JSON must be an object or array".to_string()),
        };
        if arr.is_empty() {
            return Err("catalogue arena JSON has no nodes".to_string());
        }

        let mut nodes: Vec<Node> = Vec::with_capacity(arr.len());
        let mut root: Option<usize> = None;
        for (i, item) in arr.iter().enumerate() {
            let obj = item
                .as_object()
                .ok_or_else(|| format!("catalogue arena node {i} is not an object"))?;
            let dim = obj
                .get("dim")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("catalogue arena node {i} is missing 'dim'"))?
                .to_string();
            let tokens = coord_tokens(obj.get("coords"));
            let values: Vec<String> = dedup_sorted(
                tokens
                    .into_iter()
                    .map(|t| canonical_value(&dim, &t))
                    .collect(),
            );
            let children: Vec<usize> = obj
                .get("children")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|c| c.as_u64().map(|n| n as usize))
                        .collect()
                })
                .unwrap_or_default();
            let is_root = match obj.get("parent") {
                None | Some(Value::Null) => true,
                Some(_) => false,
            };
            if is_root && root.is_none() {
                root = Some(i);
            }
            nodes.push(Node {
                dim,
                values,
                children,
            });
        }
        // Validate children indices.
        for (i, node) in nodes.iter().enumerate() {
            for &c in &node.children {
                if c >= nodes.len() {
                    return Err(format!(
                        "catalogue arena node {i} references out-of-range child {c}"
                    ));
                }
            }
        }
        let root = root.unwrap_or(0);
        Ok(Qube { nodes, root })
    }

    /// The set of dimension names present anywhere in the qube (excludes the
    /// synthetic `root` dimension).
    pub fn dimensions(&self) -> BTreeSet<String> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.root)
            .map(|(_, n)| n.dim.clone())
            .filter(|d| d != "root")
            .collect()
    }

    /// Prune-select the qube against a canonical request (`dim -> allowed
    /// canonical value set`) and return the surviving leaf datacubes.
    ///
    /// Only request keys that actually appear in the qube constrain it; keys
    /// the qube does not branch on are left to the caller (they become pinned
    /// base-request entries or axes). A node whose values do not intersect the
    /// request at all prunes its whole subtree (V2.2(4) prune semantics).
    pub fn select_datacubes(&self, request: &BTreeMap<String, BTreeSet<String>>) -> Vec<Cube> {
        let mut out = Vec::new();
        let mut path: Cube = Cube::new();
        // Walk each child of the (value-less) root.
        for &child in &self.nodes[self.root].children {
            self.walk(child, request, &mut path, &mut out);
        }
        out
    }

    fn walk(
        &self,
        idx: usize,
        request: &BTreeMap<String, BTreeSet<String>>,
        path: &mut Cube,
        out: &mut Vec<Cube>,
    ) {
        let node = &self.nodes[idx];
        let values: Vec<String> = match request.get(&node.dim) {
            Some(allowed) => {
                let hit: Vec<String> = node
                    .values
                    .iter()
                    .filter(|v| allowed.contains(*v))
                    .cloned()
                    .collect();
                if hit.is_empty() {
                    // Empty intersection: prune this subtree.
                    return;
                }
                hit
            }
            None => node.values.clone(),
        };
        // A dimension can only appear once per path in a well-formed qube.
        let inserted = !path.contains_key(&node.dim);
        path.insert(node.dim.clone(), values);
        if node.children.is_empty() {
            out.push(path.clone());
        } else {
            for &child in &node.children {
                self.walk(child, request, path, out);
            }
        }
        if inserted {
            path.remove(&node.dim);
        }
    }
}

/// Sort ascending (canonical string order) and de-duplicate.
fn dedup_sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

/// The union of all dimension names across `cubes`, ordered by the canonical
/// metkit axis order (V2.2(5)); ties broken alphabetically.
pub fn canonical_key_order(cubes: &[Cube]) -> Vec<String> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for cube in cubes {
        keys.extend(cube.keys().cloned());
    }
    let mut keys: Vec<String> = keys.into_iter().collect();
    keys.sort_by(|a, b| axis_rank(a).cmp(&axis_rank(b)).then_with(|| a.cmp(b)));
    keys
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_typed_and_mixed_coords_without_panicking() {
        let arena = json!({
            "version": "1",
            "qube": [
                {"dim": "root", "coords": null, "parent": null, "children": [1]},
                {"dim": "time", "coords": {"ints": [0], "strings": ["1200"]}, "parent": 0, "children": [2]},
                {"dim": "param", "coords": {"strings": ["167", "165"]}, "parent": 1, "children": []},
            ]
        });
        let q = Qube::from_arena_json(arena).unwrap();
        // time 0 -> "0000", 1200 stays "1200", sorted ascending.
        let cubes = q.select_datacubes(&BTreeMap::new());
        assert_eq!(cubes.len(), 1);
        assert_eq!(cubes[0]["time"], vec!["0000", "1200"]);
        assert_eq!(cubes[0]["param"], vec!["165", "167"]);
    }

    #[test]
    fn select_intersects_and_prunes() {
        let arena = json!({
            "qube": [
                {"dim": "root", "coords": null, "parent": null, "children": [1, 2]},
                {"dim": "date", "coords": "20200101/20200102", "parent": 0, "children": [3]},
                {"dim": "date", "coords": "20200103", "parent": 0, "children": [4]},
                {"dim": "param", "coords": "167", "parent": 1, "children": []},
                {"dim": "param", "coords": "228", "parent": 2, "children": []},
            ]
        });
        let q = Qube::from_arena_json(arena).unwrap();
        let mut req = BTreeMap::new();
        req.insert("date".to_string(), set(&["20200101", "20200103"]));
        let cubes = q.select_datacubes(&req);
        // Two surviving branches: {20200101/167} and {20200103/228}.
        assert_eq!(cubes.len(), 2);
    }

}
