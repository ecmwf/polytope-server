// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Structure-tree assembly for Contract v2 metadata (§V2.2(6-8), §V2.3).
//!
//! Turns the deterministic list of dense datacubes into the response tree:
//! a single cube becomes the root `array_set`; multiple cubes are nested into
//! `group`s at divergence points (in canonical key order), each leaf an
//! `array_set` with pinned `base_request`, `axes`, per-param `variables`, a
//! grid-registry match, and `date` gap handling.

use std::collections::BTreeMap;

use chrono::NaiveDate;

use super::expand::{axis_rank, CanonicalRequest};
use super::metadata::{
    find_grid, Axis, ExtractInfo, Gaps, Grid, GroupAttrs, Node, OrderedMap, Variable,
};
use super::qube::Cube;
use crate::config::ChunksConfig;

/// Build the response tree from the factored dense cubes.
pub fn build_tree(
    cubes: &[Cube],
    config: &ChunksConfig,
    collection: &str,
    gaps: Gaps,
) -> Result<Node, String> {
    if cubes.is_empty() {
        return Err("no dense cubes to build a tree from".to_string());
    }
    let keys = super::qube::canonical_key_order(cubes);
    let mut root = build_subtree(cubes, &keys, config, collection, gaps)?;
    // The root array_set / group is anonymous (the client uses
    // canonical_request attrs for it).
    root.set_name(String::new());
    Ok(root)
}

fn build_subtree(
    cubes: &[Cube],
    keys: &[String],
    config: &ChunksConfig,
    collection: &str,
    gaps: Gaps,
) -> Result<Node, String> {
    if cubes.len() == 1 {
        return build_array_set(&cubes[0], config, collection, gaps);
    }

    // Divergence point: the first key (canonical order) on which the cubes
    // carry more than one distinct value-set.
    let split_key = keys
        .iter()
        .find(|k| distinct_value_sets(cubes, k) > 1)
        .cloned();

    let Some(split_key) = split_key else {
        // All cubes agree on every key yet there is more than one: identical
        // duplicates. Collapse to one array_set.
        return build_array_set(&cubes[0], config, collection, gaps);
    };

    // Partition cubes by their value-set for the split key (deterministic via
    // BTreeMap key ordering).
    let mut groups: BTreeMap<Vec<String>, Vec<Cube>> = BTreeMap::new();
    for cube in cubes {
        let vs = cube.get(&split_key).cloned().unwrap_or_default();
        groups.entry(vs).or_default().push(cube.clone());
    }

    let mut children = Vec::new();
    let mut base_names = Vec::new();
    for (valueset, subcubes) in &groups {
        let child = build_subtree(subcubes, keys, config, collection, gaps)?;
        base_names.push(name_from_values(&split_key, valueset));
        children.push(child);
    }
    for (child, name) in children.iter_mut().zip(uniquify(base_names)) {
        child.set_name(name);
    }

    Ok(Node::Group {
        name: String::new(),
        attrs: GroupAttrs {
            defined_by: OrderedMap(common_pinned(cubes)),
        },
        children,
    })
}

fn build_array_set(
    cube: &Cube,
    config: &ChunksConfig,
    collection: &str,
    gaps: Gaps,
) -> Result<Node, String> {
    let dims = ordered_dims(cube);

    // param -> variables (never an axis, never base_request).
    let params = cube
        .get("param")
        .filter(|p| !p.is_empty())
        .ok_or_else(|| format!("cube {} has no 'param' value", describe(cube, &dims)))?;

    // Grid registry match, applied to the cube's full canonical request.
    let canonical = CanonicalRequest {
        entries: dims.iter().map(|d| (d.clone(), cube[d].clone())).collect(),
    };
    let grid = find_grid(config, collection, &canonical).ok_or_else(|| {
        format!(
            "no grid is registered for cube {} in collection '{collection}' (chunks.grids)",
            describe(cube, &dims)
        )
    })?;

    let mut axes = Vec::new();
    let mut base_request: Vec<(String, String)> = Vec::new();
    let mut fill_on_missing = false;
    for dim in &dims {
        if dim == "param" {
            continue;
        }
        let values = &cube[dim];
        if values.len() <= 1 {
            // Single-valued key: pin it.
            if let Some(v) = values.first() {
                base_request.push((dim.clone(), v.clone()));
            }
            continue;
        }
        if dim == "values" {
            return Err("request key 'values' clashes with the grid dimension name".to_string());
        }
        // Multi-valued key: an axis.
        let axis_values = if gaps == Gaps::Span && dim == "date" {
            fill_on_missing = true;
            calendar_fill(values)?
        } else {
            values.clone()
        };
        axes.push(Axis {
            dim: dim.clone(),
            key: dim.clone(),
            values: axis_values,
        });
    }

    let variables = params
        .iter()
        .map(|param| Variable {
            // metkit's expansion does not expose shortnames through the bridge;
            // the canonical param id doubles as the variable name (as in v1).
            name: param.clone(),
            param: param.clone(),
        })
        .collect();

    let order: Vec<String> = axes.iter().map(|a| a.dim.clone()).collect();

    Ok(Node::ArraySet {
        name: String::new(),
        base_request: OrderedMap(base_request),
        axes,
        variables,
        grid: Grid {
            kind: "unstructured",
            count_values: grid.count_values,
            md5_grid_section: grid.md5_grid_section.clone(),
        },
        fill_on_missing,
        extract: ExtractInfo {
            grid_hash: grid.md5_grid_section.clone(),
            order,
        },
    })
}

/// The cube's dims in canonical metkit axis order.
fn ordered_dims(cube: &Cube) -> Vec<String> {
    let mut dims: Vec<String> = cube.keys().cloned().collect();
    dims.sort_by(|a, b| axis_rank(a).cmp(&axis_rank(b)).then_with(|| a.cmp(b)));
    dims
}

/// Number of distinct value-sets a key takes across the cubes.
fn distinct_value_sets(cubes: &[Cube], key: &str) -> usize {
    let mut seen: std::collections::BTreeSet<Vec<String>> = std::collections::BTreeSet::new();
    for cube in cubes {
        if let Some(vs) = cube.get(key) {
            seen.insert(vs.clone());
        }
    }
    seen.len()
}

/// Keys that are single-valued and identical across every cube (the shared
/// pinned selection at a group node), in canonical order, excluding `param`.
fn common_pinned(cubes: &[Cube]) -> Vec<(String, Vec<String>)> {
    let Some(first) = cubes.first() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for dim in ordered_dims(first) {
        if dim == "param" {
            continue;
        }
        let vs = &first[&dim];
        if vs.len() != 1 {
            continue;
        }
        if cubes.iter().all(|c| c.get(&dim) == Some(vs)) {
            out.push((dim.clone(), vs.clone()));
        }
    }
    out
}

/// A short human description of a cube for error messages.
fn describe(cube: &Cube, dims: &[String]) -> String {
    let parts: Vec<String> = dims
        .iter()
        .map(|d| {
            let vs = &cube[d];
            if vs.len() == 1 {
                format!("{d}={}", vs[0])
            } else {
                format!("{d}=[{} values]", vs.len())
            }
        })
        .collect();
    format!("{{{}}}", parts.join(", "))
}

/// A deterministic, sibling-unique name derived from the distinguishing key
/// and its values. Restricted to `[A-Za-z0-9_.-]`.
fn name_from_values(key: &str, values: &[String]) -> String {
    let body = match values.len() {
        0 => "none".to_string(),
        1 => sanitize(&values[0]),
        2..=3 => values
            .iter()
            .map(|v| sanitize(v))
            .collect::<Vec<_>>()
            .join("_"),
        n => format!(
            "{}_{}_n{n}",
            sanitize(&values[0]),
            sanitize(values.last().unwrap())
        ),
    };
    format!("{}-{body}", sanitize(key))
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Ensure names are unique among siblings by appending a deterministic numeric
/// suffix on collision.
fn uniquify(names: Vec<String>) -> Vec<String> {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let count = seen.entry(name.clone()).or_insert(0);
        if *count == 0 {
            out.push(name.clone());
        } else {
            out.push(format!("{name}-{}", *count + 1));
        }
        *count += 1;
    }
    out
}

/// Replace a `date` value set with the full daily calendar between its min and
/// max (inclusive). Dates are `YYYYMMDD` strings.
fn calendar_fill(values: &[String]) -> Result<Vec<String>, String> {
    let parse = |s: &str| {
        NaiveDate::parse_from_str(s, "%Y%m%d")
            .map_err(|_| format!("date value '{s}' is not a valid YYYYMMDD date"))
    };
    let mut min = parse(&values[0])?;
    let mut max = min;
    for v in values {
        let d = parse(v)?;
        if d < min {
            min = d;
        }
        if d > max {
            max = d;
        }
    }
    let mut out = Vec::new();
    let mut cur = min;
    while cur <= max {
        out.push(cur.format("%Y%m%d").to_string());
        cur = cur
            .succ_opt()
            .ok_or("date calendar overflowed while filling a span")?;
    }
    Ok(out)
}
