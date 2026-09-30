// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! `/chunks/v1/{collection}/extract` body validation and job-body construction
//! (contract §2).
//!
//! The job body forwarded to workers is the flat MARS request plus a
//! normalised top-level `extract` object:
//!
//! ```jsonc
//! { "<mars key>": "v" | ["v1", ...], ...,
//!   "extract": { "ranges": [[lo, hi], ...], "order": ["<key>", ...],
//!                "grid_hash": "hex" | null, "dtype": "float32" | "float64",
//!                "shuffle": true | false } }
//! ```
//!
//! `transform::metkit_expansion` sets aside every object-valued top-level key
//! (other than `verb`) before expanding and re-inserts it verbatim afterwards,
//! so `extract` reaches the worker untouched.

use std::collections::HashSet;

use serde_json::{json, Map, Value};

use super::metadata::{feature_unsupported, parse_flat_request};

pub const EXTRACT_KEY: &str = "extract";
pub const DEFAULT_DTYPE: &str = "float32";
pub const SUPPORTED_DTYPES: &[&str] = &["float32", "float64"];

const EXTRACT_FIELDS: &[&str] = &["ranges", "order", "grid_hash", "dtype", "shuffle"];

/// Number of values a request value denotes (MARS `a/to/b` ranges are
/// rejected: extract requests must enumerate canonical values).
fn value_count(key: &str, value: &Value) -> Result<u64, String> {
    let tokens: Vec<String> = match value {
        Value::Array(items) => items
            .iter()
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect(),
        Value::String(s) => s.split('/').map(str::to_string).collect(),
        _ => vec![String::new()],
    };
    if tokens
        .iter()
        .any(|t| t.eq_ignore_ascii_case("to") || t.eq_ignore_ascii_case("by"))
    {
        return Err(format!(
            "request key '{key}' uses a MARS range; extract requests must enumerate canonical \
             values"
        ));
    }
    Ok(tokens.len() as u64)
}

fn parse_ranges(value: Option<&Value>) -> Result<Vec<(u64, u64)>, String> {
    let items = value
        .and_then(Value::as_array)
        .ok_or("extract.ranges must be a non-empty array of [lo, hi] pairs")?;
    if items.is_empty() {
        return Err("extract.ranges must not be empty".to_string());
    }
    items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let pair = item
                .as_array()
                .filter(|p| p.len() == 2)
                .ok_or_else(|| format!("extract.ranges[{i}] must be a [lo, hi] pair"))?;
            let lo = pair[0].as_u64();
            let hi = pair[1].as_u64();
            match (lo, hi) {
                (Some(lo), Some(hi)) if lo < hi => Ok((lo, hi)),
                _ => Err(format!(
                    "extract.ranges[{i}] must be non-negative integers with lo < hi (half-open)"
                )),
            }
        })
        .collect()
}

fn parse_order(value: Option<&Value>) -> Result<Vec<String>, String> {
    let items = value
        .and_then(Value::as_array)
        .ok_or("extract.order must be an array of request keys")?;
    let mut seen = HashSet::new();
    items
        .iter()
        .map(|item| {
            let key = item
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or("extract.order entries must be non-empty strings")?;
            if !seen.insert(key) {
                return Err(format!("extract.order contains '{key}' more than once"));
            }
            Ok(key.to_string())
        })
        .collect()
}

/// Validate an extract body and build the flat worker job body.
pub fn build_extract_job(body: &Value, max_chunk_cost: u64) -> Result<Value, String> {
    build_single_extract_job(body, max_chunk_cost).map(|(job, _cost)| job)
}

fn build_single_extract_job(body: &Value, max_chunk_cost: u64) -> Result<(Value, u64), String> {
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
    let mut request = parse_flat_request(request)?;
    if request.contains_key(EXTRACT_KEY) {
        return Err("'extract' must not appear inside 'request'".to_string());
    }

    let extract = obj
        .get(EXTRACT_KEY)
        .ok_or("request body must contain an 'extract' object")?
        .as_object()
        .ok_or("'extract' must be a JSON object")?;
    if let Some(unknown) = extract
        .keys()
        .find(|k| !EXTRACT_FIELDS.contains(&k.as_str()))
    {
        return Err(format!("unknown extract field '{unknown}'"));
    }

    let ranges = parse_ranges(extract.get("ranges"))?;
    let order = parse_order(extract.get("order"))?;
    let dtype = match extract.get("dtype") {
        None => DEFAULT_DTYPE,
        Some(Value::String(dtype)) if SUPPORTED_DTYPES.contains(&dtype.as_str()) => dtype,
        _ => {
            return Err(format!(
                "extract.dtype must be one of {}",
                SUPPORTED_DTYPES
                    .iter()
                    .map(|dtype| format!("\"{dtype}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    };
    let shuffle = match extract.get("shuffle") {
        None => true,
        Some(Value::Bool(shuffle)) => *shuffle,
        Some(_) => return Err("extract.shuffle must be a boolean".to_string()),
    };
    let grid_hash = match extract.get("grid_hash") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::String(s)) if !s.is_empty() => Value::String(s.clone()),
        Some(_) => return Err("extract.grid_hash must be a string or null".to_string()),
    };

    // Per-key value counts (ranges `a/to/b` rejected).
    let mut counts = Map::new();
    for (key, value) in &request {
        counts.insert(key.clone(), json!(value_count(key, value)?));
    }
    let count = |key: &str| counts.get(key).and_then(Value::as_u64).unwrap_or(0);

    match request.get("param") {
        Some(_) if count("param") == 1 => {}
        _ => return Err("request must contain exactly one 'param' value".to_string()),
    }
    if order.iter().any(|k| k == "param") {
        return Err("'param' must not appear in extract.order".to_string());
    }
    for key in &order {
        if !request.contains_key(key) {
            return Err(format!("extract.order key '{key}' is not in the request"));
        }
    }
    for key in request.keys() {
        if count(key) > 1 && !order.contains(key) {
            return Err(format!(
                "request key '{key}' has several values but is not in extract.order"
            ));
        }
    }

    let fields = order
        .iter()
        .try_fold(1u64, |acc, key| acc.checked_mul(count(key)));
    let points = ranges
        .iter()
        .try_fold(0u64, |acc, (lo, hi)| acc.checked_add(hi - lo));
    let cost = fields.zip(points).and_then(|(f, p)| f.checked_mul(p));
    let cost = match cost {
        Some(cost) if cost <= max_chunk_cost => cost,
        _ => {
            return Err(format!(
                "extract cost (fields x points = {}) exceeds max_chunk_cost {max_chunk_cost}",
                cost.map_or_else(|| "overflow".to_string(), |c| c.to_string())
            ));
        }
    };

    request.insert(
        EXTRACT_KEY.to_string(),
        json!({
            "ranges": ranges.iter().map(|(lo, hi)| json!([lo, hi])).collect::<Vec<_>>(),
            "order": order,
            "grid_hash": grid_hash,
            "dtype": dtype,
            "shuffle": shuffle,
        }),
    );
    Ok((Value::Object(request), cost))
}

/// Accept either the legacy single body or Contract v2.5's multi-chunk envelope.
pub fn build_extract_request(
    body: &Value,
    max_chunk_cost: u64,
    max_multi_chunks: usize,
) -> Result<Value, String> {
    let Some(obj) = body.as_object() else {
        return Err("request body must be a JSON object".to_string());
    };
    let Some(chunks) = obj.get("chunks") else {
        return build_extract_job(body, max_chunk_cost);
    };
    if obj.len() != 1 {
        return Err("a multi-chunk body must contain only 'chunks'".to_string());
    }
    let chunks = chunks.as_array().ok_or("'chunks' must be a JSON array")?;
    if chunks.is_empty() || chunks.len() > max_multi_chunks {
        return Err(format!(
            "'chunks' must contain between 1 and {max_multi_chunks} elements"
        ));
    }

    let mut normalized = Vec::with_capacity(chunks.len());
    let mut aggregate_cost = 0u64;
    for (index, chunk) in chunks.iter().enumerate() {
        let (job, cost) = build_single_extract_job(chunk, max_chunk_cost)
            .map_err(|error| format!("chunks[{index}]: {error}"))?;
        aggregate_cost = aggregate_cost
            .checked_add(cost)
            .ok_or("aggregate extract cost overflow")?;
        normalized.push(job);
    }
    if aggregate_cost > max_chunk_cost {
        return Err(format!(
            "aggregate extract cost {aggregate_cost} exceeds max_chunk_cost {max_chunk_cost}"
        ));
    }
    Ok(json!({"chunks": normalized}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body() -> Value {
        json!({
            "request": {
                "class": "d1",
                "date": ["20200101", "20200102"],
                "time": "0000/1200",
                "param": "167",
                "levtype": "sfc",
            },
            "extract": {
                "ranges": [[0, 10], [20, 25]],
                "order": ["date", "time"],
                "grid_hash": "abc123",
                "dtype": "float64",
            }
        })
    }

    #[test]
    fn builds_flat_job_body_with_normalised_extract() {
        let job = build_extract_job(&body(), 1_000).unwrap();
        assert_eq!(
            job,
            json!({
                "class": "d1",
                "date": ["20200101", "20200102"],
                "time": "0000/1200",
                "param": "167",
                "levtype": "sfc",
                "extract": {
                    "ranges": [[0, 10], [20, 25]],
                    "order": ["date", "time"],
                    "grid_hash": "abc123",
                    "dtype": "float64",
                    "shuffle": true,
                }
            })
        );
    }

    #[test]
    fn defaults_dtype_shuffle_and_allows_legacy_options() {
        let mut b = json!({
            "request": {"class": "d1", "param": ["167"]},
            "extract": {"ranges": [[0, 5]], "order": []}
        });
        let job = build_extract_job(&b, 100).unwrap();
        assert_eq!(job["extract"]["grid_hash"], Value::Null);
        assert_eq!(job["extract"]["dtype"], "float32");
        assert_eq!(job["extract"]["shuffle"], true);

        b["extract"]["grid_hash"] = Value::Null;
        b["extract"]["dtype"] = json!("float64");
        b["extract"]["shuffle"] = json!(false);
        let job = build_extract_job(&b, 100).unwrap();
        assert_eq!(job["extract"]["dtype"], "float64");
        assert_eq!(job["extract"]["shuffle"], false);
    }

    #[test]
    fn cost_is_fields_times_points() {
        // 2 dates x 2 times x 15 points = 60
        assert!(build_extract_job(&body(), 60).is_ok());
        let err = build_extract_job(&body(), 59).unwrap_err();
        assert!(err.contains("max_chunk_cost"), "{err}");
    }

    #[test]
    fn rejects_invalid_bodies() {
        type Mutation = Box<dyn Fn(&mut Value)>;
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "feature",
                Box::new(|b| b["feature"] = json!({"type": "polygon"})),
            ),
            (
                "feature in request",
                Box::new(|b| b["request"]["feature"] = json!({})),
            ),
            (
                "no request",
                Box::new(|b| {
                    b.as_object_mut().unwrap().remove("request");
                }),
            ),
            (
                "no extract",
                Box::new(|b| {
                    b.as_object_mut().unwrap().remove("extract");
                }),
            ),
            ("extract not object", Box::new(|b| b["extract"] = json!([]))),
            (
                "empty ranges",
                Box::new(|b| b["extract"]["ranges"] = json!([])),
            ),
            (
                "ranges missing",
                Box::new(|b| {
                    b["extract"].as_object_mut().unwrap().remove("ranges");
                }),
            ),
            (
                "lo == hi",
                Box::new(|b| b["extract"]["ranges"] = json!([[5, 5]])),
            ),
            (
                "lo > hi",
                Box::new(|b| b["extract"]["ranges"] = json!([[6, 5]])),
            ),
            (
                "negative",
                Box::new(|b| b["extract"]["ranges"] = json!([[-1, 5]])),
            ),
            (
                "float",
                Box::new(|b| b["extract"]["ranges"] = json!([[0.5, 5]])),
            ),
            (
                "triple",
                Box::new(|b| b["extract"]["ranges"] = json!([[0, 5, 6]])),
            ),
            (
                "order not array",
                Box::new(|b| b["extract"]["order"] = json!("date")),
            ),
            (
                "order non-string",
                Box::new(|b| b["extract"]["order"] = json!([1])),
            ),
            (
                "order dup",
                Box::new(|b| b["extract"]["order"] = json!(["date", "date", "time"])),
            ),
            (
                "order unknown key",
                Box::new(|b| b["extract"]["order"] = json!(["date", "time", "step"])),
            ),
            (
                "order missing multi key",
                Box::new(|b| b["extract"]["order"] = json!(["date"])),
            ),
            (
                "order has param",
                Box::new(|b| b["extract"]["order"] = json!(["date", "time", "param"])),
            ),
            (
                "unsupported dtype",
                Box::new(|b| b["extract"]["dtype"] = json!("float16")),
            ),
            (
                "dtype not string",
                Box::new(|b| b["extract"]["dtype"] = json!(32)),
            ),
            (
                "shuffle not boolean",
                Box::new(|b| b["extract"]["shuffle"] = json!("true")),
            ),
            (
                "grid_hash number",
                Box::new(|b| b["extract"]["grid_hash"] = json!(1)),
            ),
            (
                "unknown extract field",
                Box::new(|b| b["extract"]["bogus"] = json!(1)),
            ),
            (
                "two params",
                Box::new(|b| b["request"]["param"] = json!(["167", "165"])),
            ),
            (
                "two params slash",
                Box::new(|b| b["request"]["param"] = json!("167/165")),
            ),
            (
                "no param",
                Box::new(|b| {
                    b["request"].as_object_mut().unwrap().remove("param");
                }),
            ),
            (
                "mars range",
                Box::new(|b| b["request"]["date"] = json!("20200101/to/20200102")),
            ),
            (
                "extract in request",
                Box::new(|b| b["request"]["extract"] = json!("x")),
            ),
        ];
        for (name, mutate) in cases {
            let mut b = body();
            mutate(&mut b);
            assert!(
                build_extract_job(&b, u64::MAX).is_err(),
                "case '{name}' should be rejected"
            );
        }
    }

    #[test]
    fn builds_multi_job_and_enforces_limits() {
        let first = body();
        let mut second = body();
        second["request"]["param"] = json!("168");
        let job =
            build_extract_request(&json!({"chunks": [first.clone(), second]}), 1_000, 2).unwrap();
        assert_eq!(job["chunks"].as_array().unwrap().len(), 2);
        assert_eq!(job["chunks"][0]["extract"]["dtype"], "float64");

        assert!(build_extract_request(&json!({"chunks": []}), 1_000, 2).is_err());
        assert!(build_extract_request(
            &json!({"chunks": [first.clone(), first.clone(), first]}),
            1_000,
            2
        )
        .is_err());
    }

    #[test]
    fn multi_aggregate_cost_is_bounded() {
        let first = body(); // four fields x 15 points = 60
        let mut second = body();
        second["request"]["param"] = json!("168");
        assert!(build_extract_request(&json!({"chunks": [first, second]}), 100, 64).is_err());
    }
}
