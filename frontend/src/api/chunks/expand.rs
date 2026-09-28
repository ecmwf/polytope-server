// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! MARS request canonicalisation for `/chunks/v1/{collection}/metadata`.
//!
//! The real expansion is metkit (`crate::metkit_expansion::MetkitRequestExpander`,
//! behind the `metkit` feature — the same `metkit::expand_json` FFI call that
//! `transform::metkit_expansion` uses). It is isolated behind
//! [`RequestExpander`] so the endpoint logic is testable without the native
//! eckit/metkit libraries.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{Map, Value};

/// metkit's hypercube axis order (`share/metkit/axis.yaml`, metkit 1.17).
///
/// `MarsLanguage::expand` sorts the user-supplied keys by this list before
/// expanding, so this is the canonical MARS key order metkit itself uses. We
/// apply it here explicitly because the Rust `metkit` binding returns its
/// result through a `HashMap`, which loses the order emitted by the C++ side.
/// Keys not in the list sort after all listed keys, alphabetically.
pub const AXIS_ORDER: &[&str] = &[
    "class",
    "country",
    "type",
    "stream",
    "levtype",
    "origin",
    "product",
    "section",
    "method",
    "system",
    "date",
    "refdate",
    "hdate",
    "offsetdate",
    "time",
    "offsettime",
    "anoffset",
    "reference",
    "dataset",
    "step",
    "fcmonth",
    "fcperiod",
    "leadtime",
    "opttime",
    "expver",
    "domain",
    "diagnostic",
    "iteration",
    "quantile",
    "number",
    "levelist",
    "latitude",
    "longitude",
    "range",
    "param",
    "chem",
    "wavelength",
    "timespan",
    "stattype",
    "ident",
    "obstype",
    "instrument",
    "frequency",
    "direction",
    "channel",
    "obsgroup",
    "reportype",
    "activity",
    "experiment",
    "generation",
    "model",
    "realization",
    "resolution",
    "year",
    "month",
    "bcmodel",
    "icmodel",
    "grib",
    "georef",
    "coeffindex",
    "forcing",
    "configuration",
    "obscutoff",
];

/// Failure modes of a [`RequestExpander`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandError {
    /// The request is invalid (metkit rejected it) — a client error (400).
    Invalid(String),
    /// Expansion is not available in this build/deployment (501).
    Unavailable(String),
}

/// Expands a flat MARS request to metkit-canonical values.
pub trait RequestExpander: Send + Sync {
    /// `request` is a flat MARS request: no `verb`, no object-valued keys;
    /// values are strings (possibly `/`-separated), numbers, or arrays of
    /// those. Returns every key of the expanded request (including metkit
    /// defaults) with its canonical string values. Output key order is not
    /// significant; see [`canonicalise`].
    fn expand(
        &self,
        request: &Map<String, Value>,
    ) -> Result<Vec<(String, Vec<String>)>, ExpandError>;
}

/// Expander used when the frontend is built without the `metkit` feature.
pub struct UnavailableExpander;

impl RequestExpander for UnavailableExpander {
    fn expand(
        &self,
        _request: &Map<String, Value>,
    ) -> Result<Vec<(String, Vec<String>)>, ExpandError> {
        Err(ExpandError::Unavailable(
            "MARS request expansion is not available: polytope-server was built without the \
             'metkit' feature"
                .to_string(),
        ))
    }
}

/// The expander wired into [`crate::build_app`].
pub fn default_expander() -> Arc<dyn RequestExpander> {
    #[cfg(feature = "metkit")]
    {
        Arc::new(crate::metkit_expansion::MetkitRequestExpander)
    }
    #[cfg(not(feature = "metkit"))]
    {
        Arc::new(UnavailableExpander)
    }
}

/// A metkit-canonical request with keys in canonical ([`AXIS_ORDER`]) order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRequest {
    pub entries: Vec<(String, Vec<String>)>,
}

impl CanonicalRequest {
    pub fn get(&self, key: &str) -> Option<&[String]> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_slice())
    }
}

fn axis_rank(key: &str) -> usize {
    AXIS_ORDER
        .iter()
        .position(|k| *k == key)
        .unwrap_or(AXIS_ORDER.len())
}

/// Validate expander output and put it in canonical key order.
///
/// Rejects (as client errors) keys that expanded to no values, to duplicate
/// values, or to the unexpanded `all` wildcard — none of which can form a
/// well-defined axis.
pub fn canonicalise(entries: Vec<(String, Vec<String>)>) -> Result<CanonicalRequest, String> {
    let mut seen_keys = HashSet::new();
    let mut out = Vec::with_capacity(entries.len());
    for (key, values) in entries {
        if key == "verb" {
            continue;
        }
        if !seen_keys.insert(key.clone()) {
            return Err(format!(
                "key '{key}' appears more than once after expansion"
            ));
        }
        if values.is_empty() {
            return Err(format!("key '{key}' expanded to no values"));
        }
        let mut seen_values = HashSet::new();
        for value in &values {
            if value.eq_ignore_ascii_case("all") {
                return Err(format!(
                    "key '{key}' expanded to 'all'; enumerate the values explicitly"
                ));
            }
            if !seen_values.insert(value.as_str()) {
                return Err(format!(
                    "key '{key}' expanded to duplicate value '{value}' (values must be distinct \
                     after canonicalisation)"
                ));
            }
        }
        out.push((key, values));
    }
    out.sort_by(|(a, _), (b, _)| axis_rank(a).cmp(&axis_rank(b)).then_with(|| a.cmp(b)));
    Ok(CanonicalRequest { entries: out })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(key: &str, values: &[&str]) -> (String, Vec<String>) {
        (
            key.to_string(),
            values.iter().map(|v| v.to_string()).collect(),
        )
    }

    #[test]
    fn canonicalise_sorts_by_metkit_axis_order_then_alphabetically() {
        let c = canonicalise(vec![
            e("zzz", &["1"]),
            e("param", &["167"]),
            e("aaa", &["1"]),
            e("time", &["0000"]),
            e("class", &["od"]),
            e("date", &["20240101"]),
            e("verb", &["retrieve"]),
        ])
        .unwrap();
        let keys: Vec<&str> = c.entries.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["class", "date", "time", "param", "aaa", "zzz"]);
    }

    #[test]
    fn canonicalise_rejects_duplicates_empty_and_all() {
        assert!(canonicalise(vec![e("step", &["0", "0"])]).is_err());
        assert!(canonicalise(vec![e("step", &[])]).is_err());
        assert!(canonicalise(vec![e("levelist", &["all"])]).is_err());
        assert!(canonicalise(vec![e("step", &["0"]), e("step", &["6"])]).is_err());
    }

    #[test]
    fn unavailable_expander_reports_unavailable() {
        assert!(matches!(
            UnavailableExpander.expand(&Map::new()),
            Err(ExpandError::Unavailable(_))
        ));
    }
}
