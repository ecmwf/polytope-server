// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

const REDACTED: &str = "[REDACTED]";
const SECRET_PROBES: &[&str] = &[
    "FAKETOKEN_OBSERVABILITY_PROBE",
    "32eff194-66bd",
    "lAYFsKT9xYeraMbeH2Sn4RPL7iJgNaxY",
    "vv7pGSEZcFFB87",
    "BaThQ7cKxG5NuJ",
    "WQrRuQn4fvssgGYCiZTt",
    "POLY-4a7bb966e4a51b9c25b429bc96cf25dd",
    "3..izBAd75",
];

fn bearer_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)Bearer\s+[^\s,;\"'{}]+"#).unwrap())
}
fn assignment_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)\b(password|token|api_key)=([^\s,;\"'&]+)"#).unwrap())
}
fn jwt_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b").unwrap())
}
fn url_userinfo_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@").unwrap())
}

pub fn redact_string(input: &str) -> String {
    let mut out = input.to_string();
    out = bearer_re().replace_all(&out, REDACTED).into_owned();
    out = assignment_re()
        .replace_all(&out, "$1=[REDACTED]")
        .into_owned();
    out = jwt_re().replace_all(&out, REDACTED).into_owned();
    out = url_userinfo_re()
        .replace_all(&out, format!("$1{}@", REDACTED))
        .into_owned();
    for probe in SECRET_PROBES {
        out = out.replace(probe, REDACTED);
    }
    out
}

pub fn redact_field(name: &str, value: &str) -> String {
    if name.eq_ignore_ascii_case("authorization")
        || name.to_ascii_lowercase().ends_with(".authorization")
    {
        REDACTED.to_string()
    } else {
        redact_string(value)
    }
}

pub fn redact_json(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(redact_string(&s)),
        Value::Array(values) => Value::Array(values.into_iter().map(redact_json).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    let redacted = if matches!(v, Value::String(_))
                        && k.eq_ignore_ascii_case("authorization")
                    {
                        Value::String(REDACTED.to_string())
                    } else {
                        redact_json(v)
                    };
                    (k, redacted)
                })
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_all_categories() {
        let input = "Authorization: Bearer FAKETOKEN_OBSERVABILITY_PROBE password=hunter token=abc api_key=key eyJabc.def.ghi https://user:pass@example.com/path 32eff194-66bd";
        let out = redact_string(input);
        for forbidden in [
            "FAKETOKEN_OBSERVABILITY_PROBE",
            "hunter",
            "token=abc",
            "api_key=key",
            "eyJabc.def.ghi",
            "user:pass",
            "32eff194-66bd",
        ] {
            assert!(!out.contains(forbidden), "leaked {forbidden} in {out}");
        }
        assert!(out.contains(REDACTED));
    }

    #[test]
    fn authorization_field_is_fully_redacted() {
        assert_eq!(redact_field("Authorization", "Basic abc"), REDACTED);
    }

    #[test]
    fn nested_json_is_redacted() {
        let value = serde_json::json!({"a":{"token":"Bearer FAKETOKEN_OBSERVABILITY_PROBE"}, "authorization":"abc"});
        let raw = redact_json(value).to_string();
        assert!(!raw.contains("FAKETOKEN_OBSERVABILITY_PROBE"));
        assert!(!raw.contains("abc"));
    }
}
