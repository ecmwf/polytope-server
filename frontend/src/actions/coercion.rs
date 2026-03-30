use std::collections::BTreeSet;

use bits::actions::ActionError;
use chrono::{Datelike, Duration, Local, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoercionConfig {
    #[serde(default = "default_allow_ranges")]
    pub allow_ranges: Vec<String>,
    #[serde(default = "default_allow_lists")]
    pub allow_lists: Vec<String>,
    #[serde(default)]
    pub number_allow_zero: bool,
}

impl Default for CoercionConfig {
    fn default() -> Self {
        Self {
            allow_ranges: default_allow_ranges(),
            allow_lists: default_allow_lists(),
            number_allow_zero: false,
        }
    }
}

fn default_allow_ranges() -> Vec<String> {
    ["number", "step", "date", "time"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_allow_lists() -> Vec<String> {
    [
        "class", "stream", "type", "expver", "param", "number", "date", "step", "time",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn coerce_request(value: &Value, config: &CoercionConfig) -> Result<Value, ActionError> {
    let mut request = match value {
        Value::Object(map) => map.clone(),
        other => return Ok(json!({ "data": other.clone() })),
    };

    let mut errors = Vec::new();
    for (key, current) in request.clone() {
        match coerce_value(&key, current, config) {
            Ok(coerced) => {
                if let Value::Array(items) = &coerced {
                    let mut unique = BTreeSet::new();
                    let mut duplicates = BTreeSet::new();
                    for item in items {
                        let rendered = item.to_string();
                        if !unique.insert(rendered.clone()) {
                            duplicates.insert(rendered);
                        }
                    }
                    if !duplicates.is_empty() {
                        errors.push(format!(
                            "Duplicate values found in list for key '{key}': {:?}",
                            duplicates
                        ));
                    }
                }
                request.insert(key, coerced);
            }
            Err(err) => errors.push(format!("{err} for key '{key}'")),
        }
    }

    if errors.is_empty() {
        Ok(Value::Object(request))
    } else {
        Err(ActionError::ConfigError(format!(
            "Errors in request:\n {}",
            errors.join("\n ")
        )))
    }
}

pub fn coerce_value(key: &str, value: Value, config: &CoercionConfig) -> Result<Value, String> {
    let allow_ranges: BTreeSet<&str> = config.allow_ranges.iter().map(String::as_str).collect();
    let allow_lists: BTreeSet<&str> = config.allow_lists.iter().map(String::as_str).collect();
    if let Some(func) = coercer_for(key, config) {
        return match value {
            Value::Array(values) => values
                .into_iter()
                .map(func)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            Value::String(text) if text.contains("/to/") && allow_ranges.contains(key) => {
                let (start_raw, rest) = text
                    .split_once("/to/")
                    .ok_or_else(|| format!("Invalid range format for key {key}."))?;
                let (end_raw, suffix) = match rest.split_once("/by/") {
                    Some((end, by)) => (end, format!("/by/{by}")),
                    None => (rest, String::new()),
                };
                let start = scalar_to_string(func(Value::String(start_raw.to_string()))?)?;
                let end = scalar_to_string(func(Value::String(end_raw.to_string()))?)?;
                Ok(Value::String(format!("{start}/to/{end}{suffix}")))
            }
            Value::String(text) if text.contains('/') && allow_lists.contains(key) => text
                .split('/')
                .map(|item| func(Value::String(item.to_string())))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            other => func(other),
        };
    }

    match value {
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(|value| Value::String(render_scalar(&value)))
                .collect(),
        )),
        other => Ok(other),
    }
}

type Coercer<'a> = Box<dyn Fn(Value) -> Result<Value, String> + 'a>;

fn coercer_for<'a>(key: &'a str, config: &'a CoercionConfig) -> Option<Coercer<'a>> {
    match key {
        "date" => Some(Box::new(coerce_date)),
        "step" => Some(Box::new(coerce_step)),
        "number" => Some(Box::new(move |value| {
            coerce_number(value, config.number_allow_zero)
        })),
        "param" => Some(Box::new(coerce_passthrough)),
        "time" => Some(Box::new(coerce_time)),
        "expver" => Some(Box::new(coerce_expver)),
        "model" | "experiment" | "activity" => Some(Box::new(coerce_lowercase)),
        _ => None,
    }
}

fn scalar_to_string(value: Value) -> Result<String, String> {
    match value {
        Value::String(text) => Ok(text),
        other => Err(format!("expected scalar string, got {other}")),
    }
}

fn render_scalar(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

fn coerce_date(value: Value) -> Result<Value, String> {
    match value {
        Value::Number(number) => {
            let value = number.as_i64().ok_or_else(|| {
                "Invalid date format, expected YYYYMMDD or YYYY-MM-DD.".to_string()
            })?;
            if value > 0 {
                let date =
                    NaiveDate::parse_from_str(&value.to_string(), "%Y%m%d").map_err(|_| {
                        "Invalid date format, expected YYYYMMDD or YYYY-MM-DD.".to_string()
                    })?;
                Ok(Value::String(date.format("%Y%m%d").to_string()))
            } else {
                let date = Local::now().date_naive() + Duration::days(value);
                Ok(Value::String(date.format("%Y%m%d").to_string()))
            }
        }
        Value::String(text) => {
            let text = text.trim();
            if let Ok(number) = text.parse::<i64>() {
                return coerce_date(Value::Number(number.into()));
            }
            if let Ok(date) = NaiveDate::parse_from_str(text, "%Y%m%d") {
                return Ok(Value::String(date.format("%Y%m%d").to_string()));
            }
            if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
                return Ok(Value::String(date.format("%Y%m%d").to_string()));
            }
            Err("Invalid date format, expected YYYYMMDD or YYYY-MM-DD.".into())
        }
        _ => Err("Invalid date format, expected YYYYMMDD or YYYY-MM-DD.".into()),
    }
}

fn coerce_step(value: Value) -> Result<Value, String> {
    match value {
        Value::Number(number) => {
            let value = number
                .as_i64()
                .ok_or_else(|| "Invalid type, expected integer or string.".to_string())?;
            if value < 0 {
                Err("Step must be greater than or equal to 0.".into())
            } else {
                Ok(Value::String(value.to_string()))
            }
        }
        Value::String(text) => {
            if is_valid_step(&text) {
                return Ok(Value::String(text));
            }
            if let Some((start, end)) = text.split_once('-')
                && is_valid_step(start)
                && is_valid_step(end)
            {
                return Ok(Value::String(text));
            }
            Err("Invalid step format, expected integer or duration-like step.".into())
        }
        _ => Err("Invalid type, expected integer or string.".into()),
    }
}

fn is_valid_step(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == 0 {
        return false;
    }
    let mut rest = &value[idx..];
    for unit in ['d', 'h', 'm', 's'] {
        if rest.is_empty() {
            break;
        }
        let digits = rest.chars().take_while(char::is_ascii_digit).count();
        if digits == 0 {
            continue;
        }
        let (count, remaining) = rest.split_at(digits);
        if remaining.starts_with(unit) {
            let _ = count;
            rest = &remaining[1..];
        }
    }
    rest.is_empty()
}

fn coerce_number(value: Value, allow_zero: bool) -> Result<Value, String> {
    let min_value = if allow_zero { 0 } else { 1 };
    match value {
        Value::Number(number) => {
            let value = number
                .as_i64()
                .ok_or_else(|| "Invalid type, expected integer or string.".to_string())?;
            if value < min_value {
                Err(format!("Number must be >= {min_value}."))
            } else {
                Ok(Value::String(value.to_string()))
            }
        }
        Value::String(text) => {
            let value = text
                .parse::<i64>()
                .map_err(|_| format!("Number must be >= {min_value}."))?;
            if value < min_value {
                Err(format!("Number must be >= {min_value}."))
            } else {
                Ok(Value::String(value.to_string()))
            }
        }
        _ => Err("Invalid type, expected integer or string.".into()),
    }
}

fn coerce_passthrough(value: Value) -> Result<Value, String> {
    match value {
        Value::Number(number) => Ok(Value::String(number.to_string())),
        Value::String(text) => Ok(Value::String(text)),
        _ => Err("Invalid param type, expected integer or string.".into()),
    }
}

fn coerce_time(value: Value) -> Result<Value, String> {
    let (hour, minute) = match value {
        Value::Number(number) => {
            let value = number.as_i64().ok_or_else(|| {
                "Invalid time format, expected HHMM or HH greater than zero.".to_string()
            })?;
            if value < 0 {
                return Err("Invalid time format, expected HHMM or HH greater than zero.".into());
            }
            if value < 24 {
                (value as u32, 0)
            } else if (100..=2359).contains(&value) {
                ((value / 100) as u32, (value % 100) as u32)
            } else {
                return Err("Invalid time format, expected HHMM or HH.".into());
            }
        }
        Value::String(text) => {
            let text = text.trim();
            if let Some((hour, minute)) = text.split_once(':') {
                (
                    hour.parse::<u32>()
                        .map_err(|_| "Invalid time format, expected HHMM or HH.".to_string())?,
                    minute
                        .parse::<u32>()
                        .map_err(|_| "Invalid time format, expected HHMM or HH.".to_string())?,
                )
            } else if text.len() == 4 && text.chars().all(|c| c.is_ascii_digit()) {
                (
                    text[..2]
                        .parse::<u32>()
                        .map_err(|_| "Invalid time format, expected HHMM or HH.".to_string())?,
                    text[2..]
                        .parse::<u32>()
                        .map_err(|_| "Invalid time format, expected HHMM or HH.".to_string())?,
                )
            } else if text.len() <= 2 && text.chars().all(|c| c.is_ascii_digit()) {
                (
                    text.parse::<u32>()
                        .map_err(|_| "Invalid time format, expected HHMM or HH.".to_string())?,
                    0,
                )
            } else {
                return Err("Invalid time format, expected HHMM or HH.".into());
            }
        }
        _ => return Err("Invalid type for time, expected string or integer.".into()),
    };

    if hour > 23 || minute > 59 || minute != 0 {
        return Err("Invalid time format, expected HHMM or HH.".into());
    }
    Ok(Value::String(format!("{hour:02}{minute:02}")))
}

fn coerce_expver(value: Value) -> Result<Value, String> {
    match value {
        Value::Number(number) => {
            let value = number.as_u64().ok_or_else(|| {
                "expver integer must be between 0 and 9999 inclusive.".to_string()
            })?;
            if value <= 9999 {
                Ok(Value::String(format!("{value:04}")))
            } else {
                Err("expver integer must be between 0 and 9999 inclusive.".into())
            }
        }
        Value::String(text) => {
            if text.chars().all(|c| c.is_ascii_digit()) {
                let trimmed = text.trim_start_matches('0');
                let value = if trimmed.is_empty() {
                    0
                } else {
                    trimmed.parse::<u64>().unwrap_or(10_000)
                };
                if value <= 9999 {
                    Ok(Value::String(format!("{value:04}")))
                } else {
                    Err("expver integer string must represent a number between 0 and 9999 inclusive.".into())
                }
            } else if text.len() == 4 {
                Ok(Value::String(text))
            } else {
                Err("expver string length must be 4 characters exactly.".into())
            }
        }
        _ => Err("expver must be an integer or a string.".into()),
    }
}

fn coerce_lowercase(value: Value) -> Result<Value, String> {
    match value {
        Value::String(text) => Ok(Value::String(text.to_lowercase())),
        _ => Err("Invalid type, expected string.".into()),
    }
}

pub fn as_object(value: &Value) -> Result<&Map<String, Value>, ActionError> {
    value
        .as_object()
        .ok_or_else(|| ActionError::ConfigError("request must be a JSON object".into()))
}

pub fn request_field_as_strings(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items.iter().map(render_scalar).collect(),
        Value::String(text) if text.contains('/') => text.split('/').map(str::to_string).collect(),
        other => vec![render_scalar(other)],
    }
}

pub fn max_request_date(value: &Value) -> Result<NaiveDate, ActionError> {
    let dates = request_field_as_strings(value)
        .into_iter()
        .map(|item| {
            if let Some((_, tail)) = item.rsplit_once("/to/") {
                let end = tail.split("/by/").next().unwrap_or(tail);
                NaiveDate::parse_from_str(end, "%Y%m%d")
            } else {
                NaiveDate::parse_from_str(&item, "%Y%m%d")
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ActionError::ConfigError(format!("invalid date value: {err}")))?;
    dates
        .into_iter()
        .max()
        .ok_or_else(|| ActionError::ConfigError("date field is empty".into()))
}

pub fn max_request_u32(value: &Value) -> Result<u32, ActionError> {
    let mut values = Vec::new();
    for item in request_field_as_strings(value) {
        let candidate = if let Some((_, tail)) = item.rsplit_once("/to/") {
            tail.split("/by/").next().unwrap_or(tail).to_string()
        } else {
            item
        };
        values.push(candidate.parse::<u32>().map_err(|err| {
            ActionError::ConfigError(format!("invalid numeric request value: {err}"))
        })?);
    }
    values
        .into_iter()
        .max()
        .ok_or_else(|| ActionError::ConfigError("numeric request field is empty".into()))
}

pub fn parse_request_time(value: &Value) -> Result<String, ActionError> {
    let mut rendered = request_field_as_strings(value)
        .into_iter()
        .map(|text| {
            if text.len() == 4 {
                Ok(format!("{}:{}", &text[..2], &text[2..]))
            } else {
                Err(ActionError::ConfigError(format!(
                    "invalid time value: {text}"
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    rendered.sort();
    rendered
        .pop()
        .ok_or_else(|| ActionError::ConfigError("time field is empty".into()))
}

pub fn parse_request_class_like(value: &Value) -> Vec<String> {
    request_field_as_strings(value)
        .into_iter()
        .map(|item| item.to_lowercase())
        .collect()
}

pub fn date_to_ymd(date: NaiveDate) -> String {
    format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn default_config() -> CoercionConfig {
        CoercionConfig::default()
    }

    #[test]
    fn date_iso_string() {
        let result = coerce_date(json!("2024-01-15")).unwrap();
        assert_eq!(result, json!("20240115"));
    }

    #[test]
    fn date_compact_string() {
        let result = coerce_date(json!("20240115")).unwrap();
        assert_eq!(result, json!("20240115"));
    }

    #[test]
    fn date_integer() {
        let result = coerce_date(json!(20240115)).unwrap();
        assert_eq!(result, json!("20240115"));
    }

    #[test]
    fn date_negative_offset() {
        let result = coerce_date(json!(-1)).unwrap();
        let yesterday = (Local::now().date_naive() + Duration::days(-1))
            .format("%Y%m%d")
            .to_string();
        assert_eq!(result, json!(yesterday));
    }

    #[test]
    fn date_zero_offset() {
        let result = coerce_date(json!(0)).unwrap();
        let today = Local::now().date_naive().format("%Y%m%d").to_string();
        assert_eq!(result, json!(today));
    }

    #[test]
    fn date_invalid_string() {
        assert!(coerce_date(json!("not-a-date")).is_err());
    }

    #[test]
    fn date_invalid_type() {
        assert!(coerce_date(json!(true)).is_err());
    }

    #[test]
    fn time_integer_hour() {
        assert_eq!(coerce_time(json!(12)).unwrap(), json!("1200"));
    }

    #[test]
    fn time_integer_zero() {
        assert_eq!(coerce_time(json!(0)).unwrap(), json!("0000"));
    }

    #[test]
    fn time_four_digit_integer() {
        assert_eq!(coerce_time(json!(1200)).unwrap(), json!("1200"));
    }

    #[test]
    fn time_colon_string() {
        assert_eq!(coerce_time(json!("12:00")).unwrap(), json!("1200"));
    }

    #[test]
    fn time_four_digit_string() {
        assert_eq!(coerce_time(json!("1200")).unwrap(), json!("1200"));
    }

    #[test]
    fn time_two_digit_string() {
        assert_eq!(coerce_time(json!("12")).unwrap(), json!("1200"));
    }

    #[test]
    fn time_single_digit_string() {
        assert_eq!(coerce_time(json!("0")).unwrap(), json!("0000"));
    }

    #[test]
    fn time_hour_out_of_range() {
        assert!(coerce_time(json!(25)).is_err());
    }

    #[test]
    fn time_negative() {
        assert!(coerce_time(json!(-1)).is_err());
    }

    #[test]
    fn time_nonzero_minutes_rejected() {
        assert!(coerce_time(json!(1230)).is_err());
    }

    #[test]
    fn time_invalid_type() {
        assert!(coerce_time(json!(true)).is_err());
    }

    #[test]
    fn step_integer() {
        assert_eq!(coerce_step(json!(6)).unwrap(), json!("6"));
    }

    #[test]
    fn step_zero() {
        assert_eq!(coerce_step(json!(0)).unwrap(), json!("0"));
    }

    #[test]
    fn step_negative_rejected() {
        assert!(coerce_step(json!(-1)).is_err());
    }

    #[test]
    fn step_string_integer() {
        assert_eq!(coerce_step(json!("6")).unwrap(), json!("6"));
    }

    #[test]
    fn step_duration_format_rejected() {
        assert!(coerce_step(json!("1h30m")).is_err());
    }

    #[test]
    fn step_range_format() {
        assert_eq!(coerce_step(json!("0-12")).unwrap(), json!("0-12"));
    }

    #[test]
    fn step_invalid_string() {
        assert!(coerce_step(json!("abc")).is_err());
    }

    #[test]
    fn step_invalid_type() {
        assert!(coerce_step(json!(true)).is_err());
    }

    #[test]
    fn expver_integer_padded() {
        assert_eq!(coerce_expver(json!(7)).unwrap(), json!("0007"));
    }

    #[test]
    fn expver_zero() {
        assert_eq!(coerce_expver(json!(0)).unwrap(), json!("0000"));
    }

    #[test]
    fn expver_four_digit() {
        assert_eq!(coerce_expver(json!(9999)).unwrap(), json!("9999"));
    }

    #[test]
    fn expver_too_large() {
        assert!(coerce_expver(json!(10000)).is_err());
    }

    #[test]
    fn expver_numeric_string_padded() {
        assert_eq!(coerce_expver(json!("0007")).unwrap(), json!("0007"));
    }

    #[test]
    fn expver_numeric_string_unpadded() {
        assert_eq!(coerce_expver(json!("7")).unwrap(), json!("0007"));
    }

    #[test]
    fn expver_alpha_four_chars() {
        assert_eq!(coerce_expver(json!("abcd")).unwrap(), json!("abcd"));
    }

    #[test]
    fn expver_alpha_wrong_length() {
        assert!(coerce_expver(json!("ab")).is_err());
    }

    #[test]
    fn expver_invalid_type() {
        assert!(coerce_expver(json!(true)).is_err());
    }

    #[test]
    fn number_valid_integer() {
        assert_eq!(coerce_number(json!(1), false).unwrap(), json!("1"));
    }

    #[test]
    fn number_zero_rejected_by_default() {
        assert!(coerce_number(json!(0), false).is_err());
    }

    #[test]
    fn number_zero_allowed() {
        assert_eq!(coerce_number(json!(0), true).unwrap(), json!("0"));
    }

    #[test]
    fn number_negative_rejected() {
        assert!(coerce_number(json!(-1), true).is_err());
    }

    #[test]
    fn number_string() {
        assert_eq!(coerce_number(json!("5"), false).unwrap(), json!("5"));
    }

    #[test]
    fn number_invalid_type() {
        assert!(coerce_number(json!(true), false).is_err());
    }

    #[test]
    fn param_string_passthrough() {
        assert_eq!(coerce_passthrough(json!("2t")).unwrap(), json!("2t"));
    }

    #[test]
    fn param_numeric_passthrough() {
        assert_eq!(coerce_passthrough(json!(167)).unwrap(), json!("167"));
    }

    #[test]
    fn param_invalid_type() {
        assert!(coerce_passthrough(json!(true)).is_err());
    }

    #[test]
    fn lowercase_upper() {
        assert_eq!(
            coerce_lowercase(json!("SCENARIOMIP")).unwrap(),
            json!("scenariomip")
        );
    }

    #[test]
    fn lowercase_mixed() {
        assert_eq!(coerce_lowercase(json!("Mixed")).unwrap(), json!("mixed"));
    }

    #[test]
    fn lowercase_invalid_type() {
        assert!(coerce_lowercase(json!(123)).is_err());
    }

    #[test]
    fn slash_list_on_allowed_key() {
        let cfg = default_config();
        let result = coerce_value("param", json!("2t/msl"), &cfg).unwrap();
        assert_eq!(result, json!(["2t", "msl"]));
    }

    #[test]
    fn slash_list_not_split_on_disallowed_key() {
        let cfg = CoercionConfig {
            allow_lists: vec![],
            ..Default::default()
        };
        let result = coerce_value("param", json!("2t/msl"), &cfg).unwrap();
        assert_eq!(result, json!("2t/msl"));
    }

    #[test]
    fn range_date() {
        let cfg = default_config();
        let result = coerce_value("date", json!("20240101/to/20240105"), &cfg).unwrap();
        assert_eq!(result, json!("20240101/to/20240105"));
    }

    #[test]
    fn range_date_with_by() {
        let cfg = default_config();
        let result = coerce_value("date", json!("20240101/to/20240105/by/2"), &cfg).unwrap();
        assert_eq!(result, json!("20240101/to/20240105/by/2"));
    }

    #[test]
    fn range_not_expanded_on_disallowed_key() {
        let cfg = CoercionConfig {
            allow_ranges: vec![],
            allow_lists: vec!["date".into()],
            ..Default::default()
        };
        let result = coerce_value("date", json!("20240101/to/20240105"), &cfg);
        assert!(result.is_err() || result.unwrap() != json!("20240101/to/20240105"));
    }

    #[test]
    fn array_values_coerced_individually() {
        let cfg = default_config();
        let result = coerce_value("step", json!([1, 2, 3]), &cfg).unwrap();
        assert_eq!(result, json!(["1", "2", "3"]));
    }

    #[test]
    fn array_unknown_key_rendered_as_strings() {
        let cfg = default_config();
        let result = coerce_value("levtype", json!([1, "sfc"]), &cfg).unwrap();
        assert_eq!(result, json!(["1", "sfc"]));
    }

    #[test]
    fn request_non_object_wrapped() {
        let cfg = default_config();
        let result = coerce_request(&json!("just a string"), &cfg).unwrap();
        assert_eq!(result, json!({"data": "just a string"}));
    }

    #[test]
    fn request_unknown_keys_pass_through() {
        let cfg = default_config();
        let result = coerce_request(&json!({"levtype": "sfc", "domain": "g"}), &cfg).unwrap();
        assert_eq!(result["levtype"], json!("sfc"));
        assert_eq!(result["domain"], json!("g"));
    }

    #[test]
    fn request_duplicate_list_rejected() {
        let cfg = default_config();
        let err = coerce_request(&json!({"param": "2t/2t"}), &cfg).unwrap_err();
        assert!(err.to_string().contains("Duplicate values"));
    }

    #[test]
    fn request_multiple_errors_aggregated() {
        let cfg = default_config();
        let err = coerce_request(&json!({"date": "bad", "time": "bad"}), &cfg).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("date"), "should mention date: {msg}");
        assert!(msg.contains("time"), "should mention time: {msg}");
    }

    #[test]
    fn request_custom_allow_lists() {
        let cfg = CoercionConfig {
            allow_lists: vec![],
            ..Default::default()
        };
        let result = coerce_request(&json!({"param": "2t/msl"}), &cfg).unwrap();
        assert_eq!(
            result["param"],
            json!("2t/msl"),
            "slash should not be split"
        );
    }
}
