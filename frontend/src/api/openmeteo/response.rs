use serde_json::{Map, Value, json};

pub struct VariableResult {
    pub name: String,
    pub unit: String,
    pub values: Vec<Option<f64>>,
}

pub fn build_forecast_response(
    latitude: f64,
    longitude: f64,
    elevation: Option<f64>,
    timezone: &str,
    utc_offset_seconds: i32,
    generationtime_ms: f64,
    hourly_times: &[String],
    hourly_results: &[VariableResult],
) -> Value {
    let mut hourly = Map::new();
    hourly.insert(
        "time".to_string(),
        Value::Array(hourly_times.iter().cloned().map(Value::String).collect()),
    );

    let mut hourly_units = Map::new();
    hourly_units.insert("time".to_string(), Value::String("iso8601".to_string()));

    for result in hourly_results {
        hourly.insert(
            result.name.clone(),
            Value::Array(
                result
                    .values
                    .iter()
                    .map(|v| v.map_or(Value::Null, Value::from))
                    .collect(),
            ),
        );
        hourly_units.insert(result.name.clone(), Value::String(result.unit.clone()));
    }

    let mut out = Map::new();
    out.insert("latitude".to_string(), json!(latitude));
    out.insert("longitude".to_string(), json!(longitude));
    if let Some(elevation) = elevation {
        out.insert("elevation".to_string(), json!(elevation));
    }
    out.insert("generationtime_ms".to_string(), json!(generationtime_ms));
    out.insert("utc_offset_seconds".to_string(), json!(utc_offset_seconds));
    out.insert("timezone".to_string(), json!(timezone));
    out.insert("timezone_abbreviation".to_string(), json!(timezone));
    out.insert("hourly".to_string(), Value::Object(hourly));
    out.insert("hourly_units".to_string(), Value::Object(hourly_units));

    Value::Object(out)
}

pub fn build_error_response(reason: &str) -> Value {
    json!({"error": true, "reason": reason})
}
