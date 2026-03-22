use std::collections::HashMap;

#[cxx::bridge(namespace = "metkit::bridge")]
mod ffi {
    unsafe extern "C++" {
        include!("bridge.h");

        fn expand_request(
            verb: &str,
            keys: Vec<String>,
            values: Vec<String>,
            out_keys: &mut Vec<String>,
            out_values: &mut Vec<String>,
        ) -> Result<()>;
    }
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metkit error: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<cxx::Exception> for Error {
    fn from(e: cxx::Exception) -> Self {
        Self(e.what().to_string())
    }
}

pub fn expand(
    verb: &str,
    fields: HashMap<String, Vec<String>>,
) -> Result<HashMap<String, Vec<String>>, Error> {
    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (key, value_list) in &fields {
        for value in value_list {
            keys.push(key.clone());
            values.push(value.clone());
        }
    }

    let mut out_keys = Vec::new();
    let mut out_values = Vec::new();

    ffi::expand_request(verb, keys, values, &mut out_keys, &mut out_values)?;

    let mut result: HashMap<String, Vec<String>> = HashMap::new();
    for (k, v) in out_keys.into_iter().zip(out_values.into_iter()) {
        result.entry(k).or_default().push(v);
    }
    Ok(result)
}

pub fn expand_json(request: &serde_json::Value) -> Result<serde_json::Value, Error> {
    let obj = request
        .as_object()
        .ok_or_else(|| Error("request must be a JSON object".to_string()))?;

    let verb = obj
        .get("verb")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error("request must contain a \"verb\" string field".to_string()))?
        .to_string();

    let mut fields: HashMap<String, Vec<String>> = HashMap::new();
    for (key, val) in obj {
        if key == "verb" {
            continue;
        }
        let strings = json_value_to_strings(val)?;
        if !strings.is_empty() {
            fields.insert(key.clone(), strings);
        }
    }

    let expanded = expand(&verb, fields)?;

    let mut result = serde_json::Map::new();
    result.insert("verb".to_string(), serde_json::Value::String(verb));
    for (key, vals) in expanded {
        let json_val = if vals.len() == 1 {
            serde_json::Value::String(vals.into_iter().next().unwrap())
        } else {
            serde_json::Value::Array(vals.into_iter().map(serde_json::Value::String).collect())
        };
        result.insert(key, json_val);
    }

    Ok(serde_json::Value::Object(result))
}

fn json_value_to_strings(val: &serde_json::Value) -> Result<Vec<String>, Error> {
    match val {
        serde_json::Value::String(s) => Ok(vec![s.clone()]),
        serde_json::Value::Number(n) => Ok(vec![n.to_string()]),
        serde_json::Value::Array(arr) => {
            let mut result = Vec::new();
            for item in arr {
                match item {
                    serde_json::Value::String(s) => result.push(s.clone()),
                    serde_json::Value::Number(n) => result.push(n.to_string()),
                    other => {
                        return Err(Error(format!(
                            "array elements must be strings or numbers, got: {other}"
                        )))
                    }
                }
            }
            Ok(result)
        }
        other => Err(Error(format!(
            "field values must be string, number, or array, got: {other}"
        ))),
    }
}
