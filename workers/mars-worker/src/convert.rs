use std::collections::HashMap;

use serde_json::Value;

pub fn json_to_request(request: &Value) -> Result<HashMap<String, Vec<String>>, String> {
    let object = request
        .as_object()
        .ok_or_else(|| "request must be a JSON object".to_string())?;

    if object.is_empty() {
        return Err("request has no fields".to_string());
    }

    let mut out = HashMap::with_capacity(object.len());
    for (key, value) in object {
        let values = value_to_strings(key, value)?;
        out.insert(key.clone(), values);
    }

    Ok(out)
}

fn value_to_strings(key: &str, value: &Value) -> Result<Vec<String>, String> {
    match value {
        Value::String(s) => Ok(s.split('/').map(|p| p.trim().to_string()).collect()),
        Value::Number(n) => Ok(vec![n.to_string()]),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::String(s) => Ok(s.clone()),
                Value::Number(n) => Ok(n.to_string()),
                Value::Object(_) => Err(format!("unsupported nested object for key: {key}")),
                _ => Err(format!("unsupported value type for key: {key}")),
            })
            .collect(),
        Value::Object(map) => {
            if map.len() == 3
                && map.contains_key("start")
                && map.contains_key("end")
                && map.contains_key("step")
            {
                let start = map
                    .get("start")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| format!("unsupported value type for key: {key}"))?;
                let end = map
                    .get("end")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| format!("unsupported value type for key: {key}"))?;
                let step = map
                    .get("step")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| format!("unsupported value type for key: {key}"))?;

                Ok(vec![format!("{start}/to/{end}/by/{step}")])
            } else {
                Err(format!("unsupported nested object for key: {key}"))
            }
        }
        _ => Err(format!("unsupported value type for key: {key}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_conversion() {
        let input = json!({"class":"od", "stream":"oper", "param":["t", "q"]});
        let out = json_to_request(&input).expect("conversion should succeed");

        assert_eq!(out.get("class"), Some(&vec!["od".to_string()]));
        assert_eq!(out.get("stream"), Some(&vec!["oper".to_string()]));
        assert_eq!(
            out.get("param"),
            Some(&vec!["t".to_string(), "q".to_string()])
        );
    }

    #[test]
    fn test_numeric_array() {
        let input = json!({"step":[0, 12, 24]});
        let out = json_to_request(&input).expect("conversion should succeed");
        assert_eq!(
            out.get("step"),
            Some(&vec!["0".to_string(), "12".to_string(), "24".to_string()])
        );
    }

    #[test]
    fn test_single_string() {
        let input = json!({"class":"od"});
        let out = json_to_request(&input).expect("conversion should succeed");
        assert_eq!(out.get("class"), Some(&vec!["od".to_string()]));
    }

    #[test]
    fn test_single_number() {
        let input = json!({"step":12});
        let out = json_to_request(&input).expect("conversion should succeed");
        assert_eq!(out.get("step"), Some(&vec!["12".to_string()]));
    }

    #[test]
    fn test_range_object() {
        let input = json!({"step":{"start":1, "end":30, "step":2}});
        let out = json_to_request(&input).expect("conversion should succeed");
        assert_eq!(
            out.get("step"),
            Some(&vec![
                "1".to_string(),
                "to".to_string(),
                "30".to_string(),
                "by".to_string(),
                "2".to_string()
            ])
        );
    }

    #[test]
    fn test_empty_object_error() {
        let input = json!({});
        let err = json_to_request(&input).expect_err("conversion should fail");
        assert_eq!(err, "request has no fields");
    }

    #[test]
    fn test_null_error() {
        let input = Value::Null;
        let err = json_to_request(&input).expect_err("conversion should fail");
        assert_eq!(err, "request must be a JSON object");
    }
}
