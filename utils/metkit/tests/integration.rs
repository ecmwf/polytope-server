use std::collections::HashMap;

#[test]
fn expand_basic() {
    let mut fields = HashMap::new();
    fields.insert("class".to_string(), vec!["od".to_string()]);
    fields.insert("type".to_string(), vec!["an".to_string()]);
    fields.insert("stream".to_string(), vec!["oper".to_string()]);
    fields.insert("expver".to_string(), vec!["1".to_string()]);
    fields.insert("date".to_string(), vec!["20240101".to_string()]);
    fields.insert("time".to_string(), vec!["0000".to_string()]);
    fields.insert("levtype".to_string(), vec!["sfc".to_string()]);
    fields.insert("step".to_string(), vec!["0".to_string()]);
    fields.insert("param".to_string(), vec!["2t".to_string()]);

    let result = metkit::expand("retrieve", fields);
    assert!(result.is_ok(), "expand returned error: {:?}", result.err());
    let expanded = result.unwrap();
    assert!(
        expanded.contains_key("class"),
        "expanded result missing 'class'"
    );
    assert!(
        expanded.contains_key("type"),
        "expanded result missing 'type'"
    );
    assert!(
        expanded.contains_key("stream"),
        "expanded result missing 'stream'"
    );
    // Expansion may add extra fields (domain, etc.) - just verify input fields preserved
    assert!(!expanded.is_empty(), "expanded result should not be empty");
}

#[test]
fn expand_empty() {
    // Expanding an empty request with just a verb
    let result = metkit::expand("retrieve", HashMap::new());
    // Either succeeds (with defaults) or returns an error - both acceptable
    // What matters: no crash, no segfault
    match result {
        Ok(expanded) => {
            // If it succeeds, result should at least be a valid map
            let _ = expanded;
        }
        Err(e) => {
            // If it errors, the error message should be non-empty
            assert!(
                !e.to_string().is_empty(),
                "error message should not be empty"
            );
        }
    }
}

#[test]
fn expand_json_roundtrip() {
    let request = serde_json::json!({
        "verb": "retrieve",
        "class": "od",
        "type": "an",
        "stream": ["oper"],
        "expver": "1",
        "date": "20240101",
        "time": "0000",
        "levtype": "sfc",
        "step": "0",
        "param": "2t"
    });

    let result = metkit::expand_json(&request);
    assert!(
        result.is_ok(),
        "expand_json returned error: {:?}",
        result.err()
    );
    let expanded = result.unwrap();
    assert!(
        expanded.is_object(),
        "expanded result should be a JSON object"
    );
    let obj = expanded.as_object().unwrap();
    assert_eq!(
        obj.get("verb").and_then(|v| v.as_str()),
        Some("retrieve"),
        "expanded result should preserve verb"
    );
    assert!(obj.contains_key("class"), "expanded result missing 'class'");
    assert!(obj.contains_key("type"), "expanded result missing 'type'");
}

#[test]
fn expand_adds_step_when_missing() {
    let request = serde_json::json!({
        "verb": "retrieve",
        "class": "od",
        "type": "an",
        "stream": "oper",
        "expver": "1",
        "date": "20240101",
        "time": "0000",
        "levtype": "sfc",
        "param": "2t"
    });

    let result = metkit::expand_json(&request).expect("expand_json failed");
    let obj = result.as_object().expect("result is not an object");
    assert!(
        obj.contains_key("step"),
        "expansion should add 'step' when missing, got keys: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
}

#[test]
fn expand_multiple_steps() {
    let request = serde_json::json!({
        "verb": "retrieve",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "1",
        "date": "20240101",
        "time": "0000",
        "levtype": "sfc",
        "param": "2t",
        "step": ["0", "6", "12", "24"]
    });

    let result = metkit::expand_json(&request).expect("expand_json failed");
    let obj = result.as_object().expect("result is not an object");
    let step = obj.get("step").expect("expanded result missing 'step'");
    assert!(
        step.is_array(),
        "multiple steps should remain as an array, got: {step}"
    );
    let arr = step.as_array().unwrap();
    assert_eq!(arr.len(), 4, "should have 4 step values, got {}", arr.len());
}

#[test]
fn expand_relative_date() {
    let request = serde_json::json!({
        "verb": "retrieve",
        "class": "od",
        "type": "an",
        "stream": "oper",
        "expver": "1",
        "date": "-1",
        "time": "0000",
        "levtype": "sfc",
        "param": "2t",
        "step": "0"
    });

    let result = metkit::expand_json(&request).expect("expand_json failed");
    let obj = result.as_object().expect("result is not an object");
    let date = obj
        .get("date")
        .and_then(|v| v.as_str())
        .expect("expanded result missing 'date' string");
    assert_ne!(
        date, "-1",
        "relative date '-1' should be resolved to a real date"
    );
    assert!(
        date.len() == 8,
        "expanded date should be a full date (YYYYMMDD), got: {date}"
    );
}

#[test]
fn expand_thread_safety() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait(); // all threads start simultaneously
            let mut fields = HashMap::new();
            fields.insert("class".to_string(), vec!["od".to_string()]);
            fields.insert("type".to_string(), vec!["an".to_string()]);
            fields.insert("stream".to_string(), vec!["oper".to_string()]);
            metkit::expand("retrieve", fields)
        }));
    }

    for handle in handles {
        let result = handle.join().expect("thread should not panic");
        assert!(
            result.is_ok(),
            "concurrent expand returned error: {:?}",
            result.err()
        );
    }
}
