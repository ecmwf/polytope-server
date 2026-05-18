use crate::redaction::{redact_field, redact_json, redact_string};
use crate::resource::Resource;
use serde_json::{Map, Number, Value};
use std::fmt;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, format::Writer};
use tracing_subscriber::registry::LookupSpan;

const REQUEST_PREFIX: &str = "__POLYTOPE_REQUEST__";
const REQUEST_LIMIT: usize = 32 * 1024;

pub const DEFAULT_LOG_MAX_STRING_LENGTH: usize = 1000;
pub const DEFAULT_LOG_MAX_LIST_LENGTH: usize = 100;
pub const DEFAULT_LOG_LIST_PREVIEW_LENGTH: usize = 10;

pub fn bounded_request(value: &serde_json::Value) -> serde_json::Value {
    bounded_request_with(
        value,
        DEFAULT_LOG_MAX_STRING_LENGTH,
        DEFAULT_LOG_MAX_LIST_LENGTH,
        DEFAULT_LOG_LIST_PREVIEW_LENGTH,
    )
}

pub fn bounded_request_with(
    value: &serde_json::Value,
    max_string_length: usize,
    max_list_length: usize,
    list_preview_length: usize,
) -> serde_json::Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        bounded_request_with(
                            value,
                            max_string_length,
                            max_list_length,
                            list_preview_length,
                        ),
                    )
                })
                .collect(),
        ),
        Value::Array(items) if items.len() > max_list_length => {
            let preview_len = std::cmp::min(list_preview_length, max_list_length);
            Value::Object(Map::from_iter([
                ("_summary".to_string(), Value::String("list".to_string())),
                ("count".to_string(), Value::Number(items.len().into())),
                (
                    "preview".to_string(),
                    Value::Array(
                        items
                            .iter()
                            .take(preview_len)
                            .map(|value| {
                                bounded_request_with(
                                    value,
                                    max_string_length,
                                    max_list_length,
                                    list_preview_length,
                                )
                            })
                            .collect(),
                    ),
                ),
            ]))
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|value| {
                    bounded_request_with(
                        value,
                        max_string_length,
                        max_list_length,
                        list_preview_length,
                    )
                })
                .collect(),
        ),
        Value::String(s) if s.chars().count() > max_string_length => {
            // The limit is in Unicode scalar values, matching Python string slicing.
            // Convert the character boundary back to a byte index before slicing UTF-8.
            let byte_index = s
                .char_indices()
                .nth(max_string_length)
                .map(|(index, _)| index)
                .unwrap_or(s.len());
            Value::String(format!("{}...<truncated>", &s[..byte_index]))
        }
        other => other.clone(),
    }
}

#[derive(Debug, Clone)]
pub struct OtelJsonFormatter {
    resource: Resource,
}

impl OtelJsonFormatter {
    pub fn new(service_name: &'static str) -> Self {
        Self {
            resource: Resource::from_env(service_name),
        }
    }
}

pub fn request(value: &serde_json::Value) -> RequestValue<'_> {
    RequestValue(value)
}

pub struct RequestValue<'a>(&'a serde_json::Value);
impl fmt::Display for RequestValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bounded = bounded_request(self.0);
        let compact = serde_json::to_string(&bounded).map_err(|_| fmt::Error)?;
        if compact.len() > REQUEST_LIMIT {
            write!(
                f,
                "{}{}",
                REQUEST_PREFIX,
                serde_json::json!({"truncated": true, "size_bytes": compact.len()})
            )
        } else {
            write!(f, "{}{}", REQUEST_PREFIX, compact)
        }
    }
}
impl fmt::Debug for RequestValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Default, Debug, Clone)]
pub struct JsonFields(pub Map<String, Value>);

#[derive(Default, Debug, Clone)]
pub struct JsonFieldFormatter;

impl<'writer> FormatFields<'writer> for JsonFieldFormatter {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        fields.record(&mut visitor);
        write!(writer, "{}", Value::Object(visitor.fields))
    }
}

#[derive(Default)]
struct JsonVisitor {
    fields: Map<String, Value>,
    message: Option<String>,
}

impl JsonVisitor {
    fn insert(&mut self, field: &Field, value: Value) {
        let name = field.name();
        if name == "message" {
            self.message = Some(match value {
                Value::String(s) => redact_string(&s),
                other => redact_string(&other.to_string()),
            });
            return;
        }
        let value = match value {
            Value::String(s) if name == "polytope.request" => parse_request(&s),
            Value::String(s) => Value::String(redact_field(name, &s)),
            other => redact_json(other),
        };
        self.fields.insert(name.to_string(), value);
    }
}

fn parse_request(s: &str) -> Value {
    if let Some(json) = s.strip_prefix(REQUEST_PREFIX) {
        serde_json::from_str(json)
            .map(redact_json)
            .unwrap_or_else(|_| Value::String(redact_string(s)))
    } else {
        Value::String(redact_string(s))
    }
}

impl Visit for JsonVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, Value::Number(value.into()));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, Value::Number(value.into()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::Bool(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, Value::String(value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.insert(field, Value::String(format!("{value:?}")));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(
            field,
            Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        );
    }
}

impl<S, N> FormatEvent<S, N> for OtelJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        let mut attributes = Map::new();
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let exts = span.extensions();
                if let Some(fields) = exts.get::<JsonFields>() {
                    for (k, v) in &fields.0 {
                        attributes.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                }
            }
        }
        for (k, v) in visitor.fields {
            attributes.insert(k, v);
        }
        attributes.insert(
            "code.target".to_string(),
            Value::String(event.metadata().target().to_string()),
        );

        let level = *event.metadata().level();
        let record = serde_json::json!({
            "timestamp": timestamp(),
            "severityText": level.as_str(),
            "severityNumber": severity_number(level),
            "body": visitor.message.unwrap_or_default(),
            "resource": self.resource.as_json(),
            "attributes": attributes,
        });
        writeln!(writer, "{}", record)
    }
}

fn timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
fn severity_number(level: tracing::Level) -> u8 {
    match level {
        tracing::Level::TRACE => 1,
        tracing::Level::DEBUG => 5,
        tracing::Level::INFO => 9,
        tracing::Level::WARN => 13,
        tracing::Level::ERROR => 17,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::capturing_subscriber;
    use tracing_subscriber::prelude::*;

    #[test]
    fn emits_required_shape_and_preserves_event_name() {
        let (layer, logs) = capturing_subscriber("svc");
        let sub = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(sub);
        tracing::info!(
            "event.name" = "api.collection.list",
            job.id = "job-1",
            answer = 42_u64,
            "hello Bearer FAKETOKEN_OBSERVABILITY_PROBE"
        );
        let lines = logs.json_lines();
        let line = &lines[0];
        assert_eq!(line["severityText"], "INFO");
        assert_eq!(line["severityNumber"], 9);
        assert_eq!(line["attributes"]["event.name"], "api.collection.list");
        assert_eq!(line["attributes"]["job.id"], "job-1");
        assert!(
            line["attributes"]["code.target"]
                .as_str()
                .unwrap()
                .contains("observability")
        );
        assert!(
            !logs
                .raw_lines()
                .join("\n")
                .contains("FAKETOKEN_OBSERVABILITY_PROBE")
        );
    }

    #[test]
    fn bits_event_has_no_synthesised_event_name() {
        let (layer, logs) = capturing_subscriber("svc");
        let sub = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(sub);
        tracing::info!(target: "bits::broker", "bits message");
        let line = &logs.json_lines()[0];
        assert_eq!(line["attributes"]["code.target"], "bits::broker");
        assert!(line["attributes"].get("event.name").is_none());
    }

    #[test]
    fn request_is_bounded_redacted_and_backstop_truncated() {
        let (layer, logs) = capturing_subscriber("svc");
        let sub = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(sub);
        let req = serde_json::json!({"class":"od","stream":"oper","token":"Bearer FAKETOKEN_OBSERVABILITY_PROBE"});
        tracing::info!("event.name" = "api.job.submitted", polytope.request=%request(&req), "submitted");
        let line = &logs.json_lines()[0];
        let parsed = &line["attributes"]["polytope.request"];
        assert_eq!(parsed["class"], "od");
        assert_eq!(parsed["stream"], "oper");
        assert_eq!(parsed["token"], "[REDACTED]");
        assert!(
            !logs
                .raw_lines()
                .join("\n")
                .contains("FAKETOKEN_OBSERVABILITY_PROBE")
        );

        let big = serde_json::Value::Object(
            (0..4000)
                .map(|i| (format!("k{i}"), Value::String("abcdefghij".to_string())))
                .collect(),
        );
        tracing::info!("event.name" = "api.job.submitted", polytope.request=%request(&big), "submitted");
        let lines = logs.json_lines();
        assert_eq!(
            lines[1]["attributes"]["polytope.request"]["truncated"],
            true
        );
    }

    #[test]
    fn bounded_request_short_string_passes_through() {
        assert_eq!(
            bounded_request_with(&Value::String("abc".to_string()), 3, 3, 1),
            Value::String("abc".to_string())
        );
    }

    #[test]
    fn bounded_request_long_string_is_truncated() {
        assert_eq!(
            bounded_request_with(&Value::String("abcd".to_string()), 3, 3, 1),
            Value::String("abc...<truncated>".to_string())
        );
    }

    #[test]
    fn bounded_request_short_list_passes_through() {
        let value = serde_json::json!([1, 2]);
        assert_eq!(bounded_request_with(&value, 10, 3, 1), value);
    }

    #[test]
    fn bounded_request_list_at_exact_boundary_passes_through() {
        let value = serde_json::json!([1, 2, 3]);
        assert_eq!(bounded_request_with(&value, 10, 3, 1), value);
    }

    #[test]
    fn bounded_request_long_list_is_summarised() {
        let value = serde_json::json!([1, 2, 3, 4]);
        let bounded = bounded_request_with(&value, 10, 3, 2);
        assert_eq!(bounded["_summary"], "list");
        assert_eq!(bounded["count"], 4);
        assert_eq!(bounded["preview"], serde_json::json!([1, 2]));
    }

    #[test]
    fn bounded_request_preview_is_capped_by_max_list_length() {
        let value = serde_json::json!([1, 2, 3, 4]);
        assert_eq!(
            bounded_request_with(&value, 10, 3, 10)["preview"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn bounded_request_recurses_nested_objects() {
        let value = serde_json::json!({"outer": {"inner": ["abcd", "efgh", "ijkl"]}});
        let bounded = bounded_request_with(&value, 2, 2, 1);
        assert_eq!(bounded["outer"]["inner"]["_summary"], "list");
        assert_eq!(
            bounded["outer"]["inner"]["preview"],
            serde_json::json!(["ab...<truncated>"])
        );
    }

    #[test]
    fn bounded_request_multibyte_utf8_boundary_is_safe() {
        assert_eq!(
            bounded_request_with(&Value::String("ééé".to_string()), 2, 3, 1),
            Value::String("éé...<truncated>".to_string())
        );
    }
}
