pub mod openmeteo;
pub mod v1;
pub mod v2;

use std::convert::Infallible;

use authotron_types::User as AuthUser;
use axum::Json;
use axum::extract::FromRequestParts;
use axum::http::{HeaderMap, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
use bits::Job;
use serde_json::{Value, json};

use crate::auth::mock_time::{MOCK_TIME_HEADER, normalise_mocked_now};
use crate::auth::{MockRolesAudit, MockTime, MockTimeAudit, is_admin_bypass_user};

const OVERLOADED_RETRY_AFTER_SECS: &str = "5";

pub fn overloaded_response(payload: Value) -> Response {
    (
        StatusCode::from_u16(529).expect("529 is a valid HTTP status code"),
        [(header::RETRY_AFTER, OVERLOADED_RETRY_AFTER_SECS)],
        Json(payload),
    )
        .into_response()
}

/// Flatten a v1-style `{"request": "...yaml..."}` or `{"request": {...}}` wrapper
/// into a top-level MARS field object. Passes through already-flat requests unchanged.
pub fn flatten_request(val: &mut Value) -> Result<(), String> {
    let obj = val
        .as_object_mut()
        .ok_or("request body must be a JSON object")?;

    obj.remove("verb");

    if let Some(inner) = obj.remove("request") {
        match inner {
            Value::Object(inner_obj) => {
                for (k, v) in inner_obj {
                    obj.entry(k).or_insert(v);
                }
            }
            Value::String(yaml_str) => {
                let parsed: Value = serde_yaml::from_str(&yaml_str)
                    .map_err(|e| format!("failed to parse request YAML: {e}"))?;
                if let Some(parsed_obj) = parsed.as_object() {
                    for (k, v) in parsed_obj {
                        obj.entry(k.clone()).or_insert(v.clone());
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub fn client_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-real-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
}

pub fn set_job_user_context(
    job: &mut Job,
    headers: &HeaderMap,
    auth_user: Option<&AuthUser>,
    mock_audit: Option<&MockRolesAudit>,
    admin_bypass_roles: &Option<std::collections::HashMap<String, Vec<String>>>,
) {
    let mut user_context = serde_json::Map::new();
    if let Some(ip) = client_ip(headers) {
        user_context.insert("client_ip".to_string(), json!(ip));
    }
    if let Some(user) = auth_user {
        if mock_audit.is_none() && is_admin_bypass_user(user, admin_bypass_roles) {
            user_context.insert("can_bypass_role_check".to_string(), json!(true));
        }
        user_context.insert("auth".to_string(), serde_json::to_value(user).unwrap());
    }
    job.user = Value::Object(user_context).into();
}

// Trust boundary invariant: request body, `original_request`, and transform output must never
// be merged into `job.metadata`. This helper only writes the mock-time carrier from trusted
// request extensions inserted by authentication middleware.
pub fn set_job_mock_time_metadata(job: &mut Job, mock_time: Option<&MockTime>) {
    let Some(mock_time) = mock_time else {
        return;
    };

    let mocked_now = normalise_mocked_now(mock_time.now);
    let metadata = job.metadata_mut();
    if !metadata.is_object() {
        *metadata = json!({});
    }

    let metadata = metadata.as_object_mut().expect("metadata is an object");
    let admin_overrides = metadata
        .entry("admin_overrides".to_string())
        .or_insert_with(|| json!({}));
    if !admin_overrides.is_object() {
        *admin_overrides = json!({});
    }

    admin_overrides
        .as_object_mut()
        .expect("admin_overrides is an object")
        .insert("mock_now_rfc3339".to_string(), json!(mocked_now));
}

pub struct MockTimeSubmissionExtensions {
    pub mock_time: Option<MockTime>,
    pub mock_time_audit: Option<MockTimeAudit>,
}

impl<S> FromRequestParts<S> for MockTimeSubmissionExtensions
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self {
            mock_time: parts.extensions.get::<MockTime>().cloned(),
            mock_time_audit: parts.extensions.get::<MockTimeAudit>().cloned(),
        })
    }
}

pub fn audit_mock_job_submission(mock_audit: Option<&MockRolesAudit>, job_id: &str) {
    if let Some(audit) = mock_audit {
        tracing::info!(
            "event.name" = "api.auth.mock_accepted",
            real_username = audit.real_username.as_str(),
            real_realm = audit.real_realm.as_str(),
            mocked_realm = audit.mocked_realm.as_str(),
            mocked_roles = ?audit.mocked_roles,
            path = audit.path.as_str(),
            request_id = audit.request_id.as_deref(),
            request.id = job_id,
            "accepted mocked-role request submitted job"
        );
    }
}

pub fn audit_mock_time_job_submission(mock_audit: Option<&MockTimeAudit>, job_id: &str) {
    if let Some(audit) = mock_audit {
        tracing::info!(
            "event.name" = "api.auth.mock_accepted",
            real_username = audit.real_username.as_str(),
            real_realm = audit.real_realm.as_str(),
            mocked_now = audit.mocked_now.as_str(),
            path = audit.path.as_str(),
            request_id = audit.request_id.as_deref(),
            header = MOCK_TIME_HEADER,
            request.id = job_id,
            "accepted mocked-time request submitted job"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::http::HeaderMap;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::*;

    fn user(realm: &str, roles: &[&str]) -> AuthUser {
        AuthUser {
            version: 1,
            username: "alice".into(),
            realm: realm.into(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            attributes: HashMap::new(),
            scopes: HashMap::new(),
        }
    }

    fn bypass() -> Option<HashMap<String, Vec<String>>> {
        Some(HashMap::from([(
            "alpha".to_string(),
            vec!["admin".to_string()],
        )]))
    }

    #[test]
    fn ordinary_admin_job_context_gets_bypass_flag() {
        let mut job = Job::new(json!({}));
        let user = user("alpha", &["admin"]);
        set_job_user_context(&mut job, &HeaderMap::new(), Some(&user), None, &bypass());
        assert_eq!(job.user["can_bypass_role_check"], json!(true));
        assert_eq!(job.user["auth"]["realm"], json!("alpha"));
    }

    #[test]
    fn mocked_job_context_never_gets_bypass_flag_and_keeps_effective_auth() {
        let mut job = Job::new(json!({}));
        let user = AuthUser {
            version: 1,
            username: "alice".into(),
            realm: "beta".into(),
            roles: vec!["viewer".into()],
            attributes: HashMap::new(),
            scopes: HashMap::new(),
        };
        let audit = MockRolesAudit {
            real_username: "alice".into(),
            real_realm: "alpha".into(),
            mocked_realm: "beta".into(),
            mocked_roles: vec!["viewer".into()],
            path: "/api/v2/all/requests".into(),
            request_id: None,
        };
        set_job_user_context(
            &mut job,
            &HeaderMap::new(),
            Some(&user),
            Some(&audit),
            &bypass(),
        );
        assert!(job.user.get("can_bypass_role_check").is_none());
        assert_eq!(job.user["auth"]["username"], json!("alice"));
        assert_eq!(job.user["auth"]["realm"], json!("beta"));
        assert_eq!(job.user["auth"]["roles"], json!(["viewer"]));
        assert_eq!(job.user["auth"]["attributes"], json!({}));
        assert_eq!(job.user["auth"]["scopes"], json!({}));
    }

    fn mock_time() -> MockTime {
        MockTime {
            now: Utc.with_ymd_and_hms(2040, 5, 6, 7, 8, 9).unwrap(),
        }
    }

    #[test]
    fn mock_time_metadata_absent_without_mock_time() {
        let mut job = Job::new(json!({}));
        set_job_mock_time_metadata(&mut job, None);

        assert!(job.metadata.get("admin_overrides").is_none());
        assert!(job.metadata.get("mock_now_rfc3339").is_none());
    }

    #[test]
    fn mock_time_metadata_is_namespaced_and_normalised() {
        let mut job = Job::new(json!({}));
        let mock_time = mock_time();
        set_job_mock_time_metadata(&mut job, Some(&mock_time));

        assert_eq!(
            job.metadata["admin_overrides"]["mock_now_rfc3339"],
            json!("2040-05-06T07:08:09Z")
        );
        assert!(job.metadata.get("mock_now_rfc3339").is_none());
    }

    #[test]
    fn mock_time_metadata_preserves_existing_keys_across_cow_write() {
        let mut original = Job::new(json!({}));
        original.metadata_mut()["accept_encoding"] = json!("gzip");
        original.metadata_mut()["admin_overrides"] = json!({"other": "kept"});

        let mut submitted = original.clone();
        let mock_time = mock_time();
        set_job_mock_time_metadata(&mut submitted, Some(&mock_time));

        assert_eq!(submitted.metadata["accept_encoding"], json!("gzip"));
        assert_eq!(
            submitted.metadata["admin_overrides"]["other"],
            json!("kept")
        );
        assert_eq!(
            submitted.metadata["admin_overrides"]["mock_now_rfc3339"],
            json!("2040-05-06T07:08:09Z")
        );
        assert!(
            original.metadata["admin_overrides"]
                .get("mock_now_rfc3339")
                .is_none()
        );
    }

    #[test]
    fn mock_time_helper_does_not_touch_job_user() {
        let mut job = Job::new(json!({}));
        job.user = json!({"auth": {"username": "alice"}}).into();
        let user_before = job.user.clone();

        let mock_time = mock_time();
        set_job_mock_time_metadata(&mut job, Some(&mock_time));

        assert_eq!(job.user, user_before);
    }

    #[test]
    fn mock_time_submission_audit_helper_accepts_normalised_audit_data() {
        let audit = MockTimeAudit {
            real_username: "alice".into(),
            real_realm: "alpha".into(),
            mocked_now: "2040-05-06T07:08:09Z".into(),
            path: "/api/v2/all/requests".into(),
            request_id: Some("request-123".into()),
            header: MOCK_TIME_HEADER,
        };

        audit_mock_time_job_submission(Some(&audit), "job-123");
        audit_mock_time_job_submission(None, "job-123");
    }
}
