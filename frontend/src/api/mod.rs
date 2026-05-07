pub mod openmeteo;
pub mod v1;
pub mod v2;

use authotron_types::User as AuthUser;
use axum::http::HeaderMap;
use bits::Job;
use serde_json::{Value, json};

use crate::auth::{MockRolesAudit, is_admin_bypass_user};

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

pub fn audit_mock_job_submission(mock_audit: Option<&MockRolesAudit>, job_id: &str) {
    if let Some(audit) = mock_audit {
        tracing::info!(
            event = "polytope_mock_roles_job_submitted",
            real_username = audit.real_username.as_str(),
            real_realm = audit.real_realm.as_str(),
            mocked_realm = audit.mocked_realm.as_str(),
            mocked_roles = ?audit.mocked_roles,
            path = audit.path.as_str(),
            request_id = audit.request_id.as_deref(),
            job_id = job_id,
            "accepted mocked-role request submitted job"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::http::HeaderMap;
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
}
