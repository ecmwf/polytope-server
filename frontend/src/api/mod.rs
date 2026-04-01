pub mod openmeteo;
pub mod v1;
pub mod v2;

use std::collections::HashMap;

use authotron_types::User as AuthUser;
use axum::http::HeaderMap;
use serde_json::Value;

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

pub fn check_admin_bypass(user: &AuthUser, bypass: &Option<HashMap<String, Vec<String>>>) -> bool {
    let Some(bypass) = bypass else { return false };
    let Some(admin_roles) = bypass.get(&user.realm) else {
        return false;
    };
    admin_roles.iter().any(|r| user.roles.contains(r))
}

#[cfg(test)]
mod tests {
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

    fn bypass(entries: &[(&str, &[&str])]) -> Option<HashMap<String, Vec<String>>> {
        Some(
            entries
                .iter()
                .map(|(r, roles)| (r.to_string(), roles.iter().map(|s| s.to_string()).collect()))
                .collect(),
        )
    }

    #[test]
    fn admin_in_configured_realm_passes() {
        let b = bypass(&[("ecmwf", &["polytope-admin"])]);
        assert!(check_admin_bypass(&user("ecmwf", &["polytope-admin"]), &b));
    }

    #[test]
    fn admin_in_wrong_realm_fails() {
        let b = bypass(&[("ecmwf", &["polytope-admin"])]);
        assert!(!check_admin_bypass(&user("efas", &["polytope-admin"]), &b));
    }

    #[test]
    fn non_admin_fails() {
        let b = bypass(&[("ecmwf", &["polytope-admin"])]);
        assert!(!check_admin_bypass(&user("ecmwf", &["viewer"]), &b));
    }

    #[test]
    fn no_bypass_config_fails() {
        assert!(!check_admin_bypass(
            &user("ecmwf", &["polytope-admin"]),
            &None
        ));
    }
}
