use std::collections::HashMap;

use authotron_types::User as AuthUser;

pub fn is_admin_bypass_user(
    user: &AuthUser,
    bypass: &Option<HashMap<String, Vec<String>>>,
) -> bool {
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
        assert!(is_admin_bypass_user(
            &user("ecmwf", &["polytope-admin"]),
            &b
        ));
    }

    #[test]
    fn admin_in_wrong_realm_fails() {
        let b = bypass(&[("ecmwf", &["polytope-admin"])]);
        assert!(!is_admin_bypass_user(
            &user("efas", &["polytope-admin"]),
            &b
        ));
    }

    #[test]
    fn non_admin_fails() {
        let b = bypass(&[("ecmwf", &["polytope-admin"])]);
        assert!(!is_admin_bypass_user(&user("ecmwf", &["viewer"]), &b));
    }

    #[test]
    fn no_bypass_config_fails() {
        assert!(!is_admin_bypass_user(
            &user("ecmwf", &["polytope-admin"]),
            &None
        ));
    }
}
