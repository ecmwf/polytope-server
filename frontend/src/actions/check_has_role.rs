use std::collections::HashMap;

use async_trait::async_trait;
use authotron_types::User as AuthUser;
use bits::Job;
use bits::actions::{ActionError, CheckAction, CheckResult};
use serde::{Deserialize, Serialize};

const ACCESS_DENIED: &str = "insufficient permissions";

#[derive(Debug, Serialize, Deserialize)]
pub struct HasRole {
    #[serde(rename = "type", default)]
    _action_type: Option<String>,
    #[serde(flatten)]
    pub roles: HashMap<String, Vec<String>>,
}

#[async_trait]
impl CheckAction for HasRole {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let Some(auth_value) = job.user.get("auth") else {
            tracing::warn!("has_role: no authentication context in job");
            return Ok(CheckResult::Reject {
                reason: ACCESS_DENIED.to_string(),
                silent: false,
            });
        };

        let auth_user: AuthUser = serde_json::from_value(auth_value.clone())
            .map_err(|e| ActionError::AuthError(format!("invalid auth context: {}", e)))?;

        if auth_user.version != 1 {
            tracing::warn!(
                version = auth_user.version,
                "has_role: unsupported auth schema version, expected 1"
            );
            return Ok(CheckResult::Reject {
                reason: ACCESS_DENIED.to_string(),
                silent: false,
            });
        }

        if job
            .user
            .get("can_bypass_role_check")
            .and_then(|v| v.as_bool())
            == Some(true)
        {
            return Ok(CheckResult::Pass);
        }

        if let Some(allowed_roles) = self.roles.get(&auth_user.realm) {
            if allowed_roles.iter().any(|r| auth_user.roles.contains(r)) {
                return Ok(CheckResult::Pass);
            }
            tracing::warn!(
                user = auth_user.username,
                realm = auth_user.realm,
                "has_role: realm matched but user lacks a required role"
            );
        } else {
            tracing::warn!(
                user = auth_user.username,
                realm = auth_user.realm,
                "has_role: user realm not listed in allowed realms"
            );
        }

        Ok(CheckResult::Reject {
            reason: ACCESS_DENIED.to_string(),
            silent: false,
        })
    }
}

bits::register_action!(check, "has_role", HasRole);

#[cfg(test)]
mod has_role_tests {
    use super::*;
    use authotron_types::User;
    use serde_json::json;

    fn job_with_auth(auth_user: &User) -> Job {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({
            "client_ip": "1.2.3.4",
            "auth": serde_json::to_value(auth_user).unwrap(),
        });
        job
    }

    fn test_user(roles: Vec<&str>, realm: &str) -> User {
        User {
            version: 1,
            username: "alice".to_string(),
            realm: realm.to_string(),
            roles: roles.into_iter().map(String::from).collect(),
            attributes: HashMap::new(),
            scopes: HashMap::new(),
        }
    }

    fn has_role(entries: &[(&str, &[&str])]) -> HasRole {
        HasRole {
            _action_type: None,
            roles: entries
                .iter()
                .map(|(realm, roles)| {
                    (
                        realm.to_string(),
                        roles.iter().map(|r| r.to_string()).collect(),
                    )
                })
                .collect(),
        }
    }

    fn assert_reject(result: CheckResult) {
        match result {
            CheckResult::Reject { reason, silent } => {
                assert!(!silent, "HasRole rejections must be non-silent");
                assert_eq!(reason, ACCESS_DENIED);
            }
            _ => panic!("expected Reject, got Pass"),
        }
    }

    #[tokio::test]
    async fn single_realm_pass() {
        let user = test_user(vec!["data_access", "default"], "ecmwf");
        let job = job_with_auth(&user);
        let result = has_role(&[("ecmwf", &["data_access"])])
            .evaluate(&job)
            .await
            .unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn single_realm_wrong_role() {
        let user = test_user(vec!["default"], "ecmwf");
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["admin"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn single_realm_wrong_realm() {
        let user = test_user(vec!["admin"], "other");
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["admin"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn multi_realm_one_matches() {
        let user = test_user(vec!["data_access"], "cds");
        let job = job_with_auth(&user);
        let result = has_role(&[("ecmwf", &["admin"]), ("cds", &["data_access"])])
            .evaluate(&job)
            .await
            .unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn multi_realm_none_match() {
        let user = test_user(vec!["viewer"], "ecmwf");
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["admin"]), ("cds", &["data_access"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn multi_realm_role_in_wrong_realm() {
        let user = test_user(vec!["admin", "data_access"], "other");
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["admin"]), ("cds", &["data_access"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn same_realm_multiple_roles() {
        let user = test_user(vec!["viewer", "default"], "ecmwf");
        let job = job_with_auth(&user);
        let result = has_role(&[("ecmwf", &["admin", "viewer"])])
            .evaluate(&job)
            .await
            .unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn empty_roles_map_rejects() {
        let user = test_user(vec!["admin"], "ecmwf");
        let job = job_with_auth(&user);
        assert_reject(has_role(&[]).evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn realm_with_empty_allowed_roles_rejects() {
        let user = test_user(vec!["admin", "default"], "ecmwf");
        let job = job_with_auth(&user);
        assert_reject(has_role(&[("ecmwf", &[])]).evaluate(&job).await.unwrap());
    }

    #[tokio::test]
    async fn user_with_no_roles_rejects() {
        let user = test_user(vec![], "ecmwf");
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["data_access"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn bypass_flag_passes_any_check() {
        let user = test_user(vec!["viewer"], "ecmwf");
        let mut job = job_with_auth(&user);
        job.user_mut()["can_bypass_role_check"] = json!(true);
        let result = has_role(&[("ecmwf", &["some_other_role"])])
            .evaluate(&job)
            .await
            .unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn bypass_flag_passes_even_unlisted_realm() {
        let user = test_user(vec!["viewer"], "unknown_realm");
        let mut job = job_with_auth(&user);
        job.user_mut()["can_bypass_role_check"] = json!(true);
        let result = has_role(&[("ecmwf", &["admin"])])
            .evaluate(&job)
            .await
            .unwrap();
        assert!(matches!(result, CheckResult::Pass));
    }

    #[tokio::test]
    async fn no_bypass_flag_still_checks_roles() {
        let user = test_user(vec!["viewer"], "ecmwf");
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["admin"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn no_auth_context() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"client_ip": "1.2.3.4"});
        assert_reject(
            has_role(&[("ecmwf", &["admin"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn malformed_auth() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({"auth": "not a valid User object"});
        assert!(matches!(
            has_role(&[("ecmwf", &["admin"])]).evaluate(&job).await,
            Err(ActionError::AuthError(_))
        ));
    }

    #[tokio::test]
    async fn malformed_roles_type_returns_auth_error() {
        let mut job = Job::new(json!({}));
        *job.user_mut() = json!({
            "auth": {
                "version": 1,
                "username": "alice",
                "realm": "ecmwf",
                "roles": "admin"
            }
        });
        assert!(matches!(
            has_role(&[("ecmwf", &["admin"])]).evaluate(&job).await,
            Err(ActionError::AuthError(_))
        ),);
    }

    #[tokio::test]
    async fn unsupported_version() {
        let mut user = test_user(vec!["data_access"], "ecmwf");
        user.version = 2;
        let job = job_with_auth(&user);
        assert_reject(
            has_role(&[("ecmwf", &["data_access"])])
                .evaluate(&job)
                .await
                .unwrap(),
        );
    }

    #[test]
    fn deserialize_flat_yaml() {
        let yaml = r#"
ecmwf:
  - admin
  - data_access
cds:
  - viewer
"#;
        let check: HasRole = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(check.roles.len(), 2);
        assert_eq!(check.roles["ecmwf"], vec!["admin", "data_access"]);
        assert_eq!(check.roles["cds"], vec!["viewer"]);
    }

    #[test]
    fn deserialize_with_type_field() {
        let yaml = r#"
type: has_role
ecmwf:
  - admin
"#;
        let check: HasRole = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(check.roles.len(), 1);
        assert!(check.roles.contains_key("ecmwf"));
        assert!(!check.roles.contains_key("type"));
    }
}
