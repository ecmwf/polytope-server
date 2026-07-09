// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use axum::http::{HeaderMap, HeaderName};

pub const MOCK_ROLES_HEADER: &str = "polytope-mock-roles";
pub const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockRoles {
    pub realm: String,
    pub roles: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockRolesAudit {
    pub real_username: String,
    pub real_realm: String,
    pub mocked_realm: String,
    pub mocked_roles: Vec<String>,
    pub path: String,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockRolesError {
    MultipleValues,
    NonUtf8,
    MissingColon,
    ExtraColon,
    EmptyRealm,
    EmptyRoleList,
    EmptyRole,
    ControlCharacter,
    AdminRole { role: String },
}

impl MockRolesError {
    pub fn message(&self) -> String {
        match self {
            Self::MultipleValues => "Polytope-Mock-Roles must be supplied at most once".to_string(),
            Self::NonUtf8 => "Polytope-Mock-Roles must be valid UTF-8".to_string(),
            Self::MissingColon => {
                "Polytope-Mock-Roles must have form <realm>:<role>,...".to_string()
            }
            Self::ExtraColon => "Polytope-Mock-Roles roles must not contain ':'".to_string(),
            Self::EmptyRealm => "Polytope-Mock-Roles realm must not be empty".to_string(),
            Self::EmptyRoleList => "Polytope-Mock-Roles must include at least one role".to_string(),
            Self::EmptyRole => "Polytope-Mock-Roles roles must not be empty".to_string(),
            Self::ControlCharacter => {
                "Polytope-Mock-Roles must not contain control characters".to_string()
            }
            Self::AdminRole { role } => {
                format!("Polytope-Mock-Roles must not include configured admin role '{role}'")
            }
        }
    }
}

pub fn has_mock_roles_header(headers: &HeaderMap) -> bool {
    headers.contains_key(MOCK_ROLES_HEADER)
}

pub fn parse_mock_roles_header(
    headers: &HeaderMap,
    admin_bypass_roles: &Option<HashMap<String, Vec<String>>>,
) -> Result<Option<MockRoles>, MockRolesError> {
    let name = HeaderName::from_static(MOCK_ROLES_HEADER);
    let mut values = headers.get_all(&name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(MockRolesError::MultipleValues);
    }
    let value = value.to_str().map_err(|_| MockRolesError::NonUtf8)?;
    parse_mock_roles_value(value, admin_bypass_roles).map(Some)
}

pub fn parse_mock_roles_value(
    value: &str,
    admin_bypass_roles: &Option<HashMap<String, Vec<String>>>,
) -> Result<MockRoles, MockRolesError> {
    if value.chars().any(char::is_control) {
        return Err(MockRolesError::ControlCharacter);
    }

    let value = value.trim();
    let (realm, roles_text) = value.split_once(':').ok_or(MockRolesError::MissingColon)?;
    let realm = realm.trim();
    if realm.is_empty() {
        return Err(MockRolesError::EmptyRealm);
    }
    if roles_text.contains(':') {
        return Err(MockRolesError::ExtraColon);
    }
    if roles_text.trim().is_empty() {
        return Err(MockRolesError::EmptyRoleList);
    }

    let roles: Vec<String> = roles_text
        .split(',')
        .map(str::trim)
        .map(|role| {
            if role.is_empty() {
                Err(MockRolesError::EmptyRole)
            } else {
                Ok(role.to_string())
            }
        })
        .collect::<Result<_, _>>()?;

    if let Some(admin_roles) = admin_bypass_roles
        .as_ref()
        .and_then(|roles| roles.get(realm))
        && let Some(role) = roles.iter().find(|role| admin_roles.contains(*role))
    {
        return Err(MockRolesError::AdminRole { role: role.clone() });
    }

    Ok(MockRoles {
        realm: realm.to_string(),
        roles,
    })
}

pub const MOCK_USER_HEADER: &str = "polytope-mock-user";

/// Mocked usernames must carry this prefix so an admin (via admin_bypass_roles)
/// can only impersonate clearly-synthetic identities (e.g. load-test users),
/// never a real account.
pub const MOCK_USER_PREFIX: &str = "mock-";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockUserError {
    MultipleValues,
    NonUtf8,
    ControlCharacter,
    Empty,
    BadPrefix,
    TooLong,
}

impl MockUserError {
    pub fn message(&self) -> String {
        match self {
            Self::MultipleValues => "Polytope-Mock-User must be supplied at most once".to_string(),
            Self::NonUtf8 => "Polytope-Mock-User must be valid UTF-8".to_string(),
            Self::ControlCharacter => {
                "Polytope-Mock-User must not contain control characters".to_string()
            }
            Self::Empty => "Polytope-Mock-User must not be empty".to_string(),
            Self::BadPrefix => format!("Polytope-Mock-User must start with '{MOCK_USER_PREFIX}'"),
            Self::TooLong => "Polytope-Mock-User must be at most 64 characters".to_string(),
        }
    }
}

pub fn has_mock_user_header(headers: &HeaderMap) -> bool {
    headers.contains_key(MOCK_USER_HEADER)
}

pub fn parse_mock_user_header(headers: &HeaderMap) -> Result<Option<String>, MockUserError> {
    let name = HeaderName::from_static(MOCK_USER_HEADER);
    let mut values = headers.get_all(&name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(MockUserError::MultipleValues);
    }
    let value = value.to_str().map_err(|_| MockUserError::NonUtf8)?;
    parse_mock_user_value(value).map(Some)
}

pub fn parse_mock_user_value(value: &str) -> Result<String, MockUserError> {
    if value.chars().any(char::is_control) {
        return Err(MockUserError::ControlCharacter);
    }
    let value = value.trim();
    if value.is_empty() {
        return Err(MockUserError::Empty);
    }
    if value.len() > 64 {
        return Err(MockUserError::TooLong);
    }
    if !value.starts_with(MOCK_USER_PREFIX) {
        return Err(MockUserError::BadPrefix);
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_user_accepts_mock_prefix_and_rejects_real_usernames() {
        // Synthetic mock identities are accepted.
        assert_eq!(parse_mock_user_value("mock-18").unwrap(), "mock-18");
        assert_eq!(parse_mock_user_value("  mock-abc  ").unwrap(), "mock-abc");
        // Real-looking usernames cannot be impersonated (no mock- prefix).
        assert_eq!(parse_mock_user_value("majh"), Err(MockUserError::BadPrefix));
        assert_eq!(
            parse_mock_user_value("admin"),
            Err(MockUserError::BadPrefix)
        );
        // Degenerate inputs are rejected.
        assert_eq!(parse_mock_user_value(""), Err(MockUserError::Empty));
        assert_eq!(
            parse_mock_user_value("mock-\u{0007}x"),
            Err(MockUserError::ControlCharacter)
        );
        assert_eq!(
            parse_mock_user_value(&format!("mock-{}", "x".repeat(70))),
            Err(MockUserError::TooLong)
        );
    }
    use axum::http::HeaderValue;

    fn bypass() -> Option<HashMap<String, Vec<String>>> {
        Some(HashMap::from([(
            "realm".to_string(),
            vec!["admin".to_string()],
        )]))
    }

    #[test]
    fn parses_valid_header_value() {
        let parsed = parse_mock_roles_value(" realm : viewer, data ", &bypass()).unwrap();
        assert_eq!(parsed.realm, "realm");
        assert_eq!(parsed.roles, vec!["viewer", "data"]);
    }

    #[test]
    fn rejects_malformed_values() {
        for (value, expected) in [
            ("realm", MockRolesError::MissingColon),
            ("realm:", MockRolesError::EmptyRoleList),
            (":role", MockRolesError::EmptyRealm),
            ("realm:role,", MockRolesError::EmptyRole),
            ("realm:role,,other", MockRolesError::EmptyRole),
            ("realm:role:other", MockRolesError::ExtraColon),
            ("realm:ro\u{7}le", MockRolesError::ControlCharacter),
        ] {
            assert_eq!(parse_mock_roles_value(value, &bypass()), Err(expected));
        }
    }

    #[test]
    fn rejects_configured_admin_role_for_mocked_realm() {
        assert_eq!(
            parse_mock_roles_value("realm:admin", &bypass()),
            Err(MockRolesError::AdminRole {
                role: "admin".to_string()
            })
        );
    }

    #[test]
    fn rejects_multiple_header_values() {
        let mut headers = HeaderMap::new();
        headers.append(MOCK_ROLES_HEADER, HeaderValue::from_static("realm:viewer"));
        headers.append(MOCK_ROLES_HEADER, HeaderValue::from_static("realm:data"));
        assert_eq!(
            parse_mock_roles_header(&headers, &bypass()),
            Err(MockRolesError::MultipleValues)
        );
    }

    #[test]
    fn rejects_non_utf8_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            MOCK_ROLES_HEADER,
            HeaderValue::from_bytes(b"realm:\xff").unwrap(),
        );
        assert_eq!(
            parse_mock_roles_header(&headers, &bypass()),
            Err(MockRolesError::NonUtf8)
        );
    }
}
