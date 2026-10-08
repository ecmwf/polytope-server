// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Decides whether a v1 caller may be served a content-encoded result.
//!
//! The v1 API hands the client a result-store URL and the client downloads it
//! directly, so a `Content-Encoding` on that download has to be understood by
//! the client itself. polytope-client before 0.7.7 counted decoded bytes
//! against the (compressed) `Content-Length` and aborted every encoded
//! download with "Download failed: downloaded X byte(s) out of Y", so v1
//! forwards `Accept-Encoding` to workers only for callers that cope:
//! polytope-client 0.7.7 and later (see
//! <https://github.com/ecmwf/polytope-client/pull/26>), or a client that is
//! not a polytope-client at all. The one unidentified client that is still
//! withheld is a bare `python-requests/<version>` `User-Agent`: that is
//! precisely what polytope-client 0.7.6 and earlier sent, since they left
//! requests' default in place.

use crate::config::ResultEncodingConfig;

/// Product token polytope-client puts in its `User-Agent`, ahead of the
/// `python-requests/<version>` token requests adds.
const POLYTOPE_CLIENT_PRODUCT: &str = "polytope-client";

/// Product token the `requests` library adds. On its own it is indistinguishable
/// from polytope-client 0.7.6 and earlier, which sent nothing else.
const PYTHON_REQUESTS_PRODUCT: &str = "python-requests";

/// A `MAJOR.MINOR.PATCH` client version. Ordering is field order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClientVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

impl ClientVersion {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parse `MAJOR[.MINOR[.PATCH]]`, ignoring any suffix: `0.8.0.dev1`,
    /// `0.7.7rc1` and `0.7.7+local` all parse. Returns `None` when the leading
    /// component carries no number at all.
    pub fn parse(raw: &str) -> Option<Self> {
        let mut components = raw.trim().split('.');
        let major = leading_number(components.next()?)?;
        let minor = components.next().and_then(leading_number).unwrap_or(0);
        let patch = components.next().and_then(leading_number).unwrap_or(0);
        Some(Self::new(major, minor, patch))
    }
}

impl std::fmt::Display for ClientVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Leading decimal digits of a version component, so `7rc1` reads as `7`.
fn leading_number(component: &str) -> Option<u64> {
    let digits: String = component
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    digits.parse().ok()
}

/// How a `User-Agent` identifies its client, as far as encoded downloads are
/// concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientIdentity<'a> {
    /// A `polytope-client/<version>` token. The version is `""` for a
    /// versionless token, which fails to parse and so reads as too old.
    PolytopeClient(&'a str),
    /// requests' default `User-Agent` with no polytope-client token in front
    /// of it: every polytope-client up to 0.7.6 looked exactly like this, so
    /// it has to be assumed to be one of them.
    PythonRequests,
    /// Anything else, including an absent `User-Agent`.
    Other,
}

fn client_identity(user_agent: Option<&str>) -> ClientIdentity<'_> {
    let Some(user_agent) = user_agent else {
        return ClientIdentity::Other;
    };
    let mut identity = ClientIdentity::Other;
    for token in user_agent.split_whitespace() {
        let (product, version) = token.split_once('/').unwrap_or((token, ""));
        if product.eq_ignore_ascii_case(POLYTOPE_CLIENT_PRODUCT) {
            return ClientIdentity::PolytopeClient(version);
        }
        if product.eq_ignore_ascii_case(PYTHON_REQUESTS_PRODUCT) {
            identity = ClientIdentity::PythonRequests;
        }
    }
    identity
}

/// Resolved [`ResultEncodingConfig`], ready for per-request decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultEncodingPolicy {
    enabled: bool,
    min_polytope_client_version: ClientVersion,
}

impl ResultEncodingPolicy {
    /// Resolve the configured policy, rejecting a version that cannot be
    /// parsed rather than silently applying a different threshold.
    pub fn from_config(config: Option<&ResultEncodingConfig>) -> Result<Self, String> {
        let Some(config) = config else {
            return Ok(Self::default());
        };
        let min_polytope_client_version =
            ClientVersion::parse(&config.min_polytope_client_version).ok_or_else(|| {
                format!(
                    "result_encoding.min_polytope_client_version: '{}' is not a MAJOR.MINOR.PATCH version",
                    config.min_polytope_client_version
                )
            })?;
        Ok(Self {
            enabled: config.enabled,
            min_polytope_client_version,
        })
    }

    /// Whether a caller with this `User-Agent` can download a result that
    /// carries a `Content-Encoding`.
    ///
    /// Unidentified clients are trusted: decoding `Content-Encoding` is a
    /// baseline HTTP client responsibility, and only polytope-client needs
    /// protecting. A bare `python-requests` `User-Agent` is the exception,
    /// because that is exactly what polytope-client 0.7.6 and earlier sent.
    pub fn client_can_download_encoded_result(&self, user_agent: Option<&str>) -> bool {
        if !self.enabled {
            return false;
        }
        match client_identity(user_agent) {
            ClientIdentity::PolytopeClient(version) => ClientVersion::parse(version)
                .is_some_and(|version| version >= self.min_polytope_client_version),
            ClientIdentity::PythonRequests => false,
            ClientIdentity::Other => true,
        }
    }
}

impl Default for ResultEncodingPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            min_polytope_client_version: ClientVersion::parse(
                crate::config::DEFAULT_MIN_POLYTOPE_CLIENT_VERSION,
            )
            .expect("the default minimum client version parses"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ResultEncodingPolicy {
        ResultEncodingPolicy::default()
    }

    #[test]
    fn plain_requests_user_agent_is_a_pre_0_7_7_polytope_client() {
        // Every polytope-client up to 0.7.6 sent requests' default
        // User-Agent, so it is indistinguishable from a bare requests caller
        // and must be treated as unable to decode.
        assert!(
            !policy().client_can_download_encoded_result(Some("python-requests/2.34.2")),
            "requests' default User-Agent may be an old polytope-client"
        );
    }

    #[test]
    fn polytope_client_below_the_threshold_is_not_offered_an_encoding() {
        assert!(!policy().client_can_download_encoded_result(Some(
            "polytope-client/0.7.6 python-requests/2.34.2"
        )));
        assert!(!policy().client_can_download_encoded_result(Some(
            "polytope-client/0.6.12 python-requests/2.34.2"
        )));
    }

    #[test]
    fn polytope_client_at_or_above_the_threshold_is_offered_an_encoding() {
        assert!(policy().client_can_download_encoded_result(Some(
            "polytope-client/0.7.7 python-requests/2.34.2"
        )));
        assert!(policy().client_can_download_encoded_result(Some(
            "polytope-client/0.8.0.dev1 python-requests/2.34.2"
        )));
        assert!(policy().client_can_download_encoded_result(Some(
            "polytope-client/1.0.0rc2 python-requests/2.34.2"
        )));
    }

    #[test]
    fn unparsable_polytope_client_version_is_treated_as_too_old() {
        assert!(!policy().client_can_download_encoded_result(Some(
            "polytope-client/garbage python-requests/2.34.2"
        )));
        assert!(!policy().client_can_download_encoded_result(Some("polytope-client")));
        assert!(!policy().client_can_download_encoded_result(Some("polytope-client/")));
    }

    #[test]
    fn generic_http_clients_are_offered_an_encoding() {
        assert!(policy().client_can_download_encoded_result(Some("curl/8.5.0")));
        assert!(policy().client_can_download_encoded_result(Some("Mozilla/5.0")));
        assert!(
            policy().client_can_download_encoded_result(None),
            "a client without a User-Agent decodes Content-Encoding by definition"
        );
    }

    #[test]
    fn product_token_matching_is_case_insensitive_and_position_independent() {
        assert!(!policy().client_can_download_encoded_result(Some(
            "python-requests/2.34.2 Polytope-Client/0.7.6"
        )));
        assert!(policy().client_can_download_encoded_result(Some(
            "python-requests/2.34.2 POLYTOPE-CLIENT/0.7.7"
        )));
    }

    #[test]
    fn disabling_the_policy_withholds_encoding_from_every_client() {
        let policy = ResultEncodingPolicy::from_config(Some(&ResultEncodingConfig {
            enabled: false,
            min_polytope_client_version: "0.7.7".to_string(),
        }))
        .unwrap();
        assert!(!policy.client_can_download_encoded_result(Some("curl/8.5.0")));
        assert!(!policy.client_can_download_encoded_result(Some("polytope-client/9.9.9")));
        assert!(!policy.client_can_download_encoded_result(None));
    }

    #[test]
    fn a_configured_threshold_replaces_the_default() {
        let policy = ResultEncodingPolicy::from_config(Some(&ResultEncodingConfig {
            enabled: true,
            min_polytope_client_version: "1.2.3".to_string(),
        }))
        .unwrap();
        assert!(!policy.client_can_download_encoded_result(Some("polytope-client/0.7.7")));
        assert!(policy.client_can_download_encoded_result(Some("polytope-client/1.2.3")));
    }

    #[test]
    fn an_unparsable_configured_threshold_is_a_config_error() {
        let err = ResultEncodingPolicy::from_config(Some(&ResultEncodingConfig {
            enabled: true,
            min_polytope_client_version: "not-a-version".to_string(),
        }))
        .unwrap_err();
        assert!(err.contains("min_polytope_client_version"), "{err}");
    }

    #[test]
    fn versions_parse_leniently_and_order_by_component() {
        assert_eq!(
            ClientVersion::parse("0.7.7"),
            Some(ClientVersion::new(0, 7, 7))
        );
        assert_eq!(
            ClientVersion::parse("0.8.0.dev1"),
            Some(ClientVersion::new(0, 8, 0))
        );
        assert_eq!(
            ClientVersion::parse("0.7.7+local"),
            Some(ClientVersion::new(0, 7, 7))
        );
        assert_eq!(ClientVersion::parse("1"), Some(ClientVersion::new(1, 0, 0)));
        assert_eq!(
            ClientVersion::parse("0.10"),
            Some(ClientVersion::new(0, 10, 0))
        );
        assert_eq!(ClientVersion::parse("garbage"), None);
        assert_eq!(ClientVersion::parse(""), None);
        assert!(ClientVersion::new(0, 10, 0) > ClientVersion::new(0, 9, 9));
        assert!(ClientVersion::new(1, 0, 0) > ClientVersion::new(0, 99, 99));
    }
}
