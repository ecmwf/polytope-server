//! User-facing error enrichment.
//!
//! A single outer middleware gives every request a request ID (minted in the
//! BITS ID format, or reused from an inbound `X-Request-Id`) and rewrites any
//! `>= 400` JSON response body into one self-contained, human-readable
//! `{"message": ...}` field that tells the user what to do and where to raise a
//! support ticket, quoting their request ID.
//!
//! Doing it in one place (rather than at every handler) means no error path can
//! be missed, and the body is guaranteed to be a flat string→string object so
//! it renders cleanly in the Python `polytope-client` (which flattens every
//! value into its output and crashes on non-string values).

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use serde_json::json;

use crate::auth::mock_roles::REQUEST_ID_HEADER;
use crate::state::AppState;

/// Cap on the error body we will buffer to rewrite. Error bodies are tiny JSON;
/// anything larger is left untouched rather than risk buffering a data stream.
const MAX_ERROR_BODY: usize = 64 * 1024;

/// The request ID for the current request, injected into request extensions by
/// [`request_context_middleware`].
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// The support URL resolved for the authenticated user's realm. The auth
/// middleware attaches this to the response when a request is authenticated; the
/// outer middleware falls back to the deployment default when it is absent.
#[derive(Clone, Debug)]
pub struct SupportUrl(pub String);

/// Broad error families that share user-facing guidance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorClass {
    /// 401 / 403 — credentials or permissions.
    Auth,
    /// Other 4xx — the request itself.
    Client,
    /// 429 / 529 — transient overload; retryable.
    Overloaded,
    /// 5xx — a server-side fault.
    Server,
}

fn classify(status: u16) -> ErrorClass {
    match status {
        401 | 403 => ErrorClass::Auth,
        429 | 529 => ErrorClass::Overloaded,
        400..=499 => ErrorClass::Client,
        _ => ErrorClass::Server,
    }
}

/// Build the single user-facing `message` string for an error response.
///
/// `reason` is the original technical message (may be empty). `support_url` is
/// the resolved contact, and `request_id` is quoted only when non-empty.
fn compose_message(
    status: StatusCode,
    reason: &str,
    support_url: Option<&str>,
    request_id: &str,
) -> String {
    let reason = reason.trim().trim_end_matches('.').trim();
    let (lead, advice) = match classify(status.as_u16()) {
        ErrorClass::Auth => (
            "Your request was not authorised",
            "Check your credentials and try again. If you believe you should have access,",
        ),
        ErrorClass::Client => (
            "Your request could not be processed",
            "Check your request and try again. If you believe this is a mistake or need help,",
        ),
        ErrorClass::Overloaded => (
            "Polytope is temporarily overloaded and could not process your request",
            "Please wait a few seconds and retry. If this keeps happening,",
        ),
        ErrorClass::Server => (
            "Polytope encountered an internal error while handling your request",
            "This has been logged. Please retry shortly; if the problem persists,",
        ),
    };

    let mut msg = String::from(lead);
    if !reason.is_empty() {
        msg.push_str(": ");
        msg.push_str(reason);
    }
    msg.push_str(". ");
    msg.push_str(advice);

    match (support_url, request_id.is_empty()) {
        (Some(url), false) => {
            msg.push_str(&format!(
                " open a support ticket at {url} and quote your request ID {request_id}."
            ));
        }
        (Some(url), true) => msg.push_str(&format!(" open a support ticket at {url}.")),
        (None, false) => {
            msg.push_str(&format!(
                " contact Polytope support and quote your request ID {request_id}."
            ));
        }
        (None, true) => msg.push_str(" contact Polytope support."),
    }
    msg
}

/// Pull the original technical reason out of an existing error body, tolerating
/// both `{"error": ...}` and `{"message": ...}` shapes. Non-JSON or non-string
/// bodies yield an empty reason (the composed message still stands on its own).
fn extract_reason(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|x| x.as_str())
                .or_else(|| v.get("message").and_then(|x| x.as_str()))
                .map(str::to_string)
        })
        .unwrap_or_default()
}

async fn transform_error(resp: Response, state: &AppState, request_id: &str) -> Response {
    let (mut parts, body) = resp.into_parts();
    let status = parts.status;

    let support_url = parts
        .extensions
        .get::<SupportUrl>()
        .map(|s| s.0.clone())
        .or_else(|| state.support.default_url.clone());

    let bytes = axum::body::to_bytes(body, MAX_ERROR_BODY)
        .await
        .unwrap_or_default();
    let reason = extract_reason(&bytes);
    let message = compose_message(status, &reason, support_url.as_deref(), request_id);

    let new_body = serde_json::to_vec(&json!({ "message": message })).unwrap_or_default();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Ok(len) = HeaderValue::from_str(&new_body.len().to_string()) {
        parts.headers.insert(header::CONTENT_LENGTH, len);
    }
    Response::from_parts(parts, Body::from(new_body))
}

/// Outer middleware: assign a request ID, expose it on every response as
/// `X-Request-Id`, and rewrite error bodies into the support-guidance shape.
pub async fn request_context_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Response {
    let request_id = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            bits::request_id::encode(
                state.bits.site(),
                state.bits.env(),
                state.bits.broker_slot(),
                Utc::now(),
            )
            .unwrap_or_default()
        });

    if !request_id.is_empty() {
        if let Ok(hv) = HeaderValue::from_str(&request_id) {
            req.headers_mut()
                .insert(HeaderName::from_static(REQUEST_ID_HEADER), hv);
        }
        req.extensions_mut().insert(RequestId(request_id.clone()));
    }

    let resp = next.run(req).await;

    let mut resp = if resp.status().as_u16() >= 400 {
        transform_error(resp, &state, &request_id).await
    } else {
        resp
    };

    if !request_id.is_empty()
        && let Ok(hv) = HeaderValue::from_str(&request_id)
    {
        resp.headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), hv);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    const RID: &str = "3k7p9q2r5s8t1v4w6x0y2z5a8b";
    const ECMWF: &str = "https://support.ecmwf.int/";

    #[test]
    fn client_error_message_names_request_and_url_and_id() {
        let m = compose_message(
            StatusCode::NOT_FOUND,
            "unknown collection 'x'",
            Some(ECMWF),
            RID,
        );
        assert!(m.starts_with("Your request could not be processed: unknown collection 'x'."));
        assert!(m.contains(ECMWF));
        assert!(m.ends_with(&format!("quote your request ID {RID}.")));
    }

    #[test]
    fn auth_error_uses_permission_wording() {
        let m = compose_message(StatusCode::FORBIDDEN, "role check failed", Some(ECMWF), RID);
        assert!(m.starts_with("Your request was not authorised: role check failed."));
        assert!(m.contains("you should have access"));
    }

    #[test]
    fn server_error_wording_and_overloaded_wording_differ() {
        let s = compose_message(StatusCode::INTERNAL_SERVER_ERROR, "boom", Some(ECMWF), RID);
        assert!(s.starts_with("Polytope encountered an internal error"));
        let o = compose_message(
            StatusCode::from_u16(529).unwrap(),
            "broker at capacity",
            Some(ECMWF),
            RID,
        );
        assert!(o.starts_with("Polytope is temporarily overloaded"));
    }

    #[test]
    fn no_request_id_omits_the_quote_clause() {
        let m = compose_message(StatusCode::BAD_REQUEST, "bad", Some(ECMWF), "");
        assert!(m.contains(&format!("open a support ticket at {ECMWF}.")));
        assert!(!m.contains("request ID"));
    }

    #[test]
    fn no_support_url_still_gives_guidance() {
        let m = compose_message(StatusCode::BAD_REQUEST, "bad", None, RID);
        assert!(m.contains(&format!(
            "contact Polytope support and quote your request ID {RID}."
        )));
    }

    #[test]
    fn empty_reason_is_elided_cleanly() {
        let m = compose_message(StatusCode::NOT_FOUND, "", Some(ECMWF), RID);
        assert!(m.starts_with("Your request could not be processed. "));
    }

    #[test]
    fn extract_reason_reads_error_or_message_or_nothing() {
        assert_eq!(extract_reason(br#"{"error":"e"}"#), "e");
        assert_eq!(extract_reason(br#"{"message":"m"}"#), "m");
        assert_eq!(extract_reason(br#"{"retryable":true}"#), "");
        assert_eq!(extract_reason(b"not json"), "");
    }
}
