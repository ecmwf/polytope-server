// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! User-facing error enrichment.
//!
//! A single outer middleware gives every API call a request ID -- taken from the
//! URL path for the endpoints that act on an existing request, otherwise minted
//! here and adopted by the job if the call submits one -- records it on a span
//! so every log line of the call carries it, and rewrites any
//! `>= 400` JSON response body into one self-contained, human-readable
//! `{"message": ...}` field that tells the user what to do and where to raise a
//! support ticket, quoting their request ID.
//!
//! Doing it in one place (rather than at every handler) means no error path can
//! be missed, and the body is guaranteed to be a flat string→string object so
//! it renders cleanly in the Python `polytope-client` (which flattens every
//! value into its output and crashes on non-string values).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::{Value, json};
use tracing::Instrument;

use crate::state::AppState;

/// Cap on the error body we will buffer to rewrite. Error bodies are tiny JSON;
/// anything larger is left untouched rather than risk buffering a data stream.
const MAX_ERROR_BODY: usize = 64 * 1024;

/// The request ID for the current request, injected into request extensions by
/// [`request_context_middleware`].
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// A request ID minted by [`request_context_middleware`] for a call that does
/// not act on an existing request. Pass it to [`new_job`].
#[derive(Clone, Debug)]
pub struct MintedRequestId {
    id: String,
    claimed: Arc<AtomicBool>,
}

impl MintedRequestId {
    fn new(id: String) -> Self {
        Self {
            id,
            claimed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn claim(&self) -> Option<String> {
        (!self.claimed.swap(true, Ordering::AcqRel)).then(|| self.id.clone())
    }
}

/// Create a job for the current call. The first job adopts the call's minted
/// ID, so the ID quoted and logged for the call is the job's ID. Further jobs
/// from the same call (e.g. openmeteo fan-out) get their own IDs from BITS, so
/// two jobs never share one.
pub fn new_job(minted: Option<&MintedRequestId>, request: Value) -> bits::Job {
    match minted.and_then(MintedRequestId::claim) {
        Some(id) => bits::Job::new_with_id(id, request),
        None => bits::Job::new(request),
    }
}

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
    msg.push_str(
        " Please also include the original request, client logs and script/notebook if possible.",
    );
    msg
}

fn preserves_native_error_shape(path: &str) -> bool {
    path == "/edr"
        || path.starts_with("/edr/")
        || path == "/openmeteo"
        || path.starts_with("/openmeteo/")
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

/// Extract the Polytope request ID from the URL path, for the endpoints that act
/// on an existing request. The ID is an opaque path segment; we do not parse or
/// validate its internal structure (see docs/request-ids.md).
fn request_id_from_path(method: &axum::http::Method, path: &str) -> Option<String> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let id = match segments.as_slice() {
        // v1 submits are `POST /api/v1/requests/{collection}`.
        ["api", "v1", "requests", _] if method == axum::http::Method::POST => return None,
        ["api", "v1" | "v2", "requests", id] => id,
        ["api", "v1", "downloads" | "uploads", id] => id,
        _ => return None,
    };
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_string())
}

fn mint_request_id(bits: &bits::Bits) -> Option<String> {
    bits::request_id::encode(
        bits.site(),
        bits.env(),
        bits.broker_slot(),
        chrono::Utc::now(),
    )
    .inspect_err(|err| tracing::error!(error = %err, "failed to mint request ID"))
    .ok()
}

/// Outer middleware: resolve or mint the call's request ID, run the call inside
/// a span carrying it, and rewrite error bodies into the support-guidance shape
/// quoting it. An inbound `X-Request-Id` header is never read.
pub async fn request_context_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Response {
    let request_id = match request_id_from_path(req.method(), req.uri().path()) {
        Some(id) => Some(id),
        None => mint_request_id(&state.bits).inspect(|id| {
            req.extensions_mut()
                .insert(MintedRequestId::new(id.clone()));
        }),
    };
    if let Some(id) = &request_id {
        req.extensions_mut().insert(RequestId(id.clone()));
    }
    let request_id = request_id.unwrap_or_default();

    let span = tracing::info_span!(
        "api_call",
        request.id = %request_id,
        http.request.method = %req.method(),
        url.path = %req.uri().path(),
    );
    let preserve_native_error_shape = preserves_native_error_shape(req.uri().path());
    let resp = next.run(req).instrument(span).await;

    let mut resp = if resp.status().as_u16() >= 400 && !preserve_native_error_shape {
        transform_error(resp, &state, &request_id).await
    } else {
        resp
    };

    // Security / caching headers on every response, matching the legacy Python
    // frontend's `after_request` hook so downstream expectations are preserved.
    let headers = resp.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        HeaderName::from_static("x-xss-protection"),
        HeaderValue::from_static("1; mode=block"),
    );

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
        assert!(m.contains(&format!("quote your request ID {RID}.")));
        assert!(m.ends_with("script/notebook if possible."));
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

    #[test]
    fn only_the_first_job_of_a_call_adopts_the_minted_id() {
        let minted = MintedRequestId::new(RID.to_string());
        assert_eq!(new_job(Some(&minted), json!({})).id, RID);
        assert_ne!(new_job(Some(&minted), json!({})).id, RID);
        assert_ne!(new_job(None, json!({})).id, RID);
    }

    #[test]
    fn api_specific_error_contracts_are_preserved() {
        assert!(preserves_native_error_shape("/edr"));
        assert!(preserves_native_error_shape("/edr/collections"));
        assert!(preserves_native_error_shape("/openmeteo"));
        assert!(preserves_native_error_shape("/openmeteo/v1/forecast"));
        assert!(!preserves_native_error_shape("/api/v2/collections"));
    }
}
