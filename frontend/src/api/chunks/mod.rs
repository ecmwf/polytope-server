// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! `/chunks/v1` — zarr-agnostic chunk API (polytope-zarr).
//!
//! Normative wire contract: `polytope-zarr-contract.md` (v0).
//!
//! - `POST /chunks/v1/{collection}/metadata` — synchronous: metkit-canonicalise
//!   the request, match the grid registry (`chunks.grids` config) and describe
//!   the datacube (axes, variables, grid, default chunking).
//! - `POST /chunks/v1/{collection}/extract` — queued: validated and submitted
//!   through the exact v2 submission path ([`super::v2::submit_and_poll`]);
//!   responses and polling (`/api/v2/requests/{id}`) are v2's.
//!
//! Errors are `{"error": ...}` JSON (the support middleware preserves the
//! native shape for `/chunks/` paths).

pub mod expand;
pub mod extract;
pub mod metadata;

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};

use crate::auth::{AuthUser, MockRolesAudit};
use crate::config::ChunksConfig;
use crate::state::AppState;
use expand::{ExpandError, RequestExpander};

/// State for the `/chunks/v1` router: the shared app state plus the chunk
/// config and the request expander.
pub struct ChunksState {
    pub app: Arc<AppState>,
    pub config: ChunksConfig,
    pub expander: Arc<dyn RequestExpander>,
}

/// Build the `/chunks/v1` router (routes relative to the mount point).
/// Authentication is applied by the caller, exactly as for v2.
pub fn router<S>(
    app: Arc<AppState>,
    config: ChunksConfig,
    expander: Arc<dyn RequestExpander>,
) -> Router<S> {
    let state = Arc::new(ChunksState {
        app,
        config,
        expander,
    });
    Router::new()
        .route("/{collection}/metadata", post(metadata_handler))
        .route("/{collection}/extract", post(extract_handler))
        .with_state(state)
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error": message.into()}))).into_response()
}

fn bad_request(endpoint: &'static str, message: impl Into<String>) -> Response {
    let message = message.into();
    tracing::warn!("event.name" = "api.chunks.rejected", outcome = "rejected", endpoint, error = %message, "chunks request rejected");
    error_response(StatusCode::BAD_REQUEST, message)
}

fn parse_json(body: &Bytes) -> Result<Value, String> {
    serde_json::from_slice(body).map_err(|e| format!("request body is not valid JSON: {e}"))
}

pub async fn metadata_handler(
    State(state): State<Arc<ChunksState>>,
    Path(collection): Path<String>,
    body: Bytes,
) -> Response {
    const EP: &str = "metadata";
    if !state.app.collections.contains_key(&collection) {
        return bad_request(EP, format!("unknown collection '{collection}'"));
    }
    let request = match parse_json(&body).and_then(|b| metadata::parse_metadata_body(&b)) {
        Ok(request) => request,
        Err(msg) => return bad_request(EP, msg),
    };

    // metkit is a blocking C++ call; keep it off the async workers.
    let expander = state.expander.clone();
    let expanded = match tokio::task::spawn_blocking(move || expander.expand(&request)).await {
        Ok(result) => result,
        Err(err) => {
            tracing::error!("event.name" = "api.chunks.failed", endpoint = EP, error = %err, "request expansion task failed");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "request expansion failed unexpectedly",
            );
        }
    };
    let entries = match expanded {
        Ok(entries) => entries,
        Err(ExpandError::Invalid(msg)) => return bad_request(EP, msg),
        Err(ExpandError::Unavailable(msg)) => {
            return error_response(StatusCode::NOT_IMPLEMENTED, msg);
        }
    };
    let canonical = match expand::canonicalise(entries) {
        Ok(canonical) => canonical,
        Err(msg) => return bad_request(EP, msg),
    };
    match metadata::build_metadata(&state.config, &collection, &canonical) {
        Ok(md) => {
            tracing::info!("event.name" = "api.chunks.metadata", outcome = "success", collection = %collection, axes = md.axes.len() as u64, variables = md.variables.len() as u64, "chunks metadata served");
            (StatusCode::OK, Json(md)).into_response()
        }
        Err(msg) => bad_request(EP, msg),
    }
}

pub async fn extract_handler(
    State(state): State<Arc<ChunksState>>,
    headers: HeaderMap,
    auth_user: Option<Extension<AuthUser>>,
    mock_audit: Option<Extension<MockRolesAudit>>,
    mock_time_extensions: super::MockTimeSubmissionExtensions,
    Path(collection): Path<String>,
    body: Bytes,
) -> Response {
    const EP: &str = "extract";
    let Some(route_handle) = state.app.collections.get(&collection).cloned() else {
        return bad_request(EP, format!("unknown collection '{collection}'"));
    };
    let job_body = match parse_json(&body)
        .and_then(|b| extract::build_extract_job(&b, state.config.max_chunk_cost))
    {
        Ok(job_body) => job_body,
        Err(msg) => return bad_request(EP, msg),
    };

    super::v2::submit_and_poll(
        &state.app,
        super::v2::Submission {
            collection: &collection,
            route_handle,
            headers: &headers,
            auth_user: auth_user.as_ref().map(|Extension(user)| user),
            mock_audit: mock_audit.as_ref().map(|Extension(audit)| audit),
            mock_time: &mock_time_extensions,
            api: Some("chunks"),
        },
        job_body,
    )
    .await
}

#[cfg(test)]
mod tests;
