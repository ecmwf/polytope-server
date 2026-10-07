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

pub mod catalogue;
pub mod expand;
pub mod extract;
pub mod feature;
pub mod metadata;
pub mod qube;
pub mod tree;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use bits::{Job, JobResult, PollOutcome, SubmitOutcome};
use futures::StreamExt;
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
    /// Catalogue qube cache. `None` when `chunks.catalogue_url` is unset,
    /// in which case `/metadata` returns `501`.
    pub catalogue: Option<Arc<catalogue::CatalogueCache>>,
    pub feature_cache: feature::FeatureCache,
}

/// Build the `/chunks/v1` router (routes relative to the mount point).
/// Authentication is applied by the caller, exactly as for v2.
pub fn router<S>(
    app: Arc<AppState>,
    config: ChunksConfig,
    expander: Arc<dyn RequestExpander>,
    catalogue: Option<Arc<catalogue::CatalogueCache>>,
) -> Router<S> {
    let feature_cache = feature::FeatureCache::new(
        Duration::from_secs(config.feature_cache_ttl_secs),
        config.feature_cache_capacity,
    );
    let state = Arc::new(ChunksState {
        app,
        config,
        expander,
        catalogue,
        feature_cache,
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

async fn resolve_feature_job(
    state: &Arc<ChunksState>,
    submission: super::v2::Submission<'_>,
    body: Value,
) -> Result<feature::ResolvedFeature, String> {
    let mut job = Job::new(body);
    super::set_job_user_context(
        &mut job,
        submission.headers,
        submission.auth_user,
        submission.mock_audit,
        &state.app.admin_bypass_roles,
    );
    job.metadata_mut()["collection"] = json!(submission.collection);
    job.metadata_mut()["api"] = json!("chunks-feature-resolve");
    super::set_job_mock_time_metadata(&mut job, submission.mock_time.mock_time.as_ref());
    let id = match submission.route_handle.submit(job) {
        SubmitOutcome::Accepted(handle) => handle.id,
        SubmitOutcome::Overloaded => {
            return Err("feature resolution broker is at capacity".to_string());
        }
    };
    super::audit_mock_job_submission(submission.mock_audit, &id);
    super::audit_mock_time_job_submission(submission.mock_time.mock_time_audit.as_ref(), &id);

    let timeout = Duration::from_secs(state.config.feature_resolve_timeout_secs);
    let deadline = Instant::now() + timeout;
    let result = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            state.app.bits.cancel(&id);
            return Err(format!(
                "feature resolution timed out after {} seconds",
                timeout.as_secs()
            ));
        }
        match state.app.bits.poll(&id, Some(remaining)).await {
            PollOutcome::Pending { .. } => continue,
            PollOutcome::Ready(result) => break result,
            PollOutcome::NotFound => return Err("feature resolution job disappeared".to_string()),
            PollOutcome::JobLost => return Err("feature resolution job state was lost".to_string()),
        }
    };

    let bytes = match result {
        JobResult::Success { mut stream, .. } => {
            let mut output = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|error| format!("feature result stream failed: {error}"))?;
                output.extend_from_slice(&chunk);
            }
            output
        }
        JobResult::Redirect { location, .. } => {
            let response = reqwest::Client::builder()
                .timeout(deadline.saturating_duration_since(Instant::now()))
                .build()
                .map_err(|error| format!("cannot build feature result client: {error}"))?
                .get(&location)
                .send()
                .await
                .map_err(|error| format!("feature result download failed: {error}"))?;
            if !response.status().is_success() {
                return Err(format!(
                    "feature result download returned HTTP {}",
                    response.status()
                ));
            }
            response
                .bytes()
                .await
                .map_err(|error| format!("feature result body failed: {error}"))?
                .to_vec()
        }
        JobResult::Error { message } => return Err(message),
        JobResult::Failed { reason }
        | JobResult::Overloaded { reason }
        | JobResult::RateLimited { reason } => return Err(reason),
        JobResult::ClientGone => return Err("feature resolution client disconnected".to_string()),
        JobResult::Cancelled => return Err("feature resolution job was cancelled".to_string()),
    };
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("feature worker returned invalid JSON: {error}"))
}

pub async fn metadata_handler(
    State(state): State<Arc<ChunksState>>,
    headers: HeaderMap,
    auth_user: Option<Extension<AuthUser>>,
    mock_audit: Option<Extension<MockRolesAudit>>,
    mock_time_extensions: super::MockTimeSubmissionExtensions,
    Path(collection): Path<String>,
    body: Bytes,
) -> Response {
    const EP: &str = "metadata";
    if !state.app.collections.contains_key(&collection) {
        return bad_request(EP, format!("unknown collection '{collection}'"));
    }
    let metadata::MetadataBody {
        request,
        gaps,
        feature,
    } = match parse_json(&body).and_then(|b| metadata::parse_metadata_body(&b)) {
        Ok(parsed) => parsed,
        Err(msg) => return bad_request(EP, msg),
    };

    // The keys the USER actually supplied (before metkit fills unsupplied keys
    // with defaults). Only these may constrain the catalogue-qube intersection.
    let user_keys: std::collections::BTreeSet<String> = request.keys().cloned().collect();

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

    // Contract v2: derive the structure tree from the catalogue qube.
    let Some(catalogue) = state.catalogue.as_ref() else {
        return error_response(
            StatusCode::NOT_IMPLEMENTED,
            "catalogue is not configured (chunks.catalogue_url); /metadata is unavailable in \
             this deployment",
        );
    };
    let handle = match catalogue.get().await {
        Ok(handle) => handle,
        Err(catalogue::CatalogueError::Unavailable(msg)) => {
            tracing::error!("event.name" = "api.chunks.failed", endpoint = EP, error = %msg, "catalogue unavailable");
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("catalogue unavailable: {msg}"),
            );
        }
    };

    let mut md = match metadata::build_metadata_v2(
        &state.config,
        &collection,
        &canonical,
        &user_keys,
        &handle,
        gaps,
    ) {
        Ok(md) => md,
        Err(msg) => return bad_request(EP, msg),
    };
    if let Some(feature) = feature.as_ref() {
        let targets =
            match feature::resolve_targets(&md.tree, feature, state.config.max_feature_points) {
                Ok(targets) => targets,
                Err(msg) => return bad_request(EP, msg),
            };
        let route_handle = state.app.collections[&collection].clone();
        let mut resolved = HashMap::new();
        for target in targets {
            let value = if let Some(cached) = state.feature_cache.get(&target.cache_key) {
                cached
            } else {
                let submission = super::v2::Submission {
                    collection: &collection,
                    route_handle: route_handle.clone(),
                    headers: &headers,
                    auth_user: auth_user.as_ref().map(|Extension(user)| user),
                    mock_audit: mock_audit.as_ref().map(|Extension(audit)| audit),
                    mock_time: &mock_time_extensions,
                    api: Some("chunks"),
                };
                let value = match resolve_feature_job(&state, submission, target.job).await {
                    Ok(value) => value,
                    Err(msg) => {
                        return bad_request(EP, format!("feature resolution failed: {msg}"));
                    }
                };
                if let Err(msg) = feature::validate_resolved(
                    feature,
                    &value,
                    target.count_values,
                    state.config.max_feature_points,
                ) {
                    return bad_request(EP, msg);
                }
                state
                    .feature_cache
                    .insert(target.cache_key.clone(), value.clone());
                value
            };
            resolved.insert(target.cache_key, value);
        }
        if let Err(msg) = feature::attach_resolved(&mut md.tree, feature, &resolved) {
            return bad_request(EP, msg);
        }
        metadata::refresh_default_chunking(&mut md, &state.config);
    }
    tracing::info!("event.name" = "api.chunks.metadata", outcome = "success", collection = %collection, version = md.version as u64, "chunks metadata served");
    (StatusCode::OK, Json(md)).into_response()
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
    let job_body = match parse_json(&body).and_then(|body| {
        extract::build_extract_request(
            &body,
            state.config.max_chunk_cost,
            state.config.max_multi_chunks,
        )
    }) {
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
