use std::sync::Arc;
use std::time::Duration;

use axum::http::request::Parts;
use bits::{Job, JobResult, PollOutcome, SubmitOutcome};
use bytes::BytesMut;
use futures::TryStreamExt;
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::auth::{AuthUser, MockRolesAudit, MockTime, MockTimeAudit};
use crate::config::McpConfig;
use crate::state::AppState;

const MAX_POLL_TIMEOUT_SECS: f64 = 60.0;
const RETRY_AFTER_SECS: u64 = 5;

#[derive(Clone)]
pub struct PolytopeMcp {
    state: Arc<AppState>,
    config: McpConfig,
    tool_router: ToolRouter<Self>,
}

impl PolytopeMcp {
    pub fn new(state: Arc<AppState>, config: McpConfig) -> Self {
        Self {
            state,
            config,
            tool_router: Self::tool_router(),
        }
    }
}

pub fn service(
    state: Arc<AppState>,
    config: McpConfig,
) -> StreamableHttpService<PolytopeMcp, LocalSessionManager> {
    let mut http_config = StreamableHttpServerConfig::default();
    if config.allowed_hosts.is_empty() {
        http_config = http_config.disable_allowed_hosts();
    } else {
        http_config = http_config.with_allowed_hosts(config.allowed_hosts.clone());
    }
    if !config.allowed_origins.is_empty() {
        http_config = http_config.with_allowed_origins(config.allowed_origins.clone());
    }

    StreamableHttpService::new(
        move || Ok(PolytopeMcp::new(state.clone(), config.clone())),
        Arc::new(LocalSessionManager::default()),
        http_config,
    )
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SubmitParams {
    #[schemars(description = "Configured Polytope collection to submit to.")]
    collection: String,
    #[schemars(
        description = "Polytope/MARS request object. Use the configured catalogue for dataset discovery when available."
    )]
    request: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PollParams {
    #[schemars(description = "Opaque Polytope request ID returned by polytope_submit.")]
    request_id: String,
    #[schemars(
        description = "Maximum seconds to wait in this MCP call. The Polytope job continues if this returns pending. Capped at 60 seconds."
    )]
    timeout_secs: Option<f64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CancelParams {
    #[schemars(description = "Opaque Polytope request ID returned by polytope_submit.")]
    request_id: String,
}

#[tool_router]
impl PolytopeMcp {
    #[tool(
        name = "polytope_whoami",
        description = "Return the authenticated Polytope identity visible to MCP tools. Secrets and auth attributes are not returned."
    )]
    async fn whoami(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth_user =
            request_parts(&context).and_then(|parts| parts.extensions.get::<AuthUser>().cloned());

        let value = match auth_user {
            Some(user) => json!({
                "authenticated": true,
                "username": user.username,
                "realm": user.realm,
                "roles": user.roles,
                "scope_names": user.scopes.keys().cloned().collect::<Vec<_>>(),
            }),
            None => json!({
                "authenticated": false,
                "anonymous": true,
            }),
        };
        Ok(CallToolResult::structured(value))
    }

    #[tool(
        name = "polytope_list_collections",
        description = "List configured Polytope collections. If catalogue_url is present, use it for data discovery and request construction guidance."
    )]
    async fn list_collections(&self) -> CallToolResult {
        let mut collections: Vec<String> = self.state.collections.keys().cloned().collect();
        collections.sort();
        CallToolResult::structured(json!({
            "collections": collections,
            "catalogue_url": self.config.catalogue_url,
        }))
    }

    #[tool(
        name = "polytope_submit",
        description = "Submit a Polytope retrieval request asynchronously. Returns an opaque request_id; use polytope_poll to check progress and get the download URL or small inline result."
    )]
    async fn submit(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(params): Parameters<SubmitParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let parts = request_parts(&context).ok_or_else(missing_http_parts_error)?;
        let route_handle = match self.state.collections.get(&params.collection) {
            Some(handle) => handle.clone(),
            None => {
                return Ok(tool_error(json!({
                    "status": "error",
                    "error": format!("unknown collection '{}'", params.collection),
                    "collections": sorted_collections(&self.state),
                    "catalogue_url": self.config.catalogue_url,
                })));
            }
        };

        let mut request = params.request;
        if let Err(message) = super::flatten_request(&mut request) {
            return Ok(tool_error(json!({
                "status": "error",
                "error": message,
            })));
        }

        let mut job = Job::new(request);
        set_mcp_job_context(&mut job, parts, &self.state, &params.collection);
        let submitted_request = job.request.clone();

        let id = match route_handle.submit(job) {
            SubmitOutcome::Accepted(handle) => handle.id,
            SubmitOutcome::Overloaded => {
                return Ok(tool_error(json!({
                    "status": "overloaded",
                    "error": "broker at capacity",
                    "retryable": true,
                    "retry_after_seconds": RETRY_AFTER_SECS,
                })));
            }
        };

        if let Some(user) = parts.extensions.get::<AuthUser>() {
            tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %id, "enduser.id" = %user.username, "enduser.realm" = %user.realm, polytope.request = %polytope_observability::request(&submitted_request), api = "mcp", "mcp job submitted");
        } else {
            tracing::info!("event.name" = "api.job.submitted", outcome = "success", request.id = %id, polytope.request = %polytope_observability::request(&submitted_request), api = "mcp", "mcp job submitted");
        }
        super::audit_mock_job_submission(parts.extensions.get::<MockRolesAudit>(), &id);
        super::audit_mock_time_job_submission(parts.extensions.get::<MockTimeAudit>(), &id);

        Ok(CallToolResult::structured(json!({
            "status": "queued",
            "request_id": id,
            "collection": params.collection,
            "poll_tool": "polytope_poll",
            "cancel_tool": "polytope_cancel",
            "poll_after_seconds": RETRY_AFTER_SECS,
            "catalogue_url": self.config.catalogue_url,
        })))
    }

    #[tool(
        name = "polytope_poll",
        description = "Poll an existing Polytope request. A pending response means the job is still running; call again later. Completed data is returned as a download URL when available, or inline only for small JSON/text results."
    )]
    async fn poll(
        &self,
        Parameters(params): Parameters<PollParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let timeout = params.timeout_secs.map(clamp_poll_timeout);
        let outcome = self.state.bits.poll(&params.request_id, timeout).await;
        Ok(self
            .poll_outcome_to_tool_result(&params.request_id, outcome)
            .await)
    }

    #[tool(
        name = "polytope_cancel",
        description = "Cancel an existing Polytope request by opaque request_id."
    )]
    async fn cancel(&self, Parameters(params): Parameters<CancelParams>) -> CallToolResult {
        let revoked = self.state.bits.cancel(&params.request_id);
        CallToolResult::structured(json!({
            "status": if revoked { "cancelled" } else { "not_found_or_already_complete" },
            "request_id": params.request_id,
            "revoked": revoked,
        }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for PolytopeMcp {
    fn get_info(&self) -> ServerInfo {
        let instructions = match &self.config.catalogue_url {
            Some(url) => format!(
                "Polytope data retrieval MCP. Use {url} for catalogue discovery, then submit requests with polytope_submit and poll with polytope_poll."
            ),
            None => "Polytope data retrieval MCP. Submit requests with polytope_submit and poll with polytope_poll.".to_string(),
        };
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(instructions)
    }
}

impl PolytopeMcp {
    async fn poll_outcome_to_tool_result(
        &self,
        request_id: &str,
        outcome: PollOutcome,
    ) -> CallToolResult {
        match outcome {
            PollOutcome::Pending { id } => CallToolResult::structured(json!({
                "status": "pending",
                "request_id": id,
                "poll_tool": "polytope_poll",
                "poll_after_seconds": RETRY_AFTER_SECS,
            })),
            PollOutcome::NotFound => tool_error(json!({
                "status": "not_found",
                "request_id": request_id,
                "error": "request not found",
            })),
            PollOutcome::JobLost => tool_error(json!({
                "status": "lost",
                "request_id": request_id,
                "error": "request state expired or was lost",
            })),
            PollOutcome::Ready(result) => self.job_result_to_tool_result(request_id, result).await,
        }
    }

    async fn job_result_to_tool_result(
        &self,
        request_id: &str,
        result: JobResult,
    ) -> CallToolResult {
        match result {
            JobResult::Redirect {
                location,
                message,
                content_type,
                content_length,
            } => CallToolResult::structured(json!({
                "status": "ready",
                "request_id": request_id,
                "delivery": "redirect",
                "download_url": location,
                "message": message,
                "content_type": content_type,
                "content_length": content_length,
            })),
            JobResult::Success {
                content_type,
                size,
                stream,
            } => {
                if !is_inline_content_type(&content_type) {
                    return tool_error(json!({
                        "status": "ready_unavailable",
                        "request_id": request_id,
                        "delivery": "direct_stream_omitted",
                        "content_type": content_type,
                        "content_length": known_size(size),
                        "error": "result was a direct non-text stream; MCP does not inline binary data. Configure the route/worker to return a redirect for MCP downloads.",
                    }));
                }
                if let Some(length) = known_size(size)
                    && length > self.config.inline_result_max_bytes as u64
                {
                    return tool_error(json!({
                        "status": "ready_unavailable",
                        "request_id": request_id,
                        "delivery": "direct_stream_too_large",
                        "content_type": content_type,
                        "content_length": length,
                        "inline_result_max_bytes": self.config.inline_result_max_bytes,
                        "error": "result exceeded the MCP inline result limit. Configure the route/worker to return a redirect for MCP downloads.",
                    }));
                }

                match collect_inline(stream, self.config.inline_result_max_bytes).await {
                    Ok(Some(bytes)) => {
                        let content = parse_inline_content(&content_type, &bytes);
                        CallToolResult::structured(json!({
                            "status": "ready",
                            "request_id": request_id,
                            "delivery": "inline",
                            "content_type": content_type,
                            "content_length": bytes.len(),
                            "content": content,
                        }))
                    }
                    Ok(None) => tool_error(json!({
                        "status": "ready_unavailable",
                        "request_id": request_id,
                        "delivery": "direct_stream_too_large",
                        "content_type": content_type,
                        "inline_result_max_bytes": self.config.inline_result_max_bytes,
                        "error": "result exceeded the MCP inline result limit. Configure the route/worker to return a redirect for MCP downloads.",
                    })),
                    Err(error) => tool_error(json!({
                        "status": "error",
                        "request_id": request_id,
                        "error": error,
                    })),
                }
            }
            JobResult::Error { message } => tool_error(json!({
                "status": "failed",
                "request_id": request_id,
                "message": message,
            })),
            JobResult::Failed { reason } => tool_error(json!({
                "status": "failed",
                "request_id": request_id,
                "message": reason,
            })),
            JobResult::Overloaded { reason } => tool_error(json!({
                "status": "overloaded",
                "request_id": request_id,
                "message": reason,
                "retryable": true,
                "retry_after_seconds": RETRY_AFTER_SECS,
            })),
            JobResult::Cancelled => CallToolResult::structured(json!({
                "status": "cancelled",
                "request_id": request_id,
            })),
            JobResult::ClientGone => tool_error(json!({
                "status": "failed",
                "request_id": request_id,
                "message": "request abandoned: client disconnected",
            })),
        }
    }
}

fn request_parts(context: &RequestContext<RoleServer>) -> Option<&Parts> {
    context.extensions.get::<Parts>()
}

fn missing_http_parts_error() -> ErrorData {
    ErrorData::internal_error("MCP HTTP request metadata was not available", None)
}

fn set_mcp_job_context(job: &mut Job, parts: &Parts, state: &AppState, collection: &str) {
    super::set_job_user_context(
        job,
        &parts.headers,
        parts.extensions.get::<AuthUser>(),
        parts.extensions.get::<MockRolesAudit>(),
        &state.admin_bypass_roles,
    );
    job.metadata_mut()["api"] = json!("mcp");
    job.metadata_mut()["collection"] = json!(collection);
    // MCP clients should receive download URLs or small inline JSON/text, not
    // arbitrary GRIB/NetCDF streams in the model context.
    job.metadata_mut()["buffer_full_output"] = json!(true);
    super::set_job_mock_time_metadata(job, parts.extensions.get::<MockTime>());
}

fn sorted_collections(state: &AppState) -> Vec<String> {
    let mut collections: Vec<String> = state.collections.keys().cloned().collect();
    collections.sort();
    collections
}

fn clamp_poll_timeout(timeout_secs: f64) -> Duration {
    let safe = if timeout_secs.is_finite() {
        timeout_secs.clamp(0.0, MAX_POLL_TIMEOUT_SECS)
    } else {
        0.0
    };
    Duration::from_secs_f64(safe)
}

fn known_size(size: i64) -> Option<u64> {
    u64::try_from(size).ok()
}

fn is_inline_content_type(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.starts_with("text/")
        || lower.contains("json")
        || lower.ends_with("+json")
        || lower.starts_with("application/xml")
        || lower.starts_with("application/yaml")
}

async fn collect_inline(
    mut stream: Box<
        dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin,
    >,
    limit: usize,
) -> Result<Option<Vec<u8>>, String> {
    let mut buf = BytesMut::new();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|err| format!("failed to read direct result stream: {err}"))?
    {
        if buf.len().saturating_add(chunk.len()) > limit {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Some(buf.to_vec()))
}

fn parse_inline_content(content_type: &str, bytes: &[u8]) -> Value {
    if content_type.to_ascii_lowercase().contains("json")
        && let Ok(value) = serde_json::from_slice(bytes)
    {
        return value;
    }
    json!(String::from_utf8_lossy(bytes).to_string())
}

fn tool_error(value: Value) -> CallToolResult {
    CallToolResult::structured_error(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_timeout_is_per_call_and_capped() {
        assert_eq!(clamp_poll_timeout(-5.0), Duration::from_secs(0));
        assert_eq!(clamp_poll_timeout(f64::NAN), Duration::from_secs(0));
        assert_eq!(clamp_poll_timeout(120.0), Duration::from_secs(60));
    }

    #[test]
    fn inline_content_type_accepts_text_and_json_only() {
        assert!(is_inline_content_type("text/plain"));
        assert!(is_inline_content_type("application/json"));
        assert!(is_inline_content_type("application/geo+json"));
        assert!(!is_inline_content_type("application/x-grib"));
        assert!(!is_inline_content_type("application/x-netcdf"));
    }
}
