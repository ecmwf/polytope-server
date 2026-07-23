// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::auth::AuthClient;
use crate::config::SupportConfig;

/// How long a completed redirect is retained in the frontend cache.
///
/// Set to match the BOBS production `read_idle_ttl_secs` (600 s) as a
/// reasonable ceiling.  Note that BOBS resets its idle timer on each read
/// while `cached_at` is fixed at first delivery, so the two clocks do not
/// track each other exactly: a URL served near the TTL boundary may point to
/// data that BOBS has already expired, and BOBS may still hold data after we
/// evict the entry.  Both cases result in a client-visible 404 on the BOBS
/// download, which clients already tolerate.
pub const COMPLETED_REDIRECT_TTL: Duration = Duration::from_secs(600);

/// Maximum number of completed-redirect entries retained at any time.
/// Entries are evicted by TTL on every insert, so this cap is a last-resort
/// guard against burst traffic filling memory before the next insert sweeps.
pub const MAX_COMPLETED_REDIRECTS: usize = 10_000;

/// Metadata cached in the frontend after the first delivery of a completed
/// redirect result.  Allows v1 clients to re-poll for the same 303 response
/// within the BOBS idle-TTL window, matching the behaviour of the legacy
/// Python frontend.
pub struct CachedRedirect {
    pub username: String,
    pub realm: String,
    pub location: String,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub cached_at: Instant,
}

pub struct AppState {
    pub bits: bits::Bits,
    pub auth_client: Option<AuthClient>,
    pub collections: HashMap<String, bits::RouteHandle>,
    pub allow_anonymous: bool,
    pub admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
    pub support: SupportConfig,
    /// Short-lived cache of completed redirect results keyed by job ID.
    /// Populated by `v1::get_request` on first delivery; entries expire after
    /// `COMPLETED_REDIRECT_TTL` (set to match BOBS `read_idle_ttl_secs`).
    /// The `Mutex` is never held across an `.await` point.
    pub completed_redirects: Mutex<HashMap<String, CachedRedirect>>,
}
