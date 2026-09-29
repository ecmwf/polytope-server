// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Catalogue qube acquisition for `/chunks/v1/{collection}/metadata`
//! (Contract v2 §V2.2(2)).
//!
//! The full-collection qube (arena JSON, ~5 MB) is fetched over HTTP from
//! `chunks.catalogue_url`, cached in memory behind the app state, and
//! ETag/TTL-refreshed lazily on access:
//!
//!   * fresh cache (age < TTL)              -> served directly, no network;
//!   * stale cache, refresh succeeds        -> replaced (or revalidated on 304);
//!   * stale cache, refresh fails           -> stale qube is served;
//!   * no cache ever AND refresh fails      -> `503`.
//!
//! The fetch runs on the async runtime; JSON parsing (potentially several MB)
//! is moved off the hot path onto a blocking thread. The [`CatalogueSource`]
//! trait lets tests inject a qube without any HTTP.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use super::qube::Qube;

/// Outcome of a single upstream fetch attempt.
pub enum Fetched {
    /// `200` — new body (with its `ETag`, if any).
    Modified {
        body: Vec<u8>,
        etag: Option<String>,
    },
    /// `304 Not Modified` — the cached body is still current.
    NotModified,
}

/// Something that can fetch the catalogue arena JSON. Injected so tests never
/// touch the network.
#[async_trait::async_trait]
pub trait CatalogueSource: Send + Sync {
    /// Fetch the qube, passing the currently cached `ETag` (if any) so the
    /// upstream can answer `304`.
    async fn fetch(&self, etag: Option<String>) -> Result<Fetched, String>;
    /// The source URL, echoed into the metadata response.
    fn source_url(&self) -> &str;
}

/// Failure modes surfaced to the metadata handler.
#[derive(Debug)]
pub enum CatalogueError {
    /// Refresh failed and no qube has ever been loaded -> `503`.
    Unavailable(String),
}

/// A qube handed back to the handler, with provenance for the response.
#[derive(Clone)]
pub struct QubeHandle {
    pub qube: Arc<Qube>,
    pub etag: Option<String>,
    pub source: String,
}

struct Cached {
    qube: Arc<Qube>,
    etag: Option<String>,
    fetched_at: Instant,
}

/// In-memory, ETag/TTL-refreshed catalogue cache.
pub struct CatalogueCache {
    source: Arc<dyn CatalogueSource>,
    ttl: Duration,
    state: Mutex<Option<Cached>>,
    strip_keys: BTreeSet<String>,
}

impl CatalogueCache {
    pub fn new(
        source: Arc<dyn CatalogueSource>,
        ttl: Duration,
        strip_keys: BTreeSet<String>,
    ) -> Self {
        Self {
            source,
            ttl,
            state: Mutex::new(None),
            strip_keys,
        }
    }

    /// Return the current qube, refreshing lazily when stale. Serves a stale
    /// qube if a refresh fails; only errors when nothing has ever been loaded.
    pub async fn get(&self) -> Result<QubeHandle, CatalogueError> {
        let mut guard = self.state.lock().await;

        let stale = match guard.as_ref() {
            Some(c) => c.fetched_at.elapsed() >= self.ttl,
            None => true,
        };

        if stale {
            let etag = guard.as_ref().and_then(|c| c.etag.clone());
            match self.source.fetch(etag).await {
                Ok(Fetched::NotModified) => {
                    if let Some(c) = guard.as_mut() {
                        c.fetched_at = Instant::now();
                    }
                }
                Ok(Fetched::Modified { body, etag }) => {
                    // Parse off the hot path (the body may be several MB).
                    let strip_keys = self.strip_keys.clone();
                    let parsed = tokio::task::spawn_blocking(move || {
                        Qube::from_arena_json_bytes(&body)
                            .map(|qube| qube.strip_dimensions(&strip_keys))
                    })
                    .await;
                    match parsed {
                        Ok(Ok(qube)) => {
                            *guard = Some(Cached {
                                qube: Arc::new(qube),
                                etag,
                                fetched_at: Instant::now(),
                            });
                        }
                        Ok(Err(parse_err)) => {
                            // Bad body: keep serving stale if we have it.
                            if guard.is_none() {
                                return Err(CatalogueError::Unavailable(format!(
                                    "catalogue parse failed: {parse_err}"
                                )));
                            }
                            tracing::warn!(
                                error = %parse_err,
                                "catalogue refresh parse failed; serving stale qube"
                            );
                        }
                        Err(join_err) => {
                            if guard.is_none() {
                                return Err(CatalogueError::Unavailable(format!(
                                    "catalogue parse task failed: {join_err}"
                                )));
                            }
                            tracing::warn!(
                                error = %join_err,
                                "catalogue parse task failed; serving stale qube"
                            );
                        }
                    }
                }
                Err(fetch_err) => {
                    if guard.is_none() {
                        return Err(CatalogueError::Unavailable(fetch_err));
                    }
                    tracing::warn!(
                        error = %fetch_err,
                        "catalogue refresh failed; serving stale qube"
                    );
                }
            }
        }

        let cached = guard
            .as_ref()
            .expect("cache is populated after a successful/stale-served refresh");
        Ok(QubeHandle {
            qube: cached.qube.clone(),
            etag: cached.etag.clone(),
            source: self.source.source_url().to_string(),
        })
    }
}

/// Production [`CatalogueSource`]: a plain conditional HTTP GET via reqwest.
pub struct HttpCatalogueSource {
    url: String,
    client: reqwest::Client,
}

impl HttpCatalogueSource {
    pub fn new(url: String) -> Self {
        let client = reqwest::Client::builder()
            // Cross-site catalogue responses are several MiB and can be slow over
            // the inter-site route; keep the bounded fetch but allow enough time.
            .timeout(Duration::from_secs(120))
            .build()
            .unwrap_or_default();
        Self { url, client }
    }
}

#[async_trait::async_trait]
impl CatalogueSource for HttpCatalogueSource {
    async fn fetch(&self, etag: Option<String>) -> Result<Fetched, String> {
        let mut req = self.client.get(&self.url);
        if let Some(tag) = &etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, tag.clone());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("catalogue GET {} failed: {e}", self.url))?;
        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(Fetched::NotModified);
        }
        if !resp.status().is_success() {
            return Err(format!(
                "catalogue GET {} returned HTTP {}",
                self.url,
                resp.status()
            ));
        }
        let etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("catalogue body read failed: {e}"))?
            .to_vec();
        Ok(Fetched::Modified { body, etag })
    }

    fn source_url(&self) -> &str {
        &self.url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn http_source_preserves_catalogue_query_string() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v2/")
            .match_query(mockito::Matcher::UrlEncoded(
                "location".to_string(),
                "mn5".to_string(),
            ))
            .with_status(200)
            .with_body(br#"{"qube":[]}"#)
            .create_async()
            .await;
        let source = HttpCatalogueSource::new(format!(
            "{}/api/v2/?location=mn5",
            server.url()
        ));

        let fetched = source.fetch(None).await.unwrap();
        assert!(matches!(fetched, Fetched::Modified { .. }));
        mock.assert_async().await;
    }
}
