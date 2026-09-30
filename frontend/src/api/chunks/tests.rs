// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end `/chunks/v1` tests through the full `build_app` stack (auth,
//! support middleware, bits routing) with a fake MARS expander and an injected
//! catalogue qube (no network).

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::post,
};
use http_body_util::BodyExt;
use serde_json::{Map, Value, json};
use tower::ServiceExt;

use super::catalogue::{CatalogueSource, Fetched};
use super::expand::{ExpandError, RequestExpander, UnavailableExpander};
use crate::config::ServerConfig;
use crate::state::AppState;

/// Deterministic stand-in for metkit: canonicalises a few keys the way
/// metkit does (param shortname -> id, time -> HHMM, expver -> 4 chars),
/// fails on the value "bogus", and returns keys in reverse alphabetical
/// order (to prove the handler imposes canonical order itself).
struct FakeExpander;

fn tokens(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
            .collect(),
        Value::String(s) => s.split('/').map(str::to_string).collect(),
        other => vec![other.to_string()],
    }
}

impl RequestExpander for FakeExpander {
    fn expand(
        &self,
        request: &Map<String, Value>,
    ) -> Result<Vec<(String, Vec<String>)>, ExpandError> {
        let mut out = Vec::new();
        for (key, value) in request {
            let mut values = Vec::new();
            for token in tokens(value) {
                if token == "bogus" {
                    return Err(ExpandError::Invalid(format!(
                        "request expansion failed: invalid value '{token}' for '{key}'"
                    )));
                }
                let canonical = match key.as_str() {
                    "param" => match token.as_str() {
                        "2t" => "167".to_string(),
                        "10u" => "165".to_string(),
                        other => other.to_string(),
                    },
                    "time" => {
                        let n: u32 = token.parse().unwrap_or(0);
                        format!("{:04}", if n < 100 { n * 100 } else { n })
                    }
                    "expver" => format!("{token:0>4}"),
                    _ => token,
                };
                values.push(canonical);
            }
            out.push((key.clone(), values));
        }
        out.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(out)
    }
}

const COLLECTION: &str = "destination-earth";
const MD5: &str = "f78d9d2d6f6f1b4c8f0b3a6d1b1e2c3d";
const CATALOGUE_URL: &str = "https://catalogue.test/api/v2/";

fn server_config(target_url: &str, extra: &str) -> ServerConfig {
    let yaml = format!(
        r#"
polytope:
  site: bol
  env: tst
bits:
  targets:
    worker:
      type: http
      url: "{target_url}"
  collections:
    {COLLECTION}:
      - extract_route:
          - target::worker
{extra}
"#
    );
    serde_yaml::from_str(&yaml).expect("test config parses")
}

fn chunks_section() -> &'static str {
    r#"
chunks:
  max_chunk_cost: 30000000
  grids:
    - collection: destination-earth
      match: { class: d1, dataset: climate-dt, resolution: high }
      count_values: 12582912
      nside: 1024
      md5_grid_section: "f78d9d2d6f6f1b4c8f0b3a6d1b1e2c3d"
    - collection: destination-earth
      match: { class: d1, dataset: climate-dt, resolution: standard, generation: 2 }
      count_values: 196608
"#
}

fn app_with(
    target_url: &str,
    extra: &str,
    expander: Arc<dyn RequestExpander>,
) -> (Router, Arc<AppState>) {
    crate::build_app_with_expander(server_config(target_url, extra), expander).expect("app builds")
}

fn app() -> Router {
    app_with(
        "http://127.0.0.1:1/",
        chunks_section(),
        Arc::new(FakeExpander),
    )
    .0
}

// ---------------------------------------------------------------------------
// Catalogue injection (no network)
// ---------------------------------------------------------------------------

/// A [`CatalogueSource`] that serves a fixed arena-JSON qube.
struct FixtureCatalogue {
    arena: Value,
}

#[async_trait::async_trait]
impl CatalogueSource for FixtureCatalogue {
    async fn fetch(&self, _etag: Option<String>) -> Result<Fetched, String> {
        Ok(Fetched::Modified {
            body: serde_json::to_vec(&self.arena).unwrap(),
            etag: Some("\"fixture-etag\"".to_string()),
        })
    }
    fn source_url(&self) -> &str {
        CATALOGUE_URL
    }
}

/// A [`CatalogueSource`] whose fetch always fails.
struct FailingCatalogue;

#[async_trait::async_trait]
impl CatalogueSource for FailingCatalogue {
    async fn fetch(&self, _etag: Option<String>) -> Result<Fetched, String> {
        Err("connection refused".to_string())
    }
    fn source_url(&self) -> &str {
        CATALOGUE_URL
    }
}

fn app_with_catalogue(source: Option<Arc<dyn CatalogueSource>>) -> (Router, Arc<AppState>) {
    crate::build_app_with_catalogue(
        server_config("http://127.0.0.1:1/", chunks_section()),
        Arc::new(FakeExpander),
        source,
    )
    .expect("app builds")
}

fn app_with_qube(arena: Value) -> Router {
    app_with_catalogue(Some(Arc::new(FixtureCatalogue { arena }))).0
}

fn app_with_qube_and_config(arena: Value, chunks_config: &str) -> Router {
    crate::build_app_with_catalogue(
        server_config("http://127.0.0.1:1/", chunks_config),
        Arc::new(FakeExpander),
        Some(Arc::new(FixtureCatalogue { arena })),
    )
    .expect("app builds")
    .0
}

fn nside_one_chunks_section(max_feature_points: u64) -> String {
    format!(
        r#"
chunks:
  max_chunk_cost: 30000000
  max_feature_points: {max_feature_points}
  grids:
    - collection: destination-earth
      match: {{ class: d1, dataset: climate-dt, resolution: high }}
      count_values: 12
      nside: 1
      md5_grid_section: "{MD5}"
"#
    )
}

fn split_feature_chunks_section() -> &'static str {
    r#"
chunks:
  max_chunk_cost: 30000000
  max_feature_points: 100
  grids:
    - collection: destination-earth
      match: { class: d1, dataset: climate-dt, resolution: high }
      count_values: 12
      nside: 1
      md5_grid_section: "f78d9d2d6f6f1b4c8f0b3a6d1b1e2c3d"
    - collection: destination-earth
      match: { class: d1, dataset: climate-dt, resolution: standard, generation: 2 }
      count_values: 48
      nside: 2
"#
}

// ---------------------------------------------------------------------------
// Arena-JSON fixtures (crafted to reproduce real-world cases)
// ---------------------------------------------------------------------------

/// (a) One dense cube: date x time, two params.
fn arena_simple() -> Value {
    json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1]},
        {"dim": "date", "coords": {"strings": ["20200101", "20200102"]}, "parent": 0, "children": [2]},
        {"dim": "time", "coords": {"ints": [0, 1200]}, "parent": 1, "children": [3]},
        {"dim": "param", "coords": {"strings": ["165", "167"]}, "parent": 2, "children": []},
    ]})
}

/// (b) climate-dt sfc heterogeneity: 3 "common" params at 3 times, 2 "special"
/// params only at time 0000 (a faithful, pasteable down-scaling of the real
/// 34 params x 24 times u 2 params x 1 time case). Stored param-major to prove
/// the derivation re-factors deterministically in canonical (time-major) order.
fn arena_heterogeneous() -> Value {
    json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1, 3]},
        {"dim": "param", "coords": {"strings": ["167", "168", "169"]}, "parent": 0, "children": [2]},
        {"dim": "time", "coords": {"ints": [0, 600, 1200]}, "parent": 1, "children": []},
        {"dim": "param", "coords": {"strings": ["228", "229"]}, "parent": 0, "children": [4]},
        {"dim": "time", "coords": {"ints": [0]}, "parent": 3, "children": []},
    ]})
}

/// (c) A single param over a non-contiguous date set (holes at 0102/0104).
fn arena_date_holes() -> Value {
    json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1]},
        {"dim": "date", "coords": {"strings": ["20200101", "20200103", "20200105"]}, "parent": 0, "children": [2]},
        {"dim": "param", "coords": {"strings": ["167"]}, "parent": 1, "children": []},
    ]})
}

/// (d) Two cubes hitting different grid-registry entries via `resolution`.
fn arena_two_grids() -> Value {
    json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1, 3]},
        {"dim": "resolution", "coords": {"strings": ["high"]}, "parent": 0, "children": [2]},
        {"dim": "param", "coords": {"strings": ["167"]}, "parent": 1, "children": []},
        {"dim": "resolution", "coords": {"strings": ["standard"]}, "parent": 0, "children": [4]},
        {"dim": "param", "coords": {"strings": ["167"]}, "parent": 3, "children": []},
    ]})
}

/// A single dense cube spanning resolutions, as emitted when the catalogue
/// structure is identical for every resolution.
fn arena_unsplit_grids(resolutions: &[&str]) -> Value {
    json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1]},
        {"dim": "resolution", "coords": {"strings": resolutions}, "parent": 0, "children": [2]},
        {"dim": "param", "coords": {"strings": ["167"]}, "parent": 1, "children": []},
    ]})
}

fn climate_dt_request() -> Value {
    json!({
        "class": "d1",
        "dataset": "climate-dt",
        "activity": "scenariomip",
        "experiment": "ssp3-7.0",
        "generation": "1",
        "model": "ifs-nemo",
        "realization": "1",
        "resolution": "high",
        "expver": "1",
        "stream": "clte",
        "type": "fc",
        "levtype": "sfc",
        "date": ["20200101", "20200102"],
        "time": ["0", "1200"],
        "param": ["2t", "10u"],
    })
}

async fn post_json(
    app: Router,
    uri: &str,
    body: &Value,
) -> (StatusCode, axum::http::HeaderMap, bytes::Bytes) {
    post_raw(app, uri, serde_json::to_vec(body).unwrap()).await
}

async fn post_raw(
    app: Router,
    uri: &str,
    body: Vec<u8>,
) -> (StatusCode, axum::http::HeaderMap, bytes::Bytes) {
    let resp = app
        .oneshot(
            Request::post(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, bytes)
}

// ---------------------------------------------------------------------------
// metadata (Contract v2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_single_cube_is_root_array_set() {
    let (status, headers, body) = post_json(
        app_with_qube(arena_simple()),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": climate_dt_request()}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    let v: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(v["version"], 2);
    // Single cube -> the tree root is the array_set itself (anonymous name).
    let tree = &v["tree"];
    assert_eq!(tree["type"], "array_set");
    assert_eq!(tree["name"], "");
    assert_eq!(
        tree["axes"],
        json!([
            {"dim": "date", "key": "date", "values": ["20200101", "20200102"]},
            {"dim": "time", "key": "time", "values": ["0000", "1200"]},
        ])
    );
    // param -> variables, sorted ascending as canonical strings.
    assert_eq!(
        tree["variables"],
        json!([{"name": "165", "param": "165"}, {"name": "167", "param": "167"}])
    );
    assert_eq!(
        tree["grid"],
        json!({
            "kind": "healpix", "ordering": "nested", "count_values": 12582912, "nside": 1024,
            "md5GridSection": MD5
        })
    );
    assert_eq!(tree["fill_on_missing"], false);
    assert_eq!(
        tree["extract"],
        json!({"grid_hash": MD5, "order": ["date", "time"]})
    );

    // All single-valued keys are pinned into base_request (never param/axes).
    let base = &tree["base_request"];
    assert_eq!(base["class"], "d1");
    assert_eq!(base["dataset"], "climate-dt");
    assert_eq!(base["resolution"], "high");
    assert_eq!(base["expver"], "0001");
    assert!(base.get("date").is_none(), "date is an axis, not pinned");
    assert!(
        base.get("param").is_none(),
        "param is a variable, not pinned"
    );

    // chunking + catalogue provenance.
    assert_eq!(
        v["chunking"],
        json!({
            "default": {"date": 1, "time": 1, "values": 0},
            "max_chunk_cost": 30_000_000,
            "max_fields_per_job": 8_760,
            "max_multi_chunks": 64,
        })
    );
    assert_eq!(
        v["catalogue"],
        json!({"source": CATALOGUE_URL, "etag": "\"fixture-etag\"", "advisory": true})
    );

    // canonical_request echo is present and canonically ordered on the wire.
    let text = String::from_utf8(body.to_vec()).unwrap();
    let cr = &text[text.find("\"canonical_request\"").unwrap()..];
    assert!(cr.find("\"class\"").unwrap() < cr.find("\"date\"").unwrap());
    assert!(cr.find("\"date\"").unwrap() < cr.find("\"time\"").unwrap());
}

#[tokio::test]
async fn metadata_caps_long_temporal_default_chunks() {
    let config = r#"
chunks:
  max_chunk_cost: 30000000
  default_max_fields_per_job: 2
  grids:
    - collection: destination-earth
      match: { class: d1, dataset: climate-dt, resolution: high }
      count_values: 12
      nside: 1
      md5_grid_section: "f78d9d2d6f6f1b4c8f0b3a6d1b1e2c3d"
"#;
    let (status, _, body) = post_json(
        app_with_qube_and_config(arena_simple(), config),
        "/chunks/v1/destination-earth/metadata",
        &json!({
            "request": climate_dt_request(),
            "feature": {
                "type": "polygon",
                "shape": [[-20, -20], [20, -20], [20, 20], [-20, 20]],
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["chunking"]["default"],
        json!({"date": 1, "time": 2, "points": 0})
    );
}

#[tokio::test]
async fn metadata_packs_short_temporal_feature_series_into_one_default_chunk() {
    let config = r#"
chunks:
  max_chunk_cost: 30000000
  default_max_fields_per_job: 1000
  grids:
    - collection: destination-earth
      match: { class: d1, dataset: climate-dt, resolution: high }
      count_values: 12
      nside: 1
      md5_grid_section: "f78d9d2d6f6f1b4c8f0b3a6d1b1e2c3d"
"#;
    let (status, _, body) = post_json(
        app_with_qube_and_config(arena_simple(), config),
        "/chunks/v1/destination-earth/metadata",
        &json!({
            "request": climate_dt_request(),
            "feature": {
                "type": "polygon",
                "shape": [[-20, -20], [20, -20], [20, 20], [-20, 20]],
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let value: Value = serde_json::from_slice(&body).unwrap();
    // 2 dates x 2 times = 4 fields, under the target: one chunk, not four.
    assert_eq!(
        value["chunking"]["default"],
        json!({"date": 2, "time": 2, "points": 0})
    );
}

#[tokio::test]
async fn metadata_heterogeneous_builds_two_named_array_sets() {
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "resolution": "high",
        "activity": "scenariomip", "experiment": "ssp3-7.0", "generation": "1",
        "model": "ifs-nemo", "realization": "1", "expver": "1", "stream": "clte",
        "type": "fc", "levtype": "sfc",
        "date": "20200101",
        "time": ["0", "600", "1200"],
        "param": ["167", "168", "169", "228", "229"],
    });
    let (status, _, body) = post_json(
        app_with_qube(arena_heterogeneous()),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();

    let tree = &v["tree"];
    assert_eq!(tree["type"], "group");
    assert_eq!(tree["name"], "");
    let children = tree["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);

    // The catalogue qube's root-to-leaf cubes are used directly (param-major,
    // matching the real 34-params-x-24-times u 2-params-x-1-time case): the two
    // array_sets are {167,168,169} x {0000,0600,1200} and {228,229} x {0000}.
    // The group diverges on `time` (canonical order), so children are named by
    // their time value-set and ordered by it (BTreeMap: ["0000"] first).
    let names: Vec<&str> = children
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["time-0000", "time-0000_0600_1200"]);

    // child 0: the 2 special params, only at time 0000. time is single-valued
    // here, so it is PINNED into base_request (not an axis).
    let c0 = &children[0];
    assert_eq!(c0["type"], "array_set");
    assert_eq!(c0["axes"], json!([]));
    assert_eq!(c0["base_request"]["time"], "0000");
    assert_eq!(
        c0["variables"],
        json!([{"name": "228", "param": "228"}, {"name": "229", "param": "229"}])
    );
    assert_eq!(c0["extract"], json!({"grid_hash": MD5, "order": []}));

    // child 1: the 3 common params, at all three times.
    let c1 = &children[1];
    assert_eq!(
        c1["axes"],
        json!([{"dim": "time", "key": "time", "values": ["0000", "0600", "1200"]}])
    );
    assert_eq!(
        c1["variables"],
        json!([
            {"name": "167", "param": "167"}, {"name": "168", "param": "168"},
            {"name": "169", "param": "169"},
        ])
    );

    // date is pinned (single value) into every array_set's base_request.
    assert_eq!(c0["base_request"]["date"], "20200101");
    assert_eq!(c1["base_request"]["date"], "20200101");
    // Group attrs record the shared pinned selection.
    assert_eq!(tree["attrs"]["defined_by"]["class"], json!(["d1"]));
    assert_eq!(tree["attrs"]["defined_by"]["resolution"], json!(["high"]));
}

#[tokio::test]
async fn metadata_date_holes_exact_keeps_gaps_span_fills_calendar() {
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "resolution": "high", "levtype": "sfc",
        "date": ["20200101", "20200103", "20200105"],
        "param": "167",
    });

    // exact: non-contiguous values, no fill.
    let (status, _, body) = post_json(
        app_with_qube(arena_date_holes()),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request, "structure": {"gaps": "exact"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["tree"]["type"], "array_set");
    assert_eq!(
        v["tree"]["axes"],
        json!([{"dim": "date", "key": "date", "values": ["20200101", "20200103", "20200105"]}])
    );
    assert_eq!(v["tree"]["fill_on_missing"], false);

    // span: calendar-filled between min and max, fill_on_missing true.
    let (status, _, body) = post_json(
        app_with_qube(arena_date_holes()),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request, "structure": {"gaps": "span"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v["tree"]["axes"],
        json!([{"dim": "date", "key": "date", "values": [
            "20200101", "20200102", "20200103", "20200104", "20200105"
        ]}])
    );
    assert_eq!(v["tree"]["fill_on_missing"], true);
}

#[tokio::test]
async fn metadata_pins_qube_only_single_valued_keys_the_user_omitted() {
    // The qube branches on `stream` (single value clte) that the request never
    // mentions. It must still be pinned into base_request (catalogue carries
    // more structure than the request).
    let arena = json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1]},
        {"dim": "stream", "coords": {"strings": ["clte"]}, "parent": 0, "children": [2]},
        {"dim": "param", "coords": {"strings": ["167"]}, "parent": 1, "children": []},
    ]});
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "resolution": "high", "levtype": "sfc",
        "param": "167",
    });
    let (status, _, body) = post_json(
        app_with_qube(arena),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["tree"]["type"], "array_set");
    // stream was never in the request, yet it is pinned from the qube.
    assert_eq!(v["tree"]["base_request"]["stream"], "clte");
    assert_eq!(v["tree"]["base_request"]["class"], "d1");
    assert_eq!(v["tree"]["axes"], json!([]));
    assert_eq!(
        v["tree"]["variables"],
        json!([{"name": "167", "param": "167"}])
    );
}

#[tokio::test]
async fn metadata_strips_non_mars_catalogue_dimensions_from_everywhere() {
    let arena = json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1, 2]},
        {"dim": "location", "coords": "mn5", "parent": 0, "children": [3]},
        {"dim": "location", "coords": "lumi", "parent": 0, "children": [4]},
        {"dim": "date", "coords": "20250101", "parent": 1, "children": [5]},
        {"dim": "date", "coords": "20250101", "parent": 2, "children": [6]},
        {"dim": "param", "coords": "167", "parent": 3, "children": []},
        {"dim": "param", "coords": "165", "parent": 4, "children": []},
    ]});
    let config = r#"
chunks:
  catalogue_strip_keys: [location]
  grids:
    - collection: destination-earth
      match: {class: d1, dataset: climate-dt, resolution: high}
      count_values: 12
      nside: 1
"#;
    let (status, _, body) = post_json(
        app_with_qube_and_config(arena, config),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": {
            "class": "d1", "dataset": "climate-dt", "resolution": "high"
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(!text.contains("\"location\""), "{text}");
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["tree"]["axes"], json!([]));
    assert!(value["tree"]["base_request"].get("location").is_none());
    assert_eq!(
        value["tree"]["variables"],
        json!([{"name": "165", "param": "165"}, {"name": "167", "param": "167"}])
    );
}

#[tokio::test]
async fn metadata_location_filter_removes_lumi_only_axis_values() {
    let arena = json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1, 2]},
        {"dim": "activity", "coords": "baseline", "metadata": {"location": {"strings": ["mn5"]}}, "parent": 0, "children": [3]},
        {"dim": "activity", "coords": "baseline", "metadata": {"location": {"strings": ["lumi"]}}, "parent": 0, "children": [4]},
        {"dim": "date", "coords": "20141230/20141231", "parent": 1, "children": [5]},
        {"dim": "date", "coords": "20251231", "parent": 2, "children": [6]},
        {"dim": "param", "coords": "167", "parent": 3, "children": []},
        {"dim": "param", "coords": "167", "parent": 4, "children": []},
    ]});
    let config = r#"
chunks:
  catalogue_location: mn5
  grids:
    - collection: destination-earth
      match: {class: d1, dataset: climate-dt, resolution: high}
      count_values: 12
      nside: 1
"#;
    let (status, _, body) = post_json(
        app_with_qube_and_config(arena, config),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": {
            "class": "d1", "dataset": "climate-dt", "resolution": "high"
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value["tree"]["axes"],
        json!([{"dim": "date", "key": "date", "values": ["20141230", "20141231"]}])
    );
    assert!(!String::from_utf8_lossy(&body).contains("20251231"));
}

#[tokio::test]
async fn metadata_two_cubes_resolve_different_grids() {
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "levtype": "sfc",
        "resolution": ["high", "standard"],
        "generation": "2",
        "param": "167",
    });
    let (status, _, body) = post_json(
        app_with_qube(arena_two_grids()),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();

    let tree = &v["tree"];
    assert_eq!(tree["type"], "group");
    let children = tree["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    let names: Vec<&str> = children
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["resolution-high", "resolution-standard"]);

    // resolution=high -> grid 1 (12582912, MD5); resolution=standard+gen2 -> grid 2 (196608).
    assert_eq!(children[0]["grid"]["count_values"], 12582912);
    assert_eq!(children[0]["grid"]["md5GridSection"], MD5);
    assert_eq!(children[1]["grid"]["count_values"], 196608);
    assert_eq!(children[1]["grid"]["md5GridSection"], Value::Null);
    // Each pins its own resolution.
    assert_eq!(children[0]["base_request"]["resolution"], "high");
    assert_eq!(children[1]["base_request"]["resolution"], "standard");
}

#[tokio::test]
async fn metadata_unsplit_cube_is_split_across_grid_registry_entries() {
    // Deliberately omit resolution: the qube supplies one cube containing both.
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "levtype": "sfc",
        "generation": "2", "param": "167",
    });
    let (status, _, body) = post_json(
        app_with_qube(arena_unsplit_grids(&["standard", "high"])),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let value: Value = serde_json::from_slice(&body).unwrap();
    let tree = &value["tree"];
    assert_eq!(tree["type"], "group");
    let children = tree["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0]["name"], "resolution-high");
    assert_eq!(children[0]["base_request"]["resolution"], "high");
    assert_eq!(children[0]["grid"]["count_values"], 12_582_912);
    assert_eq!(children[0]["grid"]["nside"], 1024);
    assert_eq!(children[1]["name"], "resolution-standard");
    assert_eq!(children[1]["base_request"]["resolution"], "standard");
    assert_eq!(children[1]["grid"]["count_values"], 196_608);
    assert_eq!(children[1]["grid"]["nside"], Value::Null);
}

#[tokio::test]
async fn metadata_feature_is_resolved_per_grid_split_leaf() {
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "levtype": "sfc",
        "generation": "2", "param": "167",
    });
    let (status, _, body) = post_json(
        app_with_qube_and_config(
            arena_unsplit_grids(&["high", "standard"]),
            split_feature_chunks_section(),
        ),
        "/chunks/v1/destination-earth/metadata",
        &json!({
            "request": request,
            "feature": {
                "type": "polygon",
                "shape": [[-20, -20], [20, -20], [20, 20], [-20, 20]],
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let value: Value = serde_json::from_slice(&body).unwrap();
    let children = value["tree"]["children"].as_array().unwrap();
    assert_eq!(children[0]["grid"]["nside"], 1);
    assert_eq!(children[1]["grid"]["nside"], 2);
    let high_points = children[0]["feature"]["n_points"].as_u64().unwrap();
    let standard_points = children[1]["feature"]["n_points"].as_u64().unwrap();
    assert_ne!(high_points, standard_points);
    assert!(high_points > 0);
    assert!(standard_points > 0);
}

#[tokio::test]
async fn metadata_unsplit_cube_with_unregistered_value_still_names_original_cube() {
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "levtype": "sfc",
        "generation": "2", "param": "167",
    });
    assert_metadata_status(
        app_with_qube(arena_unsplit_grids(&["standard", "experimental", "high"])),
        json!({"request": request}),
        StatusCode::BAD_REQUEST,
        "resolution=[3 values]",
    )
    .await;
}

#[tokio::test]
async fn metadata_feature_has_exact_contract_json_and_points_dimension() {
    let config = nside_one_chunks_section(12);
    let body = json!({
        "request": climate_dt_request(),
        "feature": {
            "type": "polygon",
            "shape": [[-20, -20], [20, -20], [20, 20], [-20, 20]],
        },
    });
    let (status, _, bytes) = post_json(
        app_with_qube_and_config(arena_simple(), &config),
        "/chunks/v1/destination-earth/metadata",
        &body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["tree"],
        json!({
            "type": "array_set",
            "name": "",
            "base_request": {
                "class": "d1",
                "dataset": "climate-dt",
                "activity": "scenariomip",
                "experiment": "ssp3-7.0",
                "generation": "1",
                "model": "ifs-nemo",
                "realization": "1",
                "resolution": "high",
                "expver": "0001",
                "stream": "clte",
                "type": "fc",
                "levtype": "sfc",
            },
            "axes": [
                {"dim": "date", "key": "date", "values": ["20200101", "20200102"]},
                {"dim": "time", "key": "time", "values": ["0000", "1200"]},
            ],
            "variables": [
                {"name": "165", "param": "165"},
                {"name": "167", "param": "167"},
            ],
            "grid": {
                "kind": "healpix",
                "ordering": "nested",
                "count_values": 12,
                "nside": 1,
                "md5GridSection": MD5,
            },
            "feature": {
                "type": "polygon",
                "n_points": 1,
                "ranges": [[4, 5]],
                "coords": {"lat": [0.0], "lon": [0.0]},
            },
            "fill_on_missing": false,
            "extract": {"grid_hash": MD5, "order": ["date", "time"]},
        })
    );
    // Feature series pack their temporal axes into one default chunk while under
    // chunks.default_max_fields_per_job (here 2 dates x 2 times = 4 fields).
    assert_eq!(
        value["chunking"],
        json!({
            "default": {"date": 2, "time": 2, "points": 0},
            "max_chunk_cost": 30_000_000,
            "max_fields_per_job": 8_760,
            "max_multi_chunks": 64,
        })
    );
}

#[tokio::test]
async fn metadata_feature_is_attached_to_every_heterogeneous_array_set() {
    let config = nside_one_chunks_section(12);
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "resolution": "high",
        "date": "20200101", "time": ["0", "600", "1200"],
        "param": ["167", "168", "169", "228", "229"],
    });
    let (status, _, bytes) = post_json(
        app_with_qube_and_config(arena_heterogeneous(), &config),
        "/chunks/v1/destination-earth/metadata",
        &json!({
            "request": request,
            "feature": {
                "type": "polygon",
                "shape": [[-20, -20], [20, -20], [20, 20], [-20, 20]],
            },
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    let children = value["tree"]["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    for child in children {
        assert_eq!(child["feature"]["n_points"], 1);
        assert_eq!(child["feature"]["ranges"], json!([[4, 5]]));
    }
}

#[tokio::test]
async fn metadata_feature_and_span_are_orthogonal() {
    let config = nside_one_chunks_section(12);
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "resolution": "high",
        "date": ["20200101", "20200103", "20200105"], "param": "167",
    });
    let (status, _, bytes) = post_json(
        app_with_qube_and_config(arena_date_holes(), &config),
        "/chunks/v1/destination-earth/metadata",
        &json!({
            "request": request,
            "structure": {"gaps": "span"},
            "feature": {
                "type": "polygon",
                "shape": [[-20, -20], [20, -20], [20, 20], [-20, 20]],
            },
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["tree"]["fill_on_missing"], true);
    assert_eq!(
        value["tree"]["axes"][0]["values"],
        json!(["20200101", "20200102", "20200103", "20200104", "20200105"])
    );
    assert_eq!(value["tree"]["feature"]["ranges"], json!([[4, 5]]));
    assert!(value["chunking"]["default"].get("values").is_none());
    assert_eq!(value["chunking"]["default"]["points"], 0);
}

#[tokio::test]
async fn metadata_feature_over_cap_is_400_with_exact_count() {
    let config = nside_one_chunks_section(1);
    assert_metadata_status(
        app_with_qube_and_config(arena_simple(), &config),
        json!({
            "request": climate_dt_request(),
            "feature": {
                "type": "polygon",
                "shape": [[-20, -20], [110, -20], [110, 20], [-20, 20]],
            },
        }),
        StatusCode::BAD_REQUEST,
        "selects 2 points",
    )
    .await;
}

#[tokio::test]
async fn metadata_feature_rejects_non_healpix_set_by_name() {
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "levtype": "sfc",
        "resolution": ["high", "standard"], "generation": "2", "param": "167",
    });
    assert_metadata_status(
        app_with_qube(arena_two_grids()),
        json!({
            "request": request,
            "feature": {
                "type": "polygon",
                "shape": [[-0.1, -0.1], [0.1, -0.1], [0.1, 0.1], [-0.1, 0.1]],
            },
        }),
        StatusCode::BAD_REQUEST,
        "array_set 'resolution-standard'",
    )
    .await;
}

async fn assert_metadata_status(app: Router, body: Value, status: StatusCode, needle: &str) {
    let (got, _, bytes) = post_json(app, "/chunks/v1/destination-earth/metadata", &body).await;
    assert_eq!(got, status, "{}", String::from_utf8_lossy(&bytes));
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    let err = v["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no error field in {v}"));
    assert!(
        err.contains(needle),
        "error '{err}' should contain '{needle}'"
    );
    assert_eq!(
        v.as_object().unwrap().len(),
        1,
        "error body is exactly {{\"error\"}}: {v}"
    );
}

#[tokio::test]
async fn metadata_empty_intersection_is_400() {
    let mut request = climate_dt_request();
    request["param"] = json!("999"); // qube only has 165/167
    assert_metadata_status(
        app_with_qube(arena_simple()),
        json!({"request": request}),
        StatusCode::BAD_REQUEST,
        "empty intersection",
    )
    .await;
}

#[tokio::test]
async fn metadata_missing_catalogue_config_is_501() {
    // No catalogue source and no catalogue_url configured.
    let app = app_with_catalogue(None).0;
    assert_metadata_status(
        app,
        json!({"request": climate_dt_request()}),
        StatusCode::NOT_IMPLEMENTED,
        "catalogue is not configured",
    )
    .await;
}

#[tokio::test]
async fn metadata_catalogue_fetch_failure_with_no_cache_is_503() {
    let app = app_with_catalogue(Some(Arc::new(FailingCatalogue))).0;
    assert_metadata_status(
        app,
        json!({"request": climate_dt_request()}),
        StatusCode::SERVICE_UNAVAILABLE,
        "catalogue unavailable",
    )
    .await;
}

#[tokio::test]
async fn metadata_no_grid_match_for_a_cube_is_400_naming_the_cube() {
    // resolution=standard but generation 1 (registry entry 2 needs generation 2).
    let request = json!({
        "class": "d1", "dataset": "climate-dt", "levtype": "sfc",
        "resolution": "standard", "generation": "1", "param": "167",
    });
    assert_metadata_status(
        app_with_qube(arena_two_grids()),
        json!({"request": request}),
        StatusCode::BAD_REQUEST,
        "no grid is registered for cube",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_unknown_collection() {
    let (status, _, bytes) = post_json(
        app_with_qube(arena_simple()),
        "/chunks/v1/nope/metadata",
        &json!({"request": climate_dt_request()}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .contains("unknown collection 'nope'")
    );
}

#[tokio::test]
async fn metadata_rejects_malformed_unknown_or_misplaced_feature() {
    assert_metadata_status(
        app(),
        json!({"request": climate_dt_request(), "feature": {"type": "polygon", "shape": []}}),
        StatusCode::BAD_REQUEST,
        "feature",
    )
    .await;
    assert_metadata_status(
        app(),
        json!({
            "request": climate_dt_request(),
            "feature": {"type": "bbox", "shape": [[0, 0], [1, 0], [0, 1]]},
        }),
        StatusCode::BAD_REQUEST,
        "unsupported feature type 'bbox'",
    )
    .await;
    let mut request = climate_dt_request();
    request["feature"] = json!({"type": "polygon"});
    assert_metadata_status(
        app(),
        json!({"request": request}),
        StatusCode::BAD_REQUEST,
        "feature",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_bad_expansion() {
    let mut request = climate_dt_request();
    request["param"] = json!("bogus");
    // Expansion fails before the catalogue is consulted.
    assert_metadata_status(
        app(),
        json!({"request": request}),
        StatusCode::BAD_REQUEST,
        "request expansion failed",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_inconsistent_expansion() {
    // "0" and "0000" canonicalise to the same time: not a valid axis.
    let mut request = climate_dt_request();
    request["time"] = json!(["0", "0000"]);
    assert_metadata_status(
        app(),
        json!({"request": request}),
        StatusCode::BAD_REQUEST,
        "duplicate value '0000'",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_malformed_bodies() {
    let (status, _, _) = post_raw(
        app(),
        "/chunks/v1/destination-earth/metadata",
        b"not json".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_metadata_status(app(), json!({}), StatusCode::BAD_REQUEST, "'request'").await;
    assert_metadata_status(
        app(),
        json!({"request": {}}),
        StatusCode::BAD_REQUEST,
        "must not be empty",
    )
    .await;
    assert_metadata_status(
        app(),
        json!({"request": climate_dt_request(), "structure": {"gaps": "wat"}}),
        StatusCode::BAD_REQUEST,
        "structure.gaps",
    )
    .await;
}

#[tokio::test]
async fn metadata_without_metkit_is_not_implemented() {
    let (app, _) = crate::build_app_with_catalogue(
        server_config("http://127.0.0.1:1/", chunks_section()),
        Arc::new(UnavailableExpander),
        Some(Arc::new(FixtureCatalogue {
            arena: arena_simple(),
        })),
    )
    .expect("app builds");
    let (status, _, body) = post_json(
        app,
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": climate_dt_request()}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["error"].as_str().unwrap().contains("metkit"));
}

// ---------------------------------------------------------------------------
// mounting / auth / config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chunks_not_mounted_without_config_or_when_disabled() {
    for extra in ["", "chunks:\n  enabled: false\n"] {
        let (app, _) = app_with("http://127.0.0.1:1/", extra, Arc::new(FakeExpander));
        let (status, _, _) = post_json(
            app,
            "/chunks/v1/destination-earth/metadata",
            &json!({"request": climate_dt_request()}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "extra config: {extra:?}");
    }
}

#[tokio::test]
async fn chunks_routes_require_authentication_like_v2() {
    let extra = format!(
        "{}\nauthentication:\n  url: \"http://127.0.0.1:1\"\n  secret: \"s\"\n",
        chunks_section()
    );
    let (app, _) = app_with("http://127.0.0.1:1/", &extra, Arc::new(FakeExpander));
    for endpoint in ["metadata", "extract"] {
        let (status, headers, _) = post_json(
            app.clone(),
            &format!("/chunks/v1/destination-earth/{endpoint}"),
            &json!({"request": climate_dt_request()}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{endpoint}");
        assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
    }
}

#[test]
fn chunks_config_defaults_and_validation() {
    let cfg = server_config("http://127.0.0.1:1/", "");
    assert!(cfg.chunks.is_none());
    let cfg = server_config("http://127.0.0.1:1/", "chunks: {}\n");
    let chunks = cfg.chunks.unwrap();
    assert!(chunks.enabled);
    assert_eq!(chunks.max_chunk_cost, 20_000_000);
    assert_eq!(chunks.default_max_fields_per_job, 8_760);
    assert_eq!(chunks.max_multi_chunks, 64);
    assert_eq!(chunks.max_feature_points, 1_000_000);
    assert!(chunks.grids.is_empty());
    // Contract v2 catalogue defaults.
    assert!(chunks.catalogue_url.is_none());
    assert!(chunks.catalogue_location.is_none());
    assert_eq!(chunks.catalogue_ttl_secs, 300);
    assert!(chunks.catalogue_strip_keys.is_empty());
    chunks.validate().unwrap();

    let cfg = server_config(
        "http://127.0.0.1:1/",
        "chunks:\n  catalogue_url: \"https://x/api/v2/select/\"\n  catalogue_location: mn5\n  catalogue_strip_keys: [location]\n  catalogue_ttl_secs: 60\n",
    );
    let chunks = cfg.chunks.unwrap();
    assert_eq!(
        chunks.catalogue_url.as_deref(),
        Some("https://x/api/v2/select/")
    );
    assert_eq!(chunks.catalogue_location.as_deref(), Some("mn5"));
    assert_eq!(chunks.catalogue_strip_keys, ["location"]);
    assert_eq!(chunks.catalogue_ttl_secs, 60);

    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("grids: [{collection: c, count_values: 0}]").unwrap();
    assert!(bad.validate().is_err());
    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("grids: [{collection: c, count_values: 1, md5_grid_section: xyz}]")
            .unwrap();
    assert!(bad.validate().is_err());
    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("grids: [{collection: c, count_values: 1, match: {a: {b: 1}}}]")
            .unwrap();
    assert!(bad.validate().is_err());
    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("grids: [{collection: c, count_values: 48, nside: 3}]").unwrap();
    assert!(bad.validate().is_err());
    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("default_max_fields_per_job: 0").unwrap();
    assert!(bad.validate().is_err());
    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("max_multi_chunks: 0").unwrap();
    assert!(bad.validate().is_err());
    let bad: crate::config::ChunksConfig =
        serde_yaml::from_str("grids: [{collection: c, count_values: 47, nside: 2}]").unwrap();
    assert!(bad.validate().is_err());
    assert!(
        serde_yaml::from_str::<crate::config::ChunksConfig>(
            "grids: [{collection: c, count_values: 1, typo: 1}]"
        )
        .is_err()
    );
}

// ---------------------------------------------------------------------------
// extract (UNCHANGED by v2)
// ---------------------------------------------------------------------------

fn extract_body() -> Value {
    json!({
        "request": {
            "class": "d1",
            "dataset": "climate-dt",
            "resolution": "high",
            "expver": "0001",
            "levtype": "sfc",
            "date": ["20200101", "20200102"],
            "time": ["0000"],
            "param": "167",
        },
        "extract": {
            "ranges": [[0, 1000], [5000, 5100]],
            "order": ["date", "time"],
            "grid_hash": MD5,
            "dtype": "float64",
        }
    })
}

fn expected_job_body() -> Value {
    json!({
        "class": "d1",
        "dataset": "climate-dt",
        "resolution": "high",
        "expver": "0001",
        "levtype": "sfc",
        "date": ["20200101", "20200102"],
        "time": ["0000"],
        "param": "167",
        "extract": {
            "ranges": [[0, 1000], [5000, 5100]],
            "order": ["date", "time"],
            "grid_hash": MD5,
            "dtype": "float64",
            "shuffle": true,
        }
    })
}

/// Fake worker: records the POSTed job request and answers with chunk bytes.
async fn spawn_capturing_worker() -> (String, Arc<Mutex<Vec<Value>>>) {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::default();
    let seen_in_handler = seen.clone();
    let worker = Router::new().route(
        "/",
        post(move |axum::Json(job): axum::Json<Value>| {
            let seen = seen_in_handler.clone();
            async move {
                seen.lock().unwrap().push(job);
                (
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    b"zstd-chunk-bytes".to_vec(),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, worker).await.unwrap() });
    (format!("http://{addr}/"), seen)
}

/// Fake worker that accepts connections and never answers (job stays pending).
async fn spawn_hanging_worker() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    format!("http://{addr}/")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_forwards_flat_mars_plus_extract_and_returns_inline_bytes() {
    let (url, seen) = spawn_capturing_worker().await;
    let (app, _) = app_with(&url, chunks_section(), Arc::new(FakeExpander));
    let (status, headers, body) =
        post_json(app, "/chunks/v1/destination-earth/extract", &extract_body()).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "application/octet-stream");
    assert_eq!(&body[..], b"zstd-chunk-bytes");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0], expected_job_body());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_forwards_multi_chunks_as_one_job() {
    let (url, seen) = spawn_capturing_worker().await;
    let (app, _) = app_with(&url, chunks_section(), Arc::new(FakeExpander));
    let mut second = extract_body();
    second["request"]["param"] = json!("168");
    let (status, _, _) = post_json(
        app,
        "/chunks/v1/destination-earth/extract",
        &json!({"chunks": [extract_body(), second]}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0]["chunks"].as_array().unwrap().len(), 2);
    assert_eq!(seen[0]["chunks"][0], expected_job_body());
    assert_eq!(seen[0]["chunks"][1]["param"], "168");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_pending_redirects_to_v2_poll_url() {
    let url = spawn_hanging_worker().await;
    let extra = format!("{}\nserver:\n  v2_poll_timeout_ms: 150\n", chunks_section());
    let (app, state) = app_with(&url, &extra, Arc::new(FakeExpander));
    let (status, headers, body) = post_json(
        app.clone(),
        "/chunks/v1/destination-earth/extract",
        &extract_body(),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let location = headers[header::LOCATION].to_str().unwrap().to_string();
    let id = location
        .strip_prefix("/api/v2/requests/")
        .unwrap_or_else(|| panic!("Location should be the v2 poll URL, got {location}"))
        .to_string();
    assert!(headers.contains_key("x-bits-pending-status"));
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["id"], json!(id));
    assert_eq!(v["location"], json!(location));

    let job = state
        .bits
        .active_jobs()
        .into_iter()
        .find(|job| job.id == id)
        .expect("submitted job is active");
    assert_eq!(job.request, expected_job_body());
    assert_eq!(job.metadata["api"], "chunks");
    assert_eq!(job.metadata["collection"], COLLECTION);

    // Polling the Location on the existing v2 endpoint works (still pending).
    let resp = app
        .oneshot(Request::get(&location).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert_eq!(resp.headers()[header::LOCATION], location.as_str());
}

#[tokio::test]
async fn extract_validation_errors_are_400_and_submit_nothing() {
    let (app, state) = app_with(
        "http://127.0.0.1:1/",
        chunks_section(),
        Arc::new(FakeExpander),
    );

    let mut cases: Vec<(&str, Value, &str)> = Vec::new();
    cases.push((
        "destination-earth",
        json!({"request": {"param": "167"}}),
        "'extract'",
    ));
    let mut b = extract_body();
    b["extract"]["ranges"] = json!([]);
    cases.push(("destination-earth", b, "ranges"));
    let mut b = extract_body();
    b["extract"]["ranges"] = json!([[10, 5]]);
    cases.push(("destination-earth", b, "lo < hi"));
    let mut b = extract_body();
    b["extract"]["dtype"] = json!("float16");
    cases.push(("destination-earth", b, "float32"));
    let mut b = extract_body();
    b["extract"]["shuffle"] = json!("true");
    cases.push(("destination-earth", b, "shuffle"));
    let mut b = extract_body();
    b["extract"]["order"] = json!("date");
    cases.push(("destination-earth", b, "order"));
    let mut b = extract_body();
    b["feature"] = json!({"type": "polygon"});
    cases.push(("destination-earth", b, "feature"));
    let mut b = extract_body();
    b["extract"]["ranges"] = json!([[0, 20_000_000]]);
    cases.push(("destination-earth", b, "max_chunk_cost"));
    cases.push(("nope", extract_body(), "unknown collection 'nope'"));

    for (collection, body, needle) in cases {
        let (status, _, bytes) = post_json(
            app.clone(),
            &format!("/chunks/v1/{collection}/extract"),
            &body,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{needle}: {}",
            String::from_utf8_lossy(&bytes)
        );
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        let err = v["error"].as_str().unwrap();
        assert!(
            err.contains(needle),
            "error '{err}' should contain '{needle}'"
        );
    }
    let (status, _, _) = post_raw(app, "/chunks/v1/destination-earth/extract", b"{".to_vec()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        state.bits.active_jobs().is_empty(),
        "no job may be submitted"
    );
}

// ---------------------------------------------------------------------------
// real metkit (requires the `metkit` feature + native libs)
// ---------------------------------------------------------------------------

#[cfg(feature = "metkit")]
#[tokio::test]
async fn metadata_with_real_metkit_expansion() {
    // A qube for od/oper with the axes the request expands to.
    let arena = json!({"version": "1", "qube": [
        {"dim": "root", "coords": null, "parent": null, "children": [1]},
        {"dim": "date", "coords": {"strings": ["20240101", "20240102"]}, "parent": 0, "children": [2]},
        {"dim": "time", "coords": {"ints": [0, 1200]}, "parent": 1, "children": [3]},
        {"dim": "step", "coords": {"ints": [0, 6]}, "parent": 2, "children": [4]},
        {"dim": "param", "coords": {"strings": ["165", "167"]}, "parent": 3, "children": []},
    ]});
    let extra = r#"
chunks:
  grids:
    - collection: destination-earth
      match: { class: od, stream: oper }
      count_values: 6599680
      md5_grid_section: null
"#;
    let (app, _) = crate::build_app_with_catalogue(
        server_config("http://127.0.0.1:1/", extra),
        super::expand::default_expander(),
        Some(Arc::new(FixtureCatalogue { arena })),
    )
    .expect("app builds");
    let request = json!({
        "class": "od", "type": "fc", "stream": "oper", "expver": 1,
        "levtype": "sfc", "date": ["20240101", "20240102"], "time": ["0", "12"],
        "step": "0/6", "param": ["2t", "10u"],
    });
    let (status, _, body) = post_json(
        app.clone(),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    let text = String::from_utf8_lossy(&body).to_string();
    println!("real metkit metadata: {text}");
    assert_eq!(status, StatusCode::OK, "{text}");
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["version"], 2);
    let tree = &v["tree"];
    assert_eq!(tree["type"], "array_set");
    let dims: Vec<&str> = tree["axes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["dim"].as_str().unwrap())
        .collect();
    assert_eq!(dims, ["date", "time", "step"]);
    assert_eq!(tree["axes"][1]["values"], json!(["0000", "1200"]));
    assert_eq!(
        tree["variables"],
        json!([{"name": "165", "param": "165"}, {"name": "167", "param": "167"}])
    );
    assert_eq!(v["canonical_request"]["expver"], json!(["0001"]));
    assert_eq!(tree["extract"]["order"], json!(["date", "time", "step"]));

    // Invalid MARS values are a 400 from metkit.
    let (status, _, body) = post_json(
        app,
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": {"class": "od", "param": "not-a-param-xyz"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "{}",
        String::from_utf8_lossy(&body)
    );
}

// ---------------------------------------------------------------------------
// Regression: metkit defaults for UNSUPPLIED keys must not narrow the qube
// ---------------------------------------------------------------------------

/// Behaves like [`FakeExpander`] but, like real metkit, injects a DEFAULT
/// `param` (a value absent from the catalogue) whenever the user did not supply
/// `param`. This models metkit filling unsupplied keys with defaults that would
/// wrongly collapse the qube intersection if they were allowed to constrain it.
struct DefaultInjectingExpander;

impl RequestExpander for DefaultInjectingExpander {
    fn expand(
        &self,
        request: &Map<String, Value>,
    ) -> Result<Vec<(String, Vec<String>)>, ExpandError> {
        let mut entries = FakeExpander.expand(request)?;
        if !request.contains_key("param") {
            // A default param id that does NOT exist in the catalogue qube.
            entries.push(("param".to_string(), vec!["99999".to_string()]));
        }
        Ok(entries)
    }
}

fn app_with_qube_and_expander(arena: Value, expander: Arc<dyn RequestExpander>) -> Router {
    crate::build_app_with_catalogue(
        server_config("http://127.0.0.1:1/", chunks_section()),
        expander,
        Some(Arc::new(FixtureCatalogue { arena })),
    )
    .expect("app builds")
    .0
}

#[tokio::test]
async fn metadata_unsupplied_key_default_does_not_narrow_qube() {
    // The user omits `param`; metkit injects a default param absent from the
    // catalogue. That default must NOT prune the intersection: the qube supplies
    // the real params (165, 167) as variables, and the phantom default must not
    // leak into the echoed canonical_request.
    let mut request = climate_dt_request();
    request.as_object_mut().unwrap().remove("param");
    let (status, _, body) = post_json(
        app_with_qube_and_expander(arena_simple(), Arc::new(DefaultInjectingExpander)),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    let tree = &v["tree"];
    assert_eq!(tree["type"], "array_set");
    assert_eq!(
        tree["variables"],
        json!([{"name": "165", "param": "165"}, {"name": "167", "param": "167"}]),
        "variables must come from the qube, not the injected default 99999"
    );
    assert!(
        v["canonical_request"].get("param").is_none(),
        "phantom default param leaked into canonical_request: {}",
        v["canonical_request"]
    );
}
