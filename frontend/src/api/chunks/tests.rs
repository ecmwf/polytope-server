// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end `/chunks/v1` tests through the full `build_app` stack (auth,
//! support middleware, bits routing) with a fake MARS expander.

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

fn expected_metadata() -> Value {
    json!({
        "version": 1,
        "canonical_request": {
            "class": ["d1"],
            "type": ["fc"],
            "stream": ["clte"],
            "levtype": ["sfc"],
            "date": ["20200101", "20200102"],
            "time": ["0000", "1200"],
            "dataset": ["climate-dt"],
            "expver": ["0001"],
            "param": ["167", "165"],
            "activity": ["scenariomip"],
            "experiment": ["ssp3-7.0"],
            "generation": ["1"],
            "model": ["ifs-nemo"],
            "realization": ["1"],
            "resolution": ["high"],
        },
        "axes": [
            {"dim": "date", "key": "date", "values": ["20200101", "20200102"]},
            {"dim": "time", "key": "time", "values": ["0000", "1200"]},
        ],
        "grid": {"kind": "unstructured", "count_values": 12582912, "md5GridSection": MD5},
        "variables": [
            {"name": "167", "param": "167"},
            {"name": "165", "param": "165"},
        ],
        "chunking": {
            "default": {"date": 1, "time": 1, "values": 12582912},
            "max_chunk_cost": 30000000,
        },
        "extract": {"grid_hash": MD5, "order": ["date", "time"]},
    })
}

// ---------------------------------------------------------------------------
// metadata
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_happy_path_matches_contract_exactly() {
    let (status, headers, body) = post_json(
        app(),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": climate_dt_request()}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v, expected_metadata());

    // Object key order on the wire is canonical (metkit axis order), not
    // alphabetical, for canonical_request and chunking.default.
    let text = String::from_utf8(body.to_vec()).unwrap();
    let pos = |needle: &str| {
        text.find(needle)
            .unwrap_or_else(|| panic!("{needle} missing"))
    };
    let order = [
        "\"class\"",
        "\"type\"",
        "\"stream\"",
        "\"levtype\"",
        "\"date\"",
        "\"time\"",
        "\"dataset\"",
        "\"expver\"",
        "\"param\"",
        "\"activity\"",
        "\"resolution\"",
    ];
    for pair in order.windows(2) {
        assert!(
            pos(pair[0]) < pos(pair[1]),
            "{} should precede {}",
            pair[0],
            pair[1]
        );
    }
    let chunking = &text[pos("\"chunking\"")..];
    assert!(chunking.find("\"date\"").unwrap() < chunking.find("\"time\"").unwrap());
    assert!(chunking.find("\"time\"").unwrap() < chunking.find("\"values\"").unwrap());
}

#[tokio::test]
async fn metadata_single_param_and_second_grid_entry() {
    let mut request = climate_dt_request();
    request["resolution"] = json!("standard");
    request["generation"] = json!(2);
    request["param"] = json!("2t");
    request["time"] = json!("1200");
    let (status, _, body) = post_json(
        app(),
        "/chunks/v1/destination-earth/metadata",
        &json!({"request": request}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v["axes"],
        json!([{"dim": "date", "key": "date", "values": ["20200101", "20200102"]}])
    );
    assert_eq!(v["variables"], json!([{"name": "167", "param": "167"}]));
    assert_eq!(
        v["grid"],
        json!({"kind": "unstructured", "count_values": 196608, "md5GridSection": null})
    );
    assert_eq!(
        v["chunking"]["default"],
        json!({"date": 1, "values": 196608})
    );
    assert_eq!(v["extract"], json!({"grid_hash": null, "order": ["date"]}));
}

async fn assert_metadata_400(collection: &str, body: Value, needle: &str) {
    let (status, _, bytes) =
        post_json(app(), &format!("/chunks/v1/{collection}/metadata"), &body).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
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
async fn metadata_rejects_unknown_collection() {
    assert_metadata_400(
        "nope",
        json!({"request": climate_dt_request()}),
        "unknown collection 'nope'",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_feature() {
    assert_metadata_400(
        COLLECTION,
        json!({"request": climate_dt_request(), "feature": {"type": "polygon", "shape": []}}),
        "feature",
    )
    .await;
    let mut request = climate_dt_request();
    request["feature"] = json!({"type": "polygon"});
    assert_metadata_400(COLLECTION, json!({"request": request}), "feature").await;
}

#[tokio::test]
async fn metadata_rejects_no_grid_match() {
    let mut request = climate_dt_request();
    request["resolution"] = json!("standard"); // generation 1 != registry's 2
    assert_metadata_400(
        COLLECTION,
        json!({"request": request}),
        "no grid is registered",
    )
    .await;
    // A request spanning two grids matches neither entry.
    let mut request = climate_dt_request();
    request["resolution"] = json!(["high", "standard"]);
    assert_metadata_400(
        COLLECTION,
        json!({"request": request}),
        "no grid is registered",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_bad_expansion() {
    let mut request = climate_dt_request();
    request["param"] = json!("bogus");
    assert_metadata_400(
        COLLECTION,
        json!({"request": request}),
        "request expansion failed",
    )
    .await;
}

#[tokio::test]
async fn metadata_rejects_inconsistent_expansion() {
    // "0" and "0000" canonicalise to the same time: not a valid axis.
    let mut request = climate_dt_request();
    request["time"] = json!(["0", "0000"]);
    assert_metadata_400(
        COLLECTION,
        json!({"request": request}),
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
    assert_metadata_400(COLLECTION, json!({}), "'request'").await;
    assert_metadata_400(COLLECTION, json!({"request": {}}), "must not be empty").await;
}

#[tokio::test]
async fn metadata_without_metkit_is_not_implemented() {
    let (app, _) = app_with(
        "http://127.0.0.1:1/",
        chunks_section(),
        Arc::new(UnavailableExpander),
    );
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
    assert!(chunks.grids.is_empty());
    chunks.validate().unwrap();

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
    assert!(
        serde_yaml::from_str::<crate::config::ChunksConfig>(
            "grids: [{collection: c, count_values: 1, typo: 1}]"
        )
        .is_err()
    );
}

// ---------------------------------------------------------------------------
// extract
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
    b["extract"]["dtype"] = json!("float32");
    cases.push(("destination-earth", b, "float64"));
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
    let extra = r#"
chunks:
  grids:
    - collection: destination-earth
      match: { class: od, stream: oper }
      count_values: 6599680
      md5_grid_section: null
"#;
    let (app, _) = app_with(
        "http://127.0.0.1:1/",
        extra,
        super::expand::default_expander(),
    );
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
    let dims: Vec<&str> = v["axes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["dim"].as_str().unwrap())
        .collect();
    assert_eq!(dims, ["date", "time", "step"]);
    assert_eq!(v["axes"][1]["values"], json!(["0000", "1200"]));
    assert_eq!(
        v["variables"],
        json!([{"name": "167", "param": "167"}, {"name": "165", "param": "165"}])
    );
    assert_eq!(v["canonical_request"]["expver"], json!(["0001"]));
    assert_eq!(v["extract"]["order"], json!(["date", "time", "step"]));
    assert_eq!(
        v["chunking"]["default"],
        json!({"date": 1, "time": 1, "step": 1, "values": 6599680})
    );

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
