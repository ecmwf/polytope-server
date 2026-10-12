// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Keeps `docs/openapi/v1.yaml` honest: real v1 responses are checked against
//! the documented status codes, headers, media types and body schemas, and
//! every documented operation must be routed (and nothing else on those paths).

use std::io::{Read, Write};

use axum::body::Body;
use axum::http::{Method, Request, Response, StatusCode, header};
use http_body_util::BodyExt;
use polytope_server::build_app;
use polytope_server::config::ServerConfig;
use serde_json::{Value, json};
use tower::ServiceExt;

const SPEC: &str = include_str!("../../docs/openapi/v1.yaml");
const METHODS: [&str; 5] = ["GET", "POST", "PUT", "PATCH", "DELETE"];

fn spec() -> Value {
    serde_yaml::from_str(SPEC).expect("docs/openapi/v1.yaml is valid YAML")
}

/// Resolves a `$ref` of the form `#/components/<kind>/<name>` in the spec.
fn resolve<'a>(spec: &'a Value, node: &'a Value) -> &'a Value {
    match node.get("$ref").and_then(Value::as_str) {
        Some(r) => {
            let pointer = r.strip_prefix('#').expect("local $ref");
            spec.pointer(pointer)
                .unwrap_or_else(|| panic!("dangling $ref {r}"))
        }
        None => node,
    }
}

fn app_from_yaml(yaml: &str) -> axum::Router {
    let cfg: ServerConfig = serde_yaml::from_str(yaml).expect("config parses");
    build_app(cfg).expect("app builds").0
}

const AUTH_BLOCK_ANON: &str = r#"
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
  allow_anonymous: true
support:
  default_url: "https://support.ecmwf.int/"
"#;

fn app_without_collections() -> axum::Router {
    app_from_yaml(&format!(
        "polytope:\n  site: bol\n  env: tst\nbits: {{}}\n{AUTH_BLOCK_ANON}"
    ))
}

fn app_requiring_auth() -> axum::Router {
    app_from_yaml(&format!(
        r#"
polytope:
  site: bol
  env: tst
bits: {{}}
authentication:
  url: "http://{}"
  secret: "s"
support:
  default_url: "https://support.ecmwf.int/"
"#,
        closed_port()
    ))
}

fn app_with_target(addr: std::net::SocketAddr, poll_timeout_ms: u64) -> axum::Router {
    app_from_yaml(&format!(
        r#"
polytope:
  site: bol
  env: tst
bits:
  targets:
    t:
      type: http
      url: "http://{addr}/"
  collections:
    ecmwf:
      - r:
          - target::t
{AUTH_BLOCK_ANON}
server:
  v1_poll_timeout_ms: {poll_timeout_ms}
"#
    ))
}

/// An address nothing listens on.
fn closed_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// A backend that accepts connections but never answers, so jobs stay pending.
fn stalled_backend() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut open = Vec::new();
        while let Ok((stream, _)) = listener.accept() {
            open.push(stream);
        }
    });
    addr
}

/// Long enough to be worth compressing.
const GRIB_BODY: &[u8] = b"GRIB0000000000000000000000000000000000000000000000000000000000000";

/// A backend that answers every request with `200` and a small GRIB-typed body.
fn data_backend() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let header_end = loop {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
                let len: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                while buf.len() < header_end + len {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = GRIB_BODY;
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: application/x-grib\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(body);
            });
        }
    });
    addr
}

struct Checked {
    status: StatusCode,
    headers: header::HeaderMap,
    body: Vec<u8>,
}

impl Checked {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("JSON body")
    }
}

fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let builder = Request::builder().method(method).uri(uri);
    match body {
        Some(v) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

/// Sends `request` and asserts the response conforms to the operation
/// documented for `path_template` + the request method.
async fn check(app: &axum::Router, request: Request<Body>, path_template: &str) -> Checked {
    let method = request.method().as_str().to_ascii_lowercase();
    let uri = request.uri().to_string();
    let resp: Response<Body> = app.clone().oneshot(request).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    check_response(&method, &uri, path_template, status, &headers, &body);
    Checked {
        status,
        headers,
        body,
    }
}

/// The documented status is listed, headers marked `required: true` are
/// present, the media type is listed (falling back to `type/*` and `*/*`), a
/// body is present exactly when content is documented, and a JSON body
/// validates against its schema.
fn check_response(
    method: &str,
    uri: &str,
    path_template: &str,
    status: StatusCode,
    headers: &header::HeaderMap,
    body: &[u8],
) {
    let spec = spec();
    let operation = spec
        .pointer(&format!(
            "/paths/{}/{method}",
            path_template.replace('/', "~1")
        ))
        .unwrap_or_else(|| panic!("{method} {path_template} is not documented"));
    let doc = operation["responses"]
        .get(status.as_str())
        .unwrap_or_else(|| panic!("{method} {uri}: status {status} is not documented"));
    let doc = resolve(&spec, doc);

    for (name, def) in doc
        .get("headers")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        if def.get("required") == Some(&Value::Bool(true)) {
            assert!(
                headers.contains_key(name.as_str()),
                "{method} {uri} -> {status}: required header `{name}` missing"
            );
        }
    }

    let content = doc.get("content").and_then(Value::as_object);
    let Some(content) = content else {
        assert!(
            body.is_empty(),
            "{method} {uri} -> {status}: body present but no content documented"
        );
        return;
    };
    assert!(
        !body.is_empty(),
        "{method} {uri} -> {status}: content documented but body is empty"
    );

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap()
        .trim()
        .to_ascii_lowercase();
    let family = content_type
        .split('/')
        .next()
        .map(|t| format!("{t}/*"))
        .unwrap_or_default();
    let media = content
        .get(&content_type)
        .or_else(|| content.get(&family))
        .or_else(|| content.get("*/*"))
        .unwrap_or_else(|| {
            panic!("{method} {uri} -> {status}: media type `{content_type}` not documented")
        });

    if content.contains_key(&content_type) && content_type == "application/json" {
        let instance: Value = serde_json::from_slice(body).expect("JSON body");
        validate(
            &spec,
            &media["schema"],
            &instance,
            &format!("{method} {uri} -> {status}"),
        );
    }
}

fn validate(spec: &Value, schema: &Value, instance: &Value, what: &str) {
    let root = json!({
        "$ref": "#/__schema",
        "__schema": schema.clone(),
        "components": spec["components"].clone(),
    });
    let validator = jsonschema::validator_for(&root).expect("schema compiles");
    let errors: Vec<String> = validator
        .iter_errors(instance)
        .map(|e| format!("{e} at {}", e.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{what}: body does not match schema:\n{}\nbody: {instance}",
        errors.join("\n")
    );
}

/// Validates `instance` against a named component schema, for responses whose
/// documented media type admits several shapes.
fn validate_component(name: &str, instance: &Value) {
    let spec = spec();
    let schema = json!({"$ref": format!("#/components/schemas/{name}")});
    validate(&spec, &schema, instance, name);
}

fn submit_body() -> Value {
    json!({"verb": "retrieve", "request": {"class": "od", "param": "t"}})
}

#[tokio::test]
async fn spec_is_a_valid_openapi_document_shape() {
    let spec = spec();
    assert_eq!(spec["openapi"], "3.1.0");
    assert!(spec["paths"].as_object().is_some_and(|p| !p.is_empty()));
    // Every `$ref` must resolve.
    fn walk(root: &Value, v: &Value) {
        match v {
            Value::Object(m) => {
                if let Some(Value::String(r)) = m.get("$ref") {
                    assert!(root.pointer(&r[1..]).is_some(), "dangling $ref {r}");
                }
                m.values().for_each(|c| walk(root, c));
            }
            Value::Array(a) => a.iter().for_each(|c| walk(root, c)),
            _ => {}
        }
    }
    walk(&spec, &spec);
}

#[tokio::test]
async fn documented_methods_are_routed_and_others_are_not() {
    let app = app_without_collections();
    let spec = spec();
    for (template, item) in spec["paths"].as_object().unwrap() {
        let uri = template.replace("{id}", "x");
        for method in METHODS {
            let documented = item.get(method.to_ascii_lowercase()).is_some();
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::from_bytes(method.as_bytes()).unwrap())
                        .uri(&uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(submit_body().to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let routed = resp.status() != StatusCode::METHOD_NOT_ALLOWED;
            assert_eq!(
                routed,
                documented,
                "{method} {template}: documented={documented} but server answered {}",
                resp.status()
            );
        }
    }
}

#[tokio::test]
async fn meta_endpoints_match_spec() {
    let app = app_with_target(stalled_backend(), 50);

    let r = check(&app, req("GET", "/api/v1/test", None), "/api/v1/test").await;
    assert_eq!(r.status, StatusCode::OK);

    let r = check(
        &app,
        req("GET", "/api/v1/collections", None),
        "/api/v1/collections",
    )
    .await;
    assert_eq!(r.json()["message"], json!(["ecmwf"]));

    let r = check(&app, req("GET", "/api/v1/user", None), "/api/v1/user").await;
    assert_eq!(r.json()["live requests"], "0");

    check(
        &app,
        req("GET", "/api/v1/requests", None),
        "/api/v1/requests",
    )
    .await;
}

#[tokio::test]
async fn submit_and_poll_pending_match_spec() {
    let app = app_with_target(stalled_backend(), 50);

    let r = check(
        &app,
        req("POST", "/api/v1/requests/ecmwf", Some(submit_body())),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
    let location = r.headers[header::LOCATION].to_str().unwrap().to_string();
    assert!(
        location.starts_with("./"),
        "Location is relative: {location}"
    );
    let id = location.trim_start_matches("./");

    let r = check(
        &app,
        req("GET", &format!("/api/v1/requests/{id}"), None),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
}

#[tokio::test]
async fn completed_request_is_delivered_inline_as_documented() {
    let app = app_with_target(data_backend(), 2_000);

    let r = check(
        &app,
        req("POST", "/api/v1/requests/ecmwf", Some(submit_body())),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
    let location = r.headers[header::LOCATION].to_str().unwrap().to_string();
    let id = location.trim_start_matches("./");

    let mut r = None;
    for _ in 0..50 {
        let resp = check(
            &app,
            req("GET", &format!("/api/v1/requests/{id}"), None),
            "/api/v1/requests/{id}",
        )
        .await;
        if resp.status != StatusCode::ACCEPTED {
            r = Some(resp);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let r = r.expect("request completed within 5s");
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body, GRIB_BODY);
    assert_eq!(r.headers[header::CONTENT_TYPE], "application/x-grib");
    assert_eq!(
        r.headers[header::CONTENT_LENGTH],
        GRIB_BODY.len().to_string().as_str()
    );
    assert!(r.headers.contains_key(header::CONTENT_DISPOSITION));

    // The result is consumed by the fetch.
    let r = check(
        &app,
        req("GET", &format!("/api/v1/requests/{id}"), None),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn failures_match_spec() {
    let app = app_without_collections();

    let r = check(
        &app,
        req("POST", "/api/v1/requests/nope", Some(submit_body())),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);

    let app = app_with_target(stalled_backend(), 50);
    let r = check(
        &app,
        req(
            "POST",
            "/api/v1/requests/ecmwf",
            Some(json!({"verb": "archive", "request": {}})),
        ),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    let r = check(
        &app,
        req("GET", "/api/v1/requests/does-not-exist", None),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);

    // An unreachable backend fails the job; the failure arrives on the submit.
    let dead = app_with_target(closed_port(), 2_000);
    let r = check(
        &dead,
        req("POST", "/api/v1/requests/ecmwf", Some(submit_body())),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn collection_listing_and_cancel_match_spec() {
    let app = app_with_target(stalled_backend(), 50);

    let r = check(
        &app,
        req("GET", "/api/v1/requests/ecmwf", None),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.json()["message"].is_array());

    let r = check(
        &app,
        req("DELETE", "/api/v1/requests/some-id", None),
        "/api/v1/requests/{id}",
    )
    .await;
    assert_eq!(r.json()["message"], "Successfully revoked 0 requests");

    check(
        &app,
        req("DELETE", "/api/v1/requests/all", None),
        "/api/v1/requests/{id}",
    )
    .await;
}

#[tokio::test]
async fn missing_credentials_match_spec() {
    let app = app_requiring_auth();
    let spec = spec();
    for (template, item) in spec["paths"].as_object().unwrap() {
        for method in METHODS {
            let Some(op) = item.get(method.to_ascii_lowercase()) else {
                continue;
            };
            let open = op.get("security") == Some(&json!([]));
            let uri = template.replace("{id}", "ecmwf");
            let body = (method == "POST" && template.contains("requests")).then(submit_body);
            let r = check(&app, req(method, &uri, body), template).await;
            let expected = if open {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            };
            assert_eq!(r.status, expected, "{method} {template}");
        }
    }
}

#[tokio::test]
async fn head_on_a_poll_consumes_the_result_as_documented() {
    let app = app_with_target(data_backend(), 2_000);
    let r = check(
        &app,
        req("POST", "/api/v1/requests/ecmwf", Some(submit_body())),
        "/api/v1/requests/{id}",
    )
    .await;
    let location = r.headers[header::LOCATION].to_str().unwrap().to_string();
    let uri = format!("/api/v1/requests/{}", location.trim_start_matches("./"));

    let mut head_status = StatusCode::ACCEPTED;
    for _ in 0..50 {
        let resp = app
            .clone()
            .oneshot(Request::head(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        head_status = resp.status();
        if head_status != StatusCode::ACCEPTED {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(head_status, StatusCode::OK);

    let r = check(&app, req("GET", &uri, None), "/api/v1/requests/{id}").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn deprecated_endpoints_match_spec() {
    let app = app_without_collections();
    let r = check(
        &app,
        req("GET", "/api/v1/downloads/abc", None),
        "/api/v1/downloads/{id}",
    )
    .await;
    assert_eq!(r.status, StatusCode::GONE);
    for method in ["GET", "POST"] {
        check(
            &app,
            req(method, "/api/v1/uploads/abc", None),
            "/api/v1/uploads/{id}",
        )
        .await;
    }
}

#[tokio::test]
async fn malformed_submit_bodies_match_spec() {
    let app = app_with_target(stalled_backend(), 50);
    let post = |content_type: Option<&str>, body: &str| {
        let mut b = Request::post("/api/v1/requests/ecmwf");
        if let Some(ct) = content_type {
            b = b.header(header::CONTENT_TYPE, ct);
        }
        b.body(Body::from(body.to_string())).unwrap()
    };

    for (request, expected) in [
        (post(None, "{}"), StatusCode::UNSUPPORTED_MEDIA_TYPE),
        (
            post(Some("application/json"), "{\"verb\": \"retrieve\"}"),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            post(Some("application/json"), "{not json"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let r = check(&app, request, "/api/v1/requests/{id}").await;
        assert_eq!(r.status, expected);
    }
}

#[tokio::test]
async fn auth_service_outage_matches_spec() {
    // The auth backend is unreachable, so a presented credential cannot be checked.
    let app = app_requiring_auth();
    let request = Request::get("/api/v1/collections")
        .header(header::AUTHORIZATION, "Bearer anything")
        .body(Body::empty())
        .unwrap();
    let r = check(&app, request, "/api/v1/collections").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn redirect_responses_match_spec() {
    // A 303 needs a remote worker pool to be produced end to end, so the real
    // response builder is called directly instead of going through the router.
    use polytope_server::api::v1::build_redirect_response;
    for resp in [
        build_redirect_response("https://bobs.example/data", None, None),
        build_redirect_response(
            "https://bobs.example/data",
            Some("application/x-grib"),
            Some(12345),
        ),
    ] {
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(status, StatusCode::SEE_OTHER);
        check_response(
            "get",
            "/api/v1/requests/x",
            "/api/v1/requests/{id}",
            status,
            &headers,
            &body,
        );
    }
}

fn authenticated_app(auth_url: &str, backend: std::net::SocketAddr) -> axum::Router {
    app_from_yaml(&format!(
        r#"
polytope:
  site: bol
  env: tst
bits:
  targets:
    t:
      type: http
      url: "http://{backend}/"
  collections:
    ecmwf:
      - r:
          - target::t
authentication:
  url: "{auth_url}"
  secret: "{AUTH_SECRET}"
support:
  default_url: "https://support.ecmwf.int/"
server:
  v1_poll_timeout_ms: 50
"#
    ))
}

const AUTH_SECRET: &str = "testsecret";

fn user_jwt(username: &str) -> String {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({
            "version": 1,
            "username": username,
            "realm": "testrealm",
            "roles": ["user"],
            "attributes": {},
            "scopes": {},
            "exp": chrono::Utc::now().timestamp() + 3600,
        }),
        &jsonwebtoken::EncodingKey::from_secret(AUTH_SECRET.as_bytes()),
    )
    .unwrap()
}

fn as_user(mut request: Request<Body>, token: &str) -> Request<Body> {
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    request
}

#[tokio::test]
async fn authenticated_listing_ownership_and_cancel_match_spec() {
    let mut auth = mockito::Server::new_async().await;
    for user in ["alice", "bob"] {
        auth.mock("GET", "/authenticate")
            .match_header("Authorization", format!("Bearer tok-{user}").as_str())
            .with_status(200)
            .with_header("Authorization", &format!("Bearer {}", user_jwt(user)))
            .create_async()
            .await;
    }
    let app = authenticated_app(&auth.url(), stalled_backend());
    let tpl = "/api/v1/requests/{id}";

    let r = check(
        &app,
        as_user(
            req("POST", "/api/v1/requests/ecmwf", Some(submit_body())),
            "tok-alice",
        ),
        tpl,
    )
    .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
    let id = r.headers[header::LOCATION]
        .to_str()
        .unwrap()
        .trim_start_matches("./")
        .to_string();

    // Alice sees her request, as a full legacy record.
    for (uri, template) in [
        ("/api/v1/requests", "/api/v1/requests"),
        ("/api/v1/requests/ecmwf", tpl),
    ] {
        let r = check(&app, as_user(req("GET", uri, None), "tok-alice"), template).await;
        assert_eq!(r.status, StatusCode::OK);
        validate_component("RequestList", &r.json());
        let records = r.json()["message"].as_array().unwrap().clone();
        assert_eq!(records.len(), 1, "{uri}");
        assert_eq!(records[0]["id"], id.as_str());
        assert_eq!(records[0]["collection"], "ecmwf");
        assert_eq!(records[0]["user"]["username"], "alice");
        assert_eq!(records[0]["verb"], "retrieve");
    }
    let r = check(
        &app,
        as_user(req("GET", "/api/v1/user", None), "tok-alice"),
        "/api/v1/user",
    )
    .await;
    assert_eq!(r.json()["live requests"], "1");

    // Bob sees none of it and cannot touch it.
    let r = check(
        &app,
        as_user(req("GET", "/api/v1/requests", None), "tok-bob"),
        "/api/v1/requests",
    )
    .await;
    assert_eq!(r.json()["message"], json!([]));
    for method in ["GET", "DELETE"] {
        let r = check(
            &app,
            as_user(
                req(method, &format!("/api/v1/requests/{id}"), None),
                "tok-bob",
            ),
            tpl,
        )
        .await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{method}");
    }

    // Alice cancels it.
    let r = check(
        &app,
        as_user(
            req("DELETE", &format!("/api/v1/requests/{id}"), None),
            "tok-alice",
        ),
        tpl,
    )
    .await;
    assert_eq!(r.json()["message"], "Successfully revoked 1 requests");
}

#[tokio::test]
async fn inline_data_has_content_length_unless_compressed() {
    let app = app_with_target(data_backend(), 2_000);
    let r = check(
        &app,
        req("POST", "/api/v1/requests/ecmwf", Some(submit_body())),
        "/api/v1/requests/{id}",
    )
    .await;
    let location = r.headers[header::LOCATION].to_str().unwrap().to_string();
    let uri = format!("/api/v1/requests/{}", location.trim_start_matches("./"));

    let mut request = req("GET", &uri, None);
    request
        .headers_mut()
        .insert(header::ACCEPT_ENCODING, "gzip".parse().unwrap());
    let mut resp = None;
    for _ in 0..50 {
        let r = app.clone().oneshot(clone_request(&request)).await.unwrap();
        if r.status() != StatusCode::ACCEPTED {
            resp = Some(r);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let resp = resp.expect("request completed within 5s");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::CONTENT_ENCODING], "gzip");
    assert!(!resp.headers().contains_key(header::CONTENT_LENGTH));
}

fn clone_request(request: &Request<Body>) -> Request<Body> {
    let mut copy = Request::builder()
        .method(request.method())
        .uri(request.uri())
        .body(Body::empty())
        .unwrap();
    *copy.headers_mut() = request.headers().clone();
    copy
}
