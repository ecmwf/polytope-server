// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Checks that the server sends `location:` (lowercase `l`) in the raw HTTP
//! wire format for both HTTP/1.1 and HTTP/2, with no intermediate proxy
//! (e.g. nginx) that might normalise the casing.
//!
//! Background
//! ----------
//! HTTP/2 mandates lowercase header field names (RFC 7540 §8.1.2).  HTTP/1.1
//! headers are case-insensitive by spec, and hyper 1.x serialises them from
//! the lowercase bytes stored internally in `HeaderName`.  A capital `L`
//! (`Location:`) seen against real deployments comes from nginx normalising
//! HTTP/1.1 response headers, not from the application.

use polytope_server::build_app;
use polytope_server::config::ServerConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ── shared setup ─────────────────────────────────────────────────────────────

/// Spawn a polytope-server backed by a stalling TCP target (accepts
/// connections, never replies).  The v1 poll timeout is 50 ms so the submit
/// path returns `202 Accepted` quickly instead of blocking.
///
/// Returns the socket address the polytope-server is listening on.
async fn spawn_stalled_server() -> std::net::SocketAddr {
    // Stall target: accept connections and keep them open indefinitely so
    // the bits broker never sees a response, leaving the job "processing".
    let stall_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let stall_addr = stall_listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut open = Vec::new();
        while let Ok((stream, _)) = stall_listener.accept() {
            open.push(stream);
        }
    });

    let yaml = format!(
        r#"
polytope:
  site: bol
  env: tst
bits:
  targets:
    stall_target:
      type: http
      url: "http://{stall_addr}/"
  collections:
    ecmwf:
      - my_route:
          - target::stall_target
authentication:
  url: "http://127.0.0.1:1"
  secret: "s"
  allow_anonymous: true
support:
  default_url: "https://support.ecmwf.int/"
server:
  v1_poll_timeout_ms: 50
"#
    );

    let cfg: ServerConfig = serde_yaml::from_str(&yaml).expect("config parses");
    let (app, _) = build_app(cfg).expect("app builds");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server error");
    });

    addr
}

const SUBMIT_BODY: &str = r#"{"verb":"retrieve","request":{"param":"t"}}"#;
const SUBMIT_PATH: &str = "/api/v1/requests/ecmwf";

// ── HTTP/1.1 ─────────────────────────────────────────────────────────────────

/// Sends a raw HTTP/1.1 request over a plain `TcpStream` and inspects the
/// wire bytes to confirm `location:` (lowercase) is present and `Location:`
/// (title-case) is absent.
#[tokio::test]
async fn location_header_is_lowercase_in_http11_wire_format() {
    let addr = spawn_stalled_server().await;

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

    let request = format!(
        "POST {SUBMIT_PATH} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {body_len}\r\n\
         Connection: close\r\n\
         \r\n\
         {SUBMIT_BODY}",
        body_len = SUBMIT_BODY.len(),
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let raw_str = String::from_utf8_lossy(&raw);

    // Only inspect the header section (before the blank line).
    let headers = raw_str.split("\r\n\r\n").next().unwrap_or(&raw_str);

    assert!(
        headers.starts_with("HTTP/1.1 202"),
        "expected 202 Accepted, got: {headers}"
    );
    // Sanity check: a location header is present at all (case-insensitive).
    assert!(
        headers.to_lowercase().contains("\r\nlocation:"),
        "response must include a location header; raw headers:\n{headers}"
    );
    // The actual check: the name must be lowercase on the wire.
    assert!(
        headers.contains("\r\nlocation:"),
        "location header must be lowercase ('l') in HTTP/1.1 wire format; raw headers:\n{headers}"
    );
    assert!(
        !headers.contains("\r\nLocation:"),
        "location header must NOT be title-case ('L') in HTTP/1.1 wire format; raw headers:\n{headers}"
    );
}

// ── HTTP/2 ───────────────────────────────────────────────────────────────────

/// Opens an HTTP/2 cleartext (h2c prior-knowledge) connection and confirms
/// that the `location` header key is lowercase.
///
/// RFC 7540 §8.1.2 requires all HTTP/2 header field names to be lowercase.
/// hyper enforces this: `HeaderName` stores bytes in lowercase and the HTTP/2
/// encoder writes them as-is.  This test also validates that axum's `serve`
/// correctly auto-detects h2c via the connection preface.
#[tokio::test]
async fn location_header_is_lowercase_in_http2() {
    use axum::body::Body;
    use hyper::Request;
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let addr = spawn_stalled_server().await;

    // Connect via HTTP/2 prior knowledge (h2c) — hyper sends the connection
    // preface automatically; axum/hyper-util auto-detects it.
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/2 handshake failed");
    tokio::spawn(conn);

    let req = Request::post(format!("http://{addr}{SUBMIT_PATH}"))
        .header("content-type", "application/json")
        .header("content-length", SUBMIT_BODY.len().to_string())
        .body(Body::from(SUBMIT_BODY))
        .unwrap();

    let resp = sender.send_request(req).await.expect("request failed");

    assert_eq!(
        resp.status(),
        axum::http::StatusCode::ACCEPTED,
        "expected 202 Accepted"
    );

    // `HeaderName::as_str()` returns the raw stored bytes, which are always
    // lowercase in the `http` crate.  Iterating the keys (rather than doing
    // a case-insensitive lookup) confirms the casing coming off the wire.
    let location_key = resp.headers().keys().find(|k| k.as_str() == "location");
    assert!(
        location_key.is_some(),
        "location header must be present and lowercase in HTTP/2"
    );
}
