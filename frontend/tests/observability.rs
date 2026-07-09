// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use axum::{Router, routing::get};
use polytope_observability::test_helper::capturing_subscriber;
use polytope_server::{api, state::AppState};
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

fn test_state() -> Arc<AppState> {
    let yaml = r#"bits:
  site: tst
  env: tst
targets:
  t:
    type: http
    url: "http://127.0.0.1:0/""#;
    Arc::new(AppState {
        bits: bits::Bits::from_config(yaml).unwrap(),
        auth_client: None,
        collections: HashMap::new(),
        allow_anonymous: true,
        admin_bypass_roles: None,
        support: Default::default(),
    })
}

#[tokio::test]
async fn frontend_collection_event_has_required_fields() {
    let (layer, logs) = capturing_subscriber("polytope-frontend");
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let app = Router::new()
        .route("/api/v2/collections", get(api::v2::list_collections))
        .with_state(test_state());

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/collections")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    logs.assert_required_fields();
    logs.assert_event_emitted("api.collection.list");
    logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
}

#[test]
fn api_job_events_carry_enduser_and_bounded_request() {
    let (layer, logs) = capturing_subscriber("polytope-frontend");
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);
    let request =
        serde_json::json!({"long": "x".repeat(1100), "items": (0..101).collect::<Vec<_>>()});

    tracing::info!(
        "event.name" = "api.job.submitted",
        outcome = "success",
        request.id = "job-1",
        "enduser.id" = "alice",
        "enduser.realm" = "ecmwf",
        polytope.request = %polytope_observability::request(&request),
        "job submitted"
    );
    tracing::info!(
        "event.name" = "api.job.poll.completed",
        outcome = "success",
        request.id = "job-1",
        "enduser.id" = "alice",
        "enduser.realm" = "ecmwf",
        "job poll completed"
    );

    let submitted = logs.assert_event_emitted("api.job.submitted");
    assert_eq!(submitted["attributes"]["enduser.id"], "alice");
    assert_eq!(submitted["attributes"]["enduser.realm"], "ecmwf");
    assert!(
        submitted["attributes"]["polytope.request"]["long"]
            .as_str()
            .unwrap()
            .ends_with("...<truncated>")
    );
    assert_eq!(
        submitted["attributes"]["polytope.request"]["items"]["_summary"],
        "list"
    );

    let completed = logs.assert_event_emitted("api.job.poll.completed");
    assert_eq!(completed["attributes"]["enduser.id"], "alice");
    assert_eq!(completed["attributes"]["enduser.realm"], "ecmwf");
}

#[test]
fn frontend_redacts_bearer_probe() {
    let (layer, logs) = capturing_subscriber("polytope-frontend");
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::warn!(
        "event.name" = "api.auth.rejected",
        authorization = "Bearer FAKETOKEN_OBSERVABILITY_PROBE",
        outcome = "rejected",
        "auth failed Bearer FAKETOKEN_OBSERVABILITY_PROBE"
    );
    logs.assert_required_fields();
    logs.assert_event_emitted("api.auth.rejected");
    logs.assert_no_substring("Bearer FAKETOKEN_OBSERVABILITY_PROBE");
    logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
}
