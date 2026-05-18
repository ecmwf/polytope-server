use polytope_observability::test_helper::capturing_subscriber;
use tracing_subscriber::prelude::*;

#[test]
fn worker_events_have_required_fields_and_redact() {
    let (layer, logs) = capturing_subscriber("polytope-worker-test");
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    tracing::info!(
        "event.name" = "worker.job.started",
        outcome = "success",
        job.id = "job-1",
        "enduser.id" = "alice",
        "enduser.realm" = "ecmwf",
        "job started"
    );
    tracing::debug!(
        "event.name" = "worker.delivery.completed",
        outcome = "success",
        job.id = "job-1",
        bobs.key = "key",
        read_url = "https://user:pass@example.com/result",
        "delivery completed"
    );

    logs.assert_required_fields();
    let started = logs.assert_event_emitted("worker.job.started");
    assert_eq!(started["attributes"]["enduser.id"], "alice");
    assert_eq!(started["attributes"]["enduser.realm"], "ecmwf");
    assert!(started["attributes"].get("polytope.request").is_none());
    logs.assert_event_emitted("worker.delivery.completed");
    logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
    logs.assert_no_substring("user:pass");
}
