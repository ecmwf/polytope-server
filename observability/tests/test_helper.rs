use polytope_observability::test_helper::capturing_subscriber;
use tracing_subscriber::prelude::*;

#[test]
fn captured_logs_are_valid_json() {
    let (layer, logs) = capturing_subscriber("svc");
    let sub = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(sub);
    tracing::warn!(
        "event.name" = "startup.config.failed",
        error = "Bearer FAKETOKEN_OBSERVABILITY_PROBE",
        "failed"
    );
    logs.assert_required_fields();
    logs.assert_event_emitted("startup.config.failed");
    logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
}
