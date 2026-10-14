// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! The two requests from the "Malformed OTEL Log Events Break Field
//! Extraction" report through real metkit, configured as in the frontend image
//! (eckit's exception printing silenced) with `polytope-observability`
//! capturing fd 1/2. See `native_output_unsilenced.rs` for the default.
//!
//! Redirects the process-wide fd 1/2, so this is its own test binary and a
//! single test.

mod common;

use std::time::Duration;

use common::*;
use serde_json::json;

#[test]
fn reported_requests_succeed_and_failures_are_errors_not_stray_output() {
    unsafe { std::env::set_var("ECKIT_EXCEPTION_IS_SILENT", "1") };
    let (_restore, logs) = capture_native_output();

    let expanded = metkit::expand_json(&example_1_range_probe()).expect("example 1 expands");
    assert_eq!(expanded["date"], "20260927/20260927");
    let events = wait_for_events(&logs, 1);
    assert_eq!(events.len(), 1, "got: {events:#?}");
    assert_eq!(events[0]["severityText"], "WARN");
    assert_eq!(
        events[0]["body"],
        "Duplicate values in date [20260927,20260927] 20260927"
    );
    assert_eq!(
        events[0]["attributes"]["event.name"],
        "process.native_output"
    );
    assert_eq!(events[0]["attributes"]["log.iostream"], "stderr");

    // metkit catches an eckit exception to end the list; nothing is printed.
    let expanded = metkit::expand_json(&example_2_request()).expect("example 2 expands");
    assert_eq!(expanded["date"], "20261002");
    assert_eq!(expanded["time"], "0000/0600/1200/1800");

    // A real failure comes back as an error carrying the message.
    let mut invalid = example_2_request();
    invalid["class"] = json!("not-a-class");
    let err = metkit::expand_json(&invalid).expect_err("invalid class is rejected");
    assert!(err.to_string().contains("not-a-class"), "{err}");

    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(logs.json_lines().len(), 1, "got: {:#?}", logs.json_lines());
}
