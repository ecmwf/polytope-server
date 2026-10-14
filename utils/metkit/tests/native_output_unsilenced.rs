// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Control for `native_output.rs`: without `ECKIT_EXCEPTION_IS_SILENT`, eckit
//! prints the exception metkit catches to end a `time=a/to/b` list.
//!
//! Redirects the process-wide fd 1/2, so this is its own test binary and a
//! single test.

mod common;

use common::*;

#[test]
fn caught_eckit_exception_is_printed_by_default() {
    let (_restore, logs) = capture_native_output();

    metkit::expand_json(&example_2_request()).expect("example 2 expands");
    let events = wait_for_events(&logs, 1);
    assert_eq!(events.len(), 1, "got: {events:#?}");
    assert_eq!(events[0]["severityText"], "INFO");
    assert!(
        events[0]["body"]
            .as_str()
            .unwrap()
            .starts_with("Exception: Bad value: Wrong input for time: 24 hours"),
        "got: {events:#?}"
    );
}
