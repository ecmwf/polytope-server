// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! eckit/metkit print diagnostics straight to stdout/stderr ("Exception: ..."
//! from an `eckit::Exception` constructor, `std::cerr` "Duplicate values ...").
//! In a service that text used to land right after the JSON log lines and
//! break the collector's JSON parsing; `polytope-observability` now turns it
//! into log events. These tests run the two requests from the "Malformed OTEL
//! Log Events Break Field Extraction" report through real metkit with that
//! capture active.
//!
//! Redirects the process-wide fd 1/2, so this is its own test binary and a
//! single test.

use std::time::{Duration, Instant};

use polytope_observability::test_helper::CapturedLogs;
use polytope_observability::{capturing_subscriber, redirect_native_stdio};
use serde_json::{json, Value};
use tracing_subscriber::prelude::*;

/// Puts fd 1/2 back so the test harness can print its results.
struct RestoreFds(i32, i32);

impl Drop for RestoreFds {
    fn drop(&mut self) {
        unsafe {
            libc::dup2(self.0, 1);
            libc::dup2(self.1, 2);
        }
    }
}

fn wait_for_events(logs: &CapturedLogs, count: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = logs.json_lines();
        if events.len() >= count || Instant::now() > deadline {
            return events;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

// Example 1: `date=20260927/to/20260927`. The frontend used to probe the range
// endpoints as `[start, end]`, for which metkit warns on stderr.
fn example_1_range_probe() -> Value {
    json!({
        "verb": "retrieve",
        "class": "d1",
        "dataset": "extremes-dt",
        "date": ["20260927", "20260927"],
        "expver": "0001",
        "levtype": "sfc",
        "param": "167",
        "step": "0",
        "stream": "oper",
        "time": "0000",
        "type": "fc",
    })
}

// Example 2: `time=0000/to/2300`. metkit ends the range by catching an
// eckit::Exception for 24:00, which eckit prints on construction, yet the
// expansion succeeds.
fn example_2_request() -> Value {
    json!({
        "verb": "retrieve",
        "class": "d1",
        "dataset": "extremes-dt",
        "date": "20261002",
        "expver": "0001",
        "levtype": "sfc",
        "param": "165/167",
        "step": "1",
        "stream": "oper",
        "time": "0000/to/2300",
        "type": "fc",
    })
}

#[test]
fn reported_requests_succeed_and_their_output_becomes_log_events() {
    let _restore = unsafe { RestoreFds(libc::dup(1), libc::dup(2)) };
    let _original_stdout = redirect_native_stdio().expect("redirect stdio");
    let (layer, logs) = capturing_subscriber("svc");
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer)).unwrap();

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

    let expanded = metkit::expand_json(&example_2_request()).expect("example 2 expands");
    assert_eq!(expanded["date"], "20261002");
    assert_eq!(expanded["time"], "0000/0600/1200/1800");
    let events = wait_for_events(&logs, 2);
    assert_eq!(events.len(), 2, "got: {events:#?}");
    // A caught exception: only DEBUG.
    assert_eq!(events[1]["severityText"], "DEBUG");
    assert_eq!(
        events[1]["body"],
        "Exception: Bad value: Wrong input for time: 24 hours 0 minutes 0 seconds"
    );
}
