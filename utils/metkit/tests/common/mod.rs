// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::time::{Duration, Instant};

use polytope_observability::test_helper::CapturedLogs;
use polytope_observability::{capturing_subscriber, redirect_native_stdio};
use serde_json::{json, Value};
use tracing_subscriber::prelude::*;

/// Puts fd 1/2 back so the test harness can print its results.
pub struct RestoreFds(i32, i32);

impl Drop for RestoreFds {
    fn drop(&mut self) {
        unsafe {
            libc::dup2(self.0, 1);
            libc::dup2(self.1, 2);
        }
    }
}

pub fn wait_for_events(logs: &CapturedLogs, count: usize) -> Vec<Value> {
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
pub fn example_1_range_probe() -> Value {
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
pub fn example_2_request() -> Value {
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

/// Redirects fd 1/2 into log events and returns what was captured so far.
/// Each test binary does this once: it is process-wide.
pub fn capture_native_output() -> (RestoreFds, CapturedLogs) {
    let restore = unsafe { RestoreFds(libc::dup(1), libc::dup(2)) };
    let original_stdout = redirect_native_stdio().expect("redirect stdio");
    std::mem::forget(original_stdout);
    let (layer, logs) = capturing_subscriber("svc");
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer)).unwrap();
    (restore, logs)
}
