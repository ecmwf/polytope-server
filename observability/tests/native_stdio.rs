// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Redirects the process-wide fd 1/2, so this lives in its own test binary
//! and is a single test.

#![cfg(unix)]

use std::io::Write;
use std::time::{Duration, Instant};

use polytope_observability::{active_request, capturing_subscriber, redirect_native_stdio};
use serde_json::Value;
use tracing_subscriber::prelude::*;

fn write_fd(fd: i32, text: &str) {
    let written = unsafe { libc::write(fd, text.as_ptr().cast(), text.len()) };
    assert_eq!(written, text.len() as isize);
}

fn wait_for_events(
    logs: &polytope_observability::test_helper::CapturedLogs,
    count: usize,
) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = logs.json_lines();
        if events.len() >= count || Instant::now() > deadline {
            return events;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

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

#[test]
fn native_stdout_and_stderr_become_log_events() {
    let _restore = unsafe { RestoreFds(libc::dup(1), libc::dup(2)) };
    let mut original_stdout = redirect_native_stdio().expect("redirect stdio");

    let (layer, logs) = capturing_subscriber("svc");
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer)).unwrap();

    // What C/C++ code and Python warnings do: write to fd 1/2 directly.
    write_fd(
        2,
        "Socket read failed (TCPClient[port=0]\nTCPSocket[fd=15]\n",
    );
    write_fd(1, "Starting 7 threads\n");

    let events = wait_for_events(&logs, 2);
    assert_eq!(events.len(), 2, "got: {events:#?}");

    let stderr_event = events
        .iter()
        .find(|e| e["attributes"]["log.iostream"] == "stderr")
        .expect("stderr event");
    assert_eq!(stderr_event["severityText"], "WARN");
    assert_eq!(
        stderr_event["attributes"]["event.name"],
        "process.native_output"
    );
    // The text is the body, and a multi-line burst is one event.
    assert_eq!(
        stderr_event["body"],
        "Socket read failed (TCPClient[port=0]\nTCPSocket[fd=15]"
    );
    assert!(stderr_event["attributes"].get("request.id").is_none());

    let stdout_event = events
        .iter()
        .find(|e| e["attributes"]["log.iostream"] == "stdout")
        .expect("stdout event");
    assert_eq!(stdout_event["severityText"], "INFO");
    assert_eq!(stdout_event["body"], "Starting 7 threads");

    // Output while exactly one request is in progress is attributed to it; it
    // is never folded into the batch of another request.
    write_fd(2, "before\n");
    wait_for_events(&logs, 3);
    let request = active_request("job-1");
    write_fd(2, "during\n");
    wait_for_events(&logs, 4);
    drop(request);
    write_fd(2, "after\n");
    let events = wait_for_events(&logs, 5);
    assert_eq!(events.len(), 5, "got: {events:#?}");
    let attributed: Vec<_> = events[2..]
        .iter()
        .map(|e| {
            (
                e["body"].as_str().unwrap(),
                e["attributes"]["request.id"].as_str(),
            )
        })
        .collect();
    assert_eq!(
        attributed,
        [("before", None), ("during", Some("job-1")), ("after", None)]
    );

    // Bursts bigger than the batch cap are split on line boundaries.
    let line = format!("{}\n", "y".repeat(99));
    write_fd(2, &line.repeat(200));
    wait_for_events(&logs, 6);
    std::thread::sleep(Duration::from_millis(300));
    let events = logs.json_lines();
    let chunks = &events[5..];
    assert!(chunks.len() >= 2, "got: {chunks:#?}");
    for chunk in chunks {
        let body = chunk["body"].as_str().unwrap();
        assert!(body.len() <= 8 * 1024 && body.lines().all(|l| l.len() == 99));
    }

    // Native text goes through the same redaction as any other log body.
    let seen = logs.json_lines().len();
    write_fd(1, "request failed, Authorization: Basic dXNlcjpwYXNz\n");
    let events = wait_for_events(&logs, seen + 1);
    assert_eq!(
        events.last().unwrap()["body"],
        "request failed, Authorization: [REDACTED]"
    );

    // The returned handle is the real stdout, so JSON logs still have somewhere to go
    // and are not fed back through the pipe.
    let events_before = logs.json_lines().len();
    writeln!(original_stdout, "{{\"direct\":true}}").unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(logs.json_lines().len(), events_before);
}
