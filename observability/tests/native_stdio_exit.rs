// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! What a service printed right before exiting (clap errors, startup failures,
//! panics) must still show up as log events. Runs this test binary again as
//! the "service", since the outcome is only visible from outside the process.

#![cfg(unix)]

use std::process::Command;

use serde_json::Value;

const CHILD_ENV: &str = "NATIVE_STDIO_EXIT_CHILD";

fn run_child(mode: &str) -> (std::process::ExitStatus, Vec<Value>, String) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let events = stdout
        .lines()
        // libtest prints "test child ... " before the first event on that line.
        .filter_map(|l| l.find('{').map(|start| &l[start..]))
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("bad JSON {l:?}: {e}")))
        .collect();
    (
        output.status,
        events,
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Only does anything when re-run by the tests below.
#[test]
fn child() {
    let Ok(mode) = std::env::var(CHILD_ENV) else {
        return;
    };
    polytope_observability::init_tracing("svc");
    let _request = polytope_observability::active_request("job-9");
    // Logging from the main thread before exiting used to make the exit-time
    // flush abort the process.
    tracing::info!("service started");
    match mode.as_str() {
        "exit" => {
            eprintln!("Error: config missing 'fdb' section");
            println!("stdout before exit");
            std::process::exit(3);
        }
        "panic" => panic!("boom"),
        "abort" => {
            eprintln!("terminate called after throwing an instance of 'eckit::SeriousBug'");
            unsafe { libc::abort() }
        }
        "c-stdio" => {
            // Buffered by C stdio (stdout is a pipe), flushed by `exit` only
            // after the atexit handlers.
            unsafe { libc::printf(c"buffered by C stdio\n".as_ptr()) };
            std::process::exit(0);
        }
        _ => unreachable!(),
    }
}

#[test]
fn output_written_just_before_exit_is_logged() {
    let (status, events, stderr) = run_child("exit");
    assert_eq!(status.code(), Some(3));
    assert_eq!(stderr, "", "nothing should reach the real stderr");
    assert!(events.iter().any(|e| e["body"] == "service started"));
    let bodies: Vec<_> = events.iter().map(|e| e["body"].as_str().unwrap()).collect();
    assert!(
        bodies.contains(&"Error: config missing 'fdb' section"),
        "{bodies:?}"
    );
    assert!(bodies.contains(&"stdout before exit"), "{bodies:?}");
}

#[test]
fn text_printed_before_abort_reaches_the_real_stderr() {
    use std::os::unix::process::ExitStatusExt;
    let (status, _events, stderr) = run_child("abort");
    assert_eq!(status.signal(), Some(libc::SIGABRT));
    assert!(
        stderr.contains("terminate called after throwing an instance of 'eckit::SeriousBug'"),
        "{stderr:?}"
    );
}

#[test]
fn c_stdio_buffers_are_flushed_into_the_log_at_exit() {
    let (status, events, _stderr) = run_child("c-stdio");
    assert!(status.success());
    assert!(
        events.iter().any(|e| e["body"] == "buffered by C stdio"),
        "{events:#?}"
    );
}

#[test]
fn panics_are_logged_as_one_attributed_error_event() {
    let (status, events, stderr) = run_child("panic");
    assert_eq!(status.code(), Some(101));
    assert_eq!(stderr, "");
    let panics: Vec<_> = events
        .iter()
        .filter(|e| e["attributes"]["event.name"] == "process.panic")
        .collect();
    assert_eq!(panics.len(), 1, "{events:#?}");
    let panic = panics[0];
    assert_eq!(panic["severityText"], "ERROR");
    assert_eq!(panic["body"], "panic: boom");
    assert_eq!(panic["attributes"]["request.id"], "job-9");
    assert!(
        panic["attributes"]["code.location"]
            .as_str()
            .unwrap()
            .contains("native_stdio_exit.rs")
    );
    // The default hook's text must not be echoed through the capture as well.
    assert!(
        !events
            .iter()
            .any(|e| e["body"].as_str().unwrap().contains("panicked at")),
        "{events:#?}"
    );
}
