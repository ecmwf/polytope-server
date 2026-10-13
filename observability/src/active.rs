// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Tracks which requests a process is working on, so output that has no request
//! context of its own (native stdout/stderr, panics) can be attributed.

use std::sync::Mutex;

static ACTIVE: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn active() -> std::sync::MutexGuard<'static, Vec<String>> {
    ACTIVE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Keeps `request_id` registered as in progress until dropped.
#[must_use = "the request is only registered while the scope is alive"]
pub struct ActiveRequest(String);

/// Registers `request_id` as being processed by this process.
pub fn active_request(request_id: &str) -> ActiveRequest {
    active().push(request_id.to_string());
    ActiveRequest(request_id.to_string())
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        let mut ids = active();
        if let Some(index) = ids.iter().position(|id| *id == self.0) {
            ids.swap_remove(index);
        }
    }
}

/// The request id, if exactly one request is in progress. With several, the
/// origin of process-wide output is ambiguous, so none is claimed.
pub(crate) fn sole_active_request() -> Option<String> {
    match active().as_slice() {
        [only] => Some(only.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Single test: the registry is process-global.
    #[test]
    fn attributes_only_when_exactly_one_request_is_active() {
        assert_eq!(sole_active_request(), None);
        let first = active_request("a");
        assert_eq!(sole_active_request().as_deref(), Some("a"));
        let second = active_request("b");
        assert_eq!(sole_active_request(), None);
        drop(first);
        assert_eq!(sole_active_request().as_deref(), Some("b"));
        drop(second);
        assert_eq!(sole_active_request(), None);
    }
}
