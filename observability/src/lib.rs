// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

mod active;
mod env_filter;
mod formatter;
mod redaction;
mod resource;
#[cfg(unix)]
mod stdio;
pub mod test_helper;

pub use active::{ActiveRequest, active_request};
pub use env_filter::env_filter_from_env;
pub use formatter::{
    DEFAULT_LOG_LIST_PREVIEW_LENGTH, DEFAULT_LOG_MAX_LIST_LENGTH, DEFAULT_LOG_MAX_STRING_LENGTH,
    JsonFieldFormatter, OtelJsonFormatter, bounded_request, bounded_request_with, request,
};
pub use resource::service_version;
#[cfg(unix)]
pub use stdio::redirect_native_stdio;
pub use test_helper::capturing_subscriber;

use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::prelude::*;

/// Installs the global JSON logger and a panic hook that logs panics as
/// events. On unix this first redirects fd 1/2 (see [`redirect_native_stdio`]),
/// so anything native code or Python writes there becomes a log event instead
/// of corrupting the JSON log stream. Call this first thing in `main`, before
/// anything else writes to stdout/stderr.
pub fn init_tracing(service_name: &'static str) {
    let (writer, redirect_error) = stdout_writer();
    let subscriber = tracing_subscriber::registry()
        .with(env_filter::env_filter_from_env())
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(formatter::OtelJsonFormatter::new(service_name))
                .fmt_fields(formatter::JsonFieldFormatter)
                .with_writer(writer)
                // A failed log write would otherwise be reported on stderr,
                // which is itself captured and logged.
                .log_internal_errors(false)
                .with_ansi(false),
        );
    tracing::subscriber::set_global_default(subscriber).expect("install tracing subscriber");
    std::panic::set_hook(Box::new(log_panic));
    if let Some(err) = redirect_error {
        tracing::warn!("event.name" = "process.stdio_redirect.failed", outcome = "error", error = %err, "could not redirect native stdout/stderr");
    }
}

fn log_panic(info: &std::panic::PanicHookInfo<'_>) {
    let payload = info.payload();
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("Box<dyn Any>");
    let location = info
        .location()
        .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()));
    let backtrace = std::backtrace::Backtrace::capture();
    let backtrace = (backtrace.status() == std::backtrace::BacktraceStatus::Captured)
        .then(|| backtrace.to_string());
    tracing::error!(
        "event.name" = "process.panic",
        thread = std::thread::current().name().unwrap_or("<unnamed>"),
        "code.location" = location.as_deref(),
        backtrace = backtrace.as_deref(),
        request.id = active::sole_active_request().as_deref(),
        "panic: {message}"
    );
}

#[cfg(unix)]
fn stdout_writer() -> (BoxMakeWriter, Option<std::io::Error>) {
    match stdio::redirect_native_stdio() {
        Ok(stdout) => (BoxMakeWriter::new(std::sync::Mutex::new(stdout)), None),
        Err(err) => (BoxMakeWriter::new(std::io::stdout), Some(err)),
    }
}

#[cfg(not(unix))]
fn stdout_writer() -> (BoxMakeWriter, Option<std::io::Error>) {
    (BoxMakeWriter::new(std::io::stdout), None)
}

pub fn init_tracing_with_writer<W>(
    service_name: &'static str,
    writer: W,
) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'a> tracing_subscriber::fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::registry()
        .with(env_filter::env_filter_from_env())
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(formatter::OtelJsonFormatter::new(service_name))
                .fmt_fields(formatter::JsonFieldFormatter)
                .with_writer(writer)
                .with_ansi(false),
        )
}
