// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

mod env_filter;
mod formatter;
mod redaction;
mod resource;
pub mod test_helper;

pub use env_filter::env_filter_from_env;
pub use formatter::{
    DEFAULT_LOG_LIST_PREVIEW_LENGTH, DEFAULT_LOG_MAX_LIST_LENGTH, DEFAULT_LOG_MAX_STRING_LENGTH,
    JsonFieldFormatter, OtelJsonFormatter, bounded_request, bounded_request_with, request,
};
pub use test_helper::capturing_subscriber;

use tracing_subscriber::prelude::*;

pub fn init_tracing(service_name: &'static str) {
    let subscriber = tracing_subscriber::registry()
        .with(env_filter::env_filter_from_env())
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(formatter::OtelJsonFormatter::new(service_name))
                .fmt_fields(formatter::JsonFieldFormatter)
                .with_ansi(false),
        );
    tracing::subscriber::set_global_default(subscriber).expect("install tracing subscriber");
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
