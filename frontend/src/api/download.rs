// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Helpers for shaping data-download responses.
//!
//! `Content-Disposition` gives clients (browsers, `curl -OJ`, ...) a filename
//! for the download. The base name is the polytope request id (for
//! traceability) and the extension is derived from the worker-declared content
//! type. This logic is mirrored in the two other sinks that also serve
//! downloads:
//!   - `bobs` (`src/http/mod.rs::{sanitise_filename_stem, download_extension}`)
//!     for the object-store redirect path, and
//!   - the S3 delivery sink (`workers/common/src/delivery/s3.rs`).
//! Keep the three in sync when adding new media types.

/// Map a content type to a download file extension (without the dot).
///
/// Content-type strings may carry parameters (e.g. `application/json;
/// charset=utf-8`); only the base type is considered.
pub fn extension_for(content_type: &str) -> &'static str {
    let base = content_type.split(';').next().unwrap_or_default().trim();
    match base {
        "application/x-grib" => "grib",
        "application/prs.coverage+json" => "covjson",
        _ => "bin",
    }
}

/// Restrict the filename stem to a safe, header-injection-proof character set.
/// Falls back to `data` if nothing usable remains.
fn sanitise_stem(id: &str) -> String {
    let stem: String = id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    if stem.is_empty() {
        "data".to_string()
    } else {
        stem
    }
}

/// Build a `Content-Disposition` header value that forces a download named
/// `<request-id>.<ext>`.
pub fn content_disposition_for(id: &str, content_type: &str) -> String {
    format!(
        "attachment; filename=\"{}.{}\"",
        sanitise_stem(id),
        extension_for(content_type)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_content_types() {
        assert_eq!(extension_for("application/x-grib"), "grib");
        assert_eq!(extension_for("application/prs.coverage+json"), "covjson");
        assert_eq!(extension_for("application/octet-stream"), "bin");
        assert_eq!(extension_for("something/unknown"), "bin");
        // content-type parameters are ignored; only the base type matters
        assert_eq!(
            extension_for("application/prs.coverage+json; charset=utf-8"),
            "covjson"
        );
    }

    #[test]
    fn builds_content_disposition_from_id_and_type() {
        assert_eq!(
            content_disposition_for("abc-123", "application/x-grib"),
            "attachment; filename=\"abc-123.grib\""
        );
    }

    #[test]
    fn sanitises_unsafe_ids() {
        // quotes / path separators / control chars are stripped
        assert_eq!(
            content_disposition_for("a\"b/c", "application/x-grib"),
            "attachment; filename=\"abc.grib\""
        );
        // an id with nothing usable falls back to `data`
        assert_eq!(
            content_disposition_for("///", "application/x-grib"),
            "attachment; filename=\"data.grib\""
        );
    }
}
