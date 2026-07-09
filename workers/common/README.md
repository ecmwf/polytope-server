<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# workers/common

Shared worker runtime for the `polytope-server` workspace.

This crate owns:

- polling `/work`
- periodic `/heartbeat/{job_id}`
- streamed success uploads to `/complete/data/{job_id}`
- JSON control uploads for reject/error/redirect outcomes

`job_id` is an opaque request ID supplied by BITS. The common runtime should preserve it byte-for-byte in protocol calls and must not infer broker ownership or chronological order from the string.

The worker-specific crates depend on this crate; the frontend does not.
