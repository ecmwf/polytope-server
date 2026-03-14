# workers/common

Shared worker runtime for the `polytope-server` workspace.

This crate owns:

- polling `/work`
- periodic `/heartbeat/{job_id}`
- streamed success uploads to `/complete/data/{job_id}`
- JSON control uploads for reject/error/redirect outcomes

The worker-specific crates depend on this crate; the frontend does not.
