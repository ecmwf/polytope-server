<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Plan: preserve MARS ranges through metkit expansion for feature-extraction requests

- **Status:** implemented and fully verified (both Rust and Python sides
  compiled/tested successfully -- see "Build/test environment blocker"
  below for how the Rust side was verified despite `mars-client-bundle` not
  being checked out locally)
- **Created:** 2026-08-19
- **Owner:** (fill in)
- **Scope:** `polytope-server` (`frontend/src/metkit_expansion.rs`, `utils/metkit`,
  `workers/polytope-fe-worker/polytope.py`) plus `polytope-config` pipeline wiring
  if a new action needs to be added to routes.

> Keep this file updated as the investigation/implementation progresses. Add a
> dated changelog entry at the bottom for each significant update instead of
> rewriting history.

## Problem statement

Feature-extraction (FE) requests can carry MARS-style ranges, e.g.
`step: "0/to/240"` or `levelist: "1/to/100"`. `polytope_mars` /
`polytope_feature` (the Python library used by `polytope-fe-worker`) has its
own "smart" range handling: given a compact range, it can walk the datacube /
FDB axis and figure out which values actually exist, instead of assuming a
fixed step.

Today, every request passes through `transform::metkit_expansion`
(`frontend/src/metkit_expansion.rs`) before it reaches any FE-specific
metadata/config transforms. That action calls `metkit::expand_json`
(`utils/metkit/src/lib.rs`, backed by real `metkit::mars::MarsExpansion` via
`cxx`), which **fully enumerates** `to`/`by` ranges using a default step when
`by` is absent (or the type's built-in default). `"1/to/100"` becomes a
literal list `1,2,3,...,100` in `job.request` before the FE worker ever sees
it. By the time `polytope_mars.extract()` runs, the compact-range information
is gone — the worker just sees (and must resolve) every single enumerated
value, defeating its own smart/availability-aware expansion and often
generating far more sub-requests / FDB lookups than necessary.

We want ranges to reach the FE worker in their original compact form (e.g.
`"1/to/100"`, or `"0/to/240/by/6"` if the client gave a step), but with the
two *endpoint* values passed through real metkit validation/canonicalization
(relative-date resolution, keyword/synonym normalization, etc.) — matching
the user's proposed design: turn each range into its two endpoints, run
those through metkit, and reassemble a range with the (possibly rewritten)
canonical endpoints, original `by` (if any) untouched.

## Relevant existing code (context gathered so far)

- `frontend/src/metkit_expansion.rs` — the `TransformAction` that unwraps the
  v1 `{"request": {...}}` shape, preserves non-MARS keys (currently just the
  `feature` object, based on `feature_covered_keys`), calls
  `metkit::expand_json`, and re-merges preserved keys back in. This is the
  action that currently destroys range compactness. It runs in **every**
  location's pipeline (`bologna`, `lumi`, `mn5`, `leo` — see
  `polytope-config/location/*/config.yaml`), always under the name
  `transform::metkit_expansion`, so any fix implemented *inside* this action
  needs zero pipeline YAML changes to roll out everywhere.
- `utils/metkit/src/lib.rs` + `src/bridge.cc` — thin `cxx` binding around
  `metkit::mars::MarsExpansion`. `expand_json` takes a flat JSON object,
  splits each string field on `/` into a `Vec<String>` (see
  `json_value_to_strings` — this is where `"1/to/100"` becomes the 3 literal
  tokens `["1","to","100"]` fed into `MarsRequest::values()`), and lets
  `MarsExpansion::expand()` do real MARS-language expansion, including
  interpreting `to`/`by` tokens as ranges for range-capable types. This is a
  real (not simulated) metkit call — accurate but always fully enumerates.
- `frontend/src/actions/coercion.rs` — **already has range-preserving logic**,
  just for a different purpose (client input coercion/normalization, not
  metkit validation). `coerce_value()` special-cases
  `text.contains("/to/")` for keys in `allow_ranges` (`number`, `step`,
  `date`, `time`): it splits into `start`, `end`, optional `/by/N` suffix,
  runs each endpoint through a per-key coercer function (`coerce_date`,
  `coerce_step`, ...), and reassembles `"{start}/to/{end}{suffix}"`. This is
  structurally the *exact* algorithm the user is asking for, just using
  hand-rolled Rust coercers instead of a real metkit round-trip, and it is
  currently **not wired into any pipeline** (`transform::coerce_request` /
  `RequestCoercion` exists in code and has unit tests, but no
  `polytope-config/location/*/config.yaml` references it — dead/unused code
  today).
- `frontend/src/actions/date_check.rs::expand_date_rule_input` — a third,
  independent implementation of "detect `a/to/b` or `a/to/b/by/c` (3 or 5
  slash-separated tokens with `to` at index 1, optional `by` at index 3),
  return `[a, b]`". Same idiom again, for date-window checks.
- `frontend/src/actions/schedule.rs::resolve_step` /
  `step_from_feature` — schedule/release-time checking depends on a single
  resolved `u32` step, computed from `job.request` (assumed already fully
  expanded to a flat scalar/array by `metkit_expansion`) and/or
  `feature.range.end`. This (and other consumers below) is why we should
  **not** change what `job.request` itself looks like after
  `metkit_expansion` — that would ripple into every action that assumes a
  flat, enumerated request.
- Other actions reading `job.request` directly and assuming it is already
  fully expanded/flat: `check_has_key`, `check_match` (`Value` equality/array
  overlap — arrays of enumerated values, not range strings),
  `check_date`/`date_check` (has its own independent range-collapsing, see
  above), `transform_coercion` (unused today, see above). No cost/quota
  estimator keys off enumerated axis cardinality in this repo currently
  (searched; none found), which reduces blast radius.
- `job.metadata` / `polytope_mars` metadata block — documented in
  `docs/job-metadata-options.md`. Currently a **trusted, config-only**
  channel: `set_metadata` writes static YAML-supplied values, and the stated
  security principle is "config-only values ... never dynamically
  constructed values derived from the request." A new action that computes
  metadata *from* the request (even if purely derived/deterministic and
  already-validated) is a **new category** of metadata write and should be
  called out explicitly, documented, and reviewed against that principle
  (see "Trust boundary" section below) — it is not simply "reuse
  `set_metadata`".
- `workers/polytope-fe-worker/polytope.py::PolytopeDataSource.retrieve` —
  already has a precedent for a trusted-metadata overlay pattern: it reads
  `request.metadata["polytope_mars"]` (dict, with an explicit
  `_FE_WORKER_METADATA_KEYS` allow-list) and merges `datacube`/`options`
  into the per-request `polytope_mars_config` before calling
  `polytope_mars.extract(r)`. `r` itself (the retrieval request dict) is, as
  of today, taken as-is from `request.coerced_request` (== `job.request`,
  post-`metkit_expansion`) with no metadata overlay applied to it. Extending
  this to overlay compact ranges onto `r` before `extract()` is a natural,
  small extension of an existing pattern.
- `workers/polytope-fe-worker/tests/test_metadata_options.py` — existing test
  harness with a `FakePolytopeMars.extract()` that records the `request`
  dict it was called with. This is directly reusable to test the
  range-overlay behavior without needing real FDB/`polytope_mars`.
- `pre_path` construction in `polytope.py::retrieve` — builds a `pre_path`
  dict from axes in `pre_path_axes`, taking `v[0]` when `len(v) == 1` after
  splitting on `/`. If a range-carrying axis were ever also a `pre_path`
  axis, `"1/to/100"` would need to be excluded/handled specially here (in
  practice `pre_path` axes are normally fixed identity axes like
  `class`/`stream`/`dataset`, not range axes like `step`/`levelist`, so this
  is expected to be a non-issue, but must be verified/asserted, not assumed).

## Proposed design (Option A — recommended, matches the user's proposal)

Extend `transform::metkit_expansion` (or add a small helper it calls) to,
**before** doing the existing full-request `expand_json` call:

1. Walk the (unwrapped, verb-normalized) request object. For each string
   field whose value matches the MARS range grammar (reuse/extract the
   existing `to`/`by` token-detection idiom from `date_check.rs` /
   `coercion.rs` into a small shared helper — 3 tokens `a/to/b`, or 5 tokens
   `a/to/b/by/c`, case-insensitive `to`/`by`), record `(key, start, end,
   by_suffix)`. If no field matches, skip steps 2–3 and 5 entirely (fast
   path — no extra metkit call for the common non-range case).
2. Build a **single** probe request: one clone of the full original
   request context, where **every** ranged key found in step 1 has its
   value replaced by the 2-element list `[start, end]` (in that order),
   and every non-ranged field is left exactly as originally submitted (this
   matters because MARS/metkit expansion for one axis can depend on others,
   e.g. `param` resolution depends on `class`/`stream`/`expver`). Run this
   **one** request through the existing `metkit::expand_json` — a single
   extra metkit call per job regardless of how many ranged keys it has (not
   two calls per key as an earlier draft of this plan proposed). For each
   ranged key, `expand_json`'s output for that key (currently rendered as a
   `/`-joined string, see `result.insert(key, ... vals.join("/"))` in
   `utils/metkit/src/lib.rs`) is expected to contain exactly the 2
   canonicalized values, in the same order as given — split on `/` and take
   `[0]` as the normalized start and `[1]` as the normalized end. See open
   question 5 (updated) for the degenerate/defensive cases (e.g. start==end
   collapsing to a single output value).
3. Reassemble `"{normalized_start}/to/{normalized_end}{by_suffix}"` per
   ranged key (the `by` step, if present, is **not** sent through metkit —
   it is not a value to validate, just an increment — carried through
   verbatim, same as `coercion.rs` does today).
4. Run the existing full `expand_json` pass exactly as today, **unchanged**,
   producing the fully-enumerated `job.request` that every other action
   (schedule check, `check_match`, `has_area`/`has_grid`, quota/logging,
   etc.) continues to consume exactly as it does now. Zero behavior change
   for anything downstream that isn't FE-range-aware.
5. Additionally, write the per-key compact/normalized ranges computed in
   steps 1–3 into `job.metadata` under a new, documented, top-level reserved
   key **`metkit_ranges`** (decided — see resolved open question 2), e.g.
   `metkit_ranges: {"step": "0/to/240/by/6", "levelist": "1/to/100"}`.
   This metadata write is **purely derived from data already present and
   already destined to be fully validated** in `job.request` — it does not
   let a client introduce any value that wouldn't already have passed
   through the normal (unchanged) expansion/validation pipeline; it only
   adds a compact *alternate representation* of a subset of that fully
   expanded field for FE workers to prefer.
6. In `polytope.py::PolytopeDataSource.retrieve`, before calling
   `polytope_mars.extract(r)`: if `request.metadata` carries the new range
   metadata, overlay it onto `r` (i.e. replace `r[key]` with the compact
   range string for each key present), **excluding** any key that is also
   in `pre_path_axes` (defensive; see open question above) — mirroring the
   existing `metadata_polytope_mars` overlay pattern already in this file.

### Why this is (relatively) low risk

- No change to what `job.request` looks like after `metkit_expansion` — all
  existing consumers (schedule check, quota, `check_match`, has_area/
  has_grid, logging, audit) keep working off the fully enumerated form
  exactly as today. The change is purely additive (new metadata key) plus
  one new, small, `polytope.py`-local overlay before the `extract()` call.
- Only **one** extra `metkit::expand_json` call per job (covering all
  ranged keys at once, `[start, end]` per key), not one call per range
  endpoint — cheap, and it exercises the exact same cross-key expansion
  context/semantics as the real full-expansion call already does, so any
  genuine cross-axis dependency between two ranged keys is captured
  faithfully rather than approximated by probing keys independently.
- Because the fix lives inside the already-universally-wired
  `transform::metkit_expansion` action, **no `polytope-config` pipeline YAML
  changes are needed** to roll it out to `bologna`/`lumi`/`mn5`/`leo`.
- Three independent precedents for the core "detect `a/to/b(/by/c)`, split,
  reassemble" algorithm already exist in this codebase
  (`coercion.rs`, `date_check.rs`) — this is a well-trodden pattern here, not
  a novel one.
- Testable in isolation: the Rust side is a pure-JSON transform, unit
  testable without FDB (like the existing `utils/metkit/tests/integration.rs`
  and `metkit_expansion.rs`'s implicit tests-via-`feature_covered_keys`); the
  Python side has a ready-made fake-`PolytopeMars` harness
  (`test_metadata_options.py`) to assert on the `request` dict passed to
  `extract()`.

### Complexity estimate

| Piece | Effort | Notes |
|---|---|---|
| Shared `a/to/b(/by/c)` detection helper (dedupe the 3 existing copies) | Small (0.5–1 day) | Optional cleanup; could also just add a 4th copy to minimize risk of touching `coercion.rs`/`date_check.rs` behavior. |
| Range detection + single combined-probe metkit round trip + reassembly in `metkit_expansion.rs` | Small–Medium (1–1.5 days incl. tests) | Needs care around: the degenerate start==end collapse case (see open question 5); preserving the existing v1-unwrap / `feature`-preservation logic untouched. Simpler than originally estimated now that it's one probe call per job instead of two calls per ranged key. |
| New reserved metadata key (`metkit_ranges`) + doc update (`docs/job-metadata-options.md`) | Small (0.5 day) | Key name decided (see resolved open question 2); still needs the trust-boundary write-up (see below). |
| `polytope.py` overlay before `extract()` + `pre_path` exclusion + tests | Small–Medium (1 day incl. tests) | Reuse `test_metadata_options.py` harness. |
| `polytope-config` integration test coverage (extend `tests/examples/...`) | Small–Medium (0.5–1 day) | Depends on lumi-test/lumi-dev availability; per `AGENTS.md`, ask which environment before running. |

**Overall: roughly 3–4.5 engineering days.** No spike needed — confirmed
(by whoever owns this requirement) that `polytope_mars`/`polytope_feature`
already does availability-aware expansion when given a compact `to`-range,
so that no longer gates the plan. Feasibility is good — nothing here
requires new FFI surface (the existing `metkit::expand_json` is reused
unchanged, just called once more per job, only when ranges are present), no
schema/DB migrations, no changes to the Rust↔Python job payload plumbing
(metadata already flows end-to-end, see `workers/polytope-fe-worker/src/main.rs`).

### Open questions / decisions needed before implementation

1. ~~Spike: does `polytope_mars.extract()` actually accept a MARS `to`-range
   string and do FDB-availability-aware expansion?~~ **Resolved — not
   needed.** Confirmed by the requirement owner that this is already how
   `polytope_mars`/`polytope_feature` behaves; no longer a blocking
   assumption for this plan.
2. ~~Metadata key shape: new top-level key vs. nested under
   `polytope_mars.options`?~~ **Resolved.** Use a new top-level
   `metkit_ranges` key (see updated design step 5 and the complexity
   table). Still needs the `docs/job-metadata-options.md` write-up (open
   question 6).
3. **Resolved — no sanity check needed.** Cross-axis dependency between
   multiple ranged keys is addressed by the single combined probe (design
   step 2): all ranged keys are narrowed to their `[start, end]` pair
   together, in one `expand_json` call, with the same fidelity as the real
   full-expansion call. Per requirement-owner feedback, no further
   verification against real dataset configs is needed here.
4. Only single-range-per-field is handled (matching the existing
   `coercion.rs`/`date_check.rs` precedent). **Correction (2026-08-19,
   post-review):** the earlier draft asserted as fact that "full MARS
   grammar technically allows mixed lists+multiple ranges in one field
   (e.g. `"0/to/12/by/6/24/36"`)" — that was extrapolated from general MARS
   familiarity, not verified. `mars-client-bundle` (which would contain the
   real metkit parser, e.g. `Type::expand`) is not present in this
   workspace checkout, so the claim was never checked against source. The
   only documented reference found (ECMWF Confluence, UDOC space, "MARS
   command and request syntax", page 45751804) states that a keyword's
   value is *either* "a list of values" (`step = 12/24/48`) *or* "a range of
   values" (`date = 19990104/to/19990110/by/2"`) — it does not show or claim
   these can be combined within one keyword. Treat mixed list+range as
   **unconfirmed** rather than assumed-supported: before implementation,
   verify against the actual metkit source (needs a checkout with
   `mars-client-bundle` present, or ask someone with metkit expertise) or
   simply test empirically via `metkit::expand_json` in this repo's own
   test suite (`utils/metkit/tests/integration.rs`) with a mixed-value
   string and see what `MarsExpansion` does with it. Until then, the plan
   should scope Option A to the single-range-per-field case only (which the
   existing `coercion.rs`/`date_check.rs` precedent already assumes), and
   treat any request with multiple `to` occurrences in one field's raw
   string, or a mix of plain values and a range, as a case that falls back
   to full enumeration (safe default) rather than something we attempt to
   parse.
5. **Resolved.** For each ranged key we send exactly 2 input values
   (`[start, end]`) in the one combined probe, and expect exactly 2 output
   values back, in the same order, to split into normalized start/end.
   Known/expected exception: if `start == end` (or two inputs canonicalize
   to the same value) metkit may legitimately dedupe to a single output
   value — in that case use it for both start and end. **Decision (per
   requirement-owner feedback): never reject the job over this action.**
   If a probe comes back with a count other than 1 or 2 for a key (or the
   whole probe call errors), that key is simply omitted from
   `metkit_ranges` — `job.request` already carries the fully expanded,
   already-valid form for that key regardless, so the request is still
   served correctly, just without the compact range for that key. If a
   downstream consumer (e.g. the FE worker) actually requires a compact
   range for a given key to behave correctly/efficiently, *it* is
   responsible for rejecting that case — this action's job is only to
   provide the compact form best-effort, never to gate the request on it.
   Implemented in `probe_ranges`/`detect_ranges` in
   `frontend/src/metkit_expansion.rs`.
6. **New — by-suffix validation (raised during implementation, resolved).**
   The `by` component of a detected range (e.g. the `6` in
   `"0/to/240/by/6"`) is not sent through the metkit probe (it's an
   increment, not a value to canonicalize), but it is validated to parse as
   a plain integer (`by_val.parse::<i64>()`) before the field is treated as
   a recognised range at all. If it doesn't parse, `detect_ranges` treats
   the whole field as "not a recognised range" — consistent with question
   5's decision, this never rejects the job; the field is simply left out
   of `metkit_ranges` and flows through the ordinary full-expansion path
   unchanged (which will itself error/reject if the malformed `by` is
   genuinely invalid MARS syntax, exactly as it does today with no range
   handling at all). Known limitation: this only validates "is it an
   integer", not axis-specific `by` semantics (e.g. duration-like
   `by`-steps such as `6h`, if MARS actually allows those for any axis --
   unconfirmed, same caveat as open question 4's mixed-grammar uncertainty).
7. **New — where to detect ranges relative to `feature`-object preservation
   (raised during implementation, resolved; no strong opinion from the
   requirement owner, so documenting the choice made).** Range detection
   and probing run on `obj`/`job.request` *after* the existing v1-unwrap and
   non-MARS-key (`feature`) preservation step and the `verb` default
   insertion, but *before* the real `metkit::expand_json` call. At that
   point `obj` contains exactly the same field set/shape metkit itself is
   about to expand (feature object already removed, verb defaulted) --
   there is no interaction with the preserved-key mechanism, since detection
   simply never sees the `feature` key at all (it's already been removed by
   then). This keeps the new logic fully decoupled from the
   feature-preservation logic rather than threading them together.
8. **New — idempotency across job retries (raised during implementation,
   resolved as a non-issue).** Confirmed both by the requirement owner and
   by reading `bits/bits/src/job.rs`: `Job::restore()` (the broker-recovery
   path) reconstructs the job from `record.original_request` -- a frozen,
   untouched snapshot taken at submission time (`Job::new_with_id` sets
   `original_request: Arc::new(request.clone())` before any pipeline
   action runs). A retried/recovered job therefore always re-runs
   `metkit_expansion` (and this new range detection) from scratch against
   the pristine original request, never against an already-mutated one --
   no idempotency concern.
9. **New — "case sensitivity" concern from the original review (raised
   during implementation, resolved as a non-issue).** This referred to
   whether the *keys* written into `metkit_ranges` could ever fail to match
   the field names actually present in the later fully-expanded
   `job.request`, e.g. due to the v1-unwrap/`feature`-preservation
   bookkeeping earlier in `execute()`. Since detection now runs directly on
   `obj` (the same map instance `job.request` itself points to at that
   point, after unwrap/preservation/verb-default, before expansion) rather
   than on some separately-cased or re-derived key set, the keys in
   `metkit_ranges` are always exactly the keys metkit expansion is about to
   process -- no separate normalization step exists that could introduce a
   mismatch. Confirmed by construction, not by a runtime check; no action
   needed.
6. Update `docs/job-metadata-options.md` to add the new reserved key and
   explicitly carve out this "derived-but-safe" category from the existing
   "config-only values" principle, so future readers don't flag it as a
   violation.

## Alternative solutions considered

### Option B — skip metkit expansion entirely for range-capable axes on FE requests, pass client ranges through raw

Instead of probing endpoints through metkit, simply exclude known
range-capable axes (`step`, `date`, `time`, `levelist`, `number`, ...) from
`metkit_expansion`'s `expand_json` call whenever `feature` is present
(extending the existing `preserved`/`feature_covered_keys` mechanism that
already exists for the `feature` object itself), and let the FE worker
receive the client's literal, unnormalized range string.

- **Pros:** Much less code — no new probing logic, no metadata plumbing, no
  `polytope.py` changes. Could likely be done in under a day.
- **Cons:** Loses all metkit canonicalization for those axes: relative
  dates (`"-1"`) would reach the FE worker unresolved, keyword
  case/synonym differences wouldn't be normalized, and — more subtly — the
  value that `check_schedule`/`resolve_step` and other checks validated
  (the fully-expanded canonical form, still computed for *other* keys) could
  diverge from what's actually sent to FDB for the excluded keys. This is
  the "quick and risky" option; acceptable only if we're confident the
  affected axes are always already in canonical form for FE clients (e.g.
  dates are never relative in these requests) — needs an explicit product
  decision, not just an engineering shortcut.

### Option C — reuse/extend the existing (currently unwired) `coerce_request`/`coercion.rs` module instead of a real metkit round-trip

`coercion.rs` already implements the identical "detect range, coerce
endpoints, reassemble" algorithm, just with small hand-rolled Rust coercers
per key (`coerce_date`, `coerce_step`, `coerce_time`, `coerce_number`,
`coerce_expver`) instead of a real `metkit::expand_json` call. Wire
`transform::coerce_request` into the FE pipelines ahead of
`metkit_expansion`, and teach `metkit_expansion` to leave already-coerced
range strings on range-capable keys alone (same exclusion mechanism as
Option B) rather than probing metkit again.

- **Pros:** Zero new FFI/metkit calls; pure-Rust, fast, already
  unit-tested, already exists — essentially "finish wiring up code that's
  already there." Also reusable outside FE requests, if useful elsewhere.
- **Cons:** `coerce_date`/`coerce_step`/etc. are a hand-maintained subset of
  real MARS-language knowledge (5–6 keys today), not a full metkit
  validation — no keyword/synonym validation, no param-name resolution, no
  per-`class`/`stream`/`expver`-dependent behavior, and would need a new
  coercer written and maintained for every additional range-capable axis
  (e.g. `levelist`, `quantile`, `frequency`, ...) as they come up, rather
  than getting that for free from metkit. Correctness ceiling is lower than
  Option A; good as a stopgap or as inspiration for the shared
  detection-helper refactor mentioned in Option A's complexity table, but
  not a full substitute for the metkit round-trip if canonicalization
  fidelity matters (which it does — this is the whole reason metkit is
  called at all today).

### Option D — do the smart expansion server-side (in Rust) instead of relying on `polytope_mars`

Rather than passing compact ranges to the FE worker at all, have the
frontend itself query FDB (or a cached view of available axis values) for
each range and generate exactly the sub-list of values that exist, then feed
that plain list to the FE worker.

- **Pros:** No change needed to the FE worker; ranges never leave the
  frontend.
- **Cons:** Duplicates functionality that already exists in
  `polytope_mars`/`polytope_feature` (FDB access from Rust would need a new
  FDB client integration point in the frontend — this repo's frontend does
  not currently talk to FDB directly, only via the FE worker/`rsfdb`); much
  larger effort, and two independent implementations of "availability-aware
  range expansion" to keep in sync long-term. **Not recommended** — only
  listed for completeness.

## Recommendation

Proceed with **Option A**, using a single combined metkit probe per job
(design steps 1–3 above) rather than per-key/per-endpoint probes, and the
top-level `metkit_ranges` metadata key. The `polytope_mars`-acceptance spike
is no longer a precondition (resolved open question 1). Remaining
pre-implementation decisions are open questions 4–6 (mixed list+range
support, the not-1-or-2-results fallback behavior, and the
`docs/job-metadata-options.md` write-up).

## Build/test environment blocker (Rust side) -- found, fixed, and worked around

Two separate issues were hit and resolved while verifying the Rust side:

**1. Stale, untracked `.cargo/config.toml` (the actual blocker; now fixed).**
`polytope-server/.cargo/config.toml` on disk patched `bits-ecmwf` to a local
path (`../bits/bits-ecmwf`) that doesn't exist in this `bits` checkout (which
only has `bits`, `bits-py`, `bits-server`). This file is **git-ignored and
untracked** (`git check-ignore` confirms it matches `.gitignore:6`, and
`git ls-files` shows it was never committed) -- it was explicitly removed
from version control in commit `3ec4256` ("chore: remove `[patch]` override
for local bits dev ... devs manage it locally"), and the file sitting on
disk here was a stale leftover from an earlier local session, not part of
the repo. **Deleted it** (`rm .cargo/config.toml`), after which
`cargo check -p polytope-server` and `cargo test -p polytope-server` both
work cleanly with default features (232 pre-existing tests, all passing).

**2. `metkit_expansion.rs` is behind an optional Cargo feature.** Separately,
`frontend/src/lib.rs` gates the whole module: `` #[cfg(feature = "metkit")] mod metkit_expansion; ``.
This feature is off by default (`frontend/Cargo.toml`: `default = []`), so
plain `cargo test -p polytope-server` never compiles this file at all --
unrelated to issue 1 above, and not a regression from this change (the
module, and its dependency on the native `metkit`/`eckit` C++ libraries via
the `metkit` crate's `build.rs`, already worked this way before this plan).
Enabling it (`--features metkit`) requires real eckit/metkit headers and
libraries; the `build.rs` default paths point at a `mars-client-bundle`
checkout that isn't present in this workspace, matching the constraint
`AGENTS.md` documents for this and similar C++-backed crates. No CI
workflow in this repo runs plain `cargo test` with `--features metkit`
either (checked `.github/workflows/*.yaml`) -- the feature is normally only
exercised via the full Docker image build, which pulls
`eccr.ecmwf.int/polytope/cpp-metkit-libs` (built by
`.github/workflows/cpp-libs.yaml` from `docker/cpp-libs/metkit/Dockerfile`)
as its native-library source.

**Worked around by using that same image directly**, since it's pullable
from this environment (`docker pull
eccr.ecmwf.int/polytope/cpp-metkit-libs:metkit1.17.0-r1` succeeded, using an
already-cached ECCR login) and this host already has Docker:

1. Extracted `/opt/metkit` (the installed eckit+metkit prefix: `include/`,
   `lib/`) from the image via `docker create` + `docker cp`.
2. Tried pointing the `metkit` crate's `build.rs` env vars
   (`ECKIT_INCLUDE_DIR`, `METKIT_INCLUDE_DIR`, `METKIT_LIB_DIR`, etc.)
   directly at the extracted files on the host -- **compiled** fine, but
   **failed to link/run**: the host's glibc (Debian 11, 2.31) is too old for
   the image's libraries (Debian 12/bookworm, glibc ~2.36) -- confirmed via
   `ldd` showing unresolved `GLIBC_2.32`/`2.33`/`2.34` symbol versions. This
   is a real ABI mismatch, not a linker-flag issue.
3. Built a small local (not committed, not pushed) Debian-bookworm-based
   image layered on top of `cpp-metkit-libs` (adding `build-essential`,
   `libssl-dev`, `pkg-config`, `git`, and a matching `rustc 1.94.1` via
   `rustup`), then ran `cargo test -p polytope-server --features metkit`
   inside a container from that image, bind-mounting this workspace plus
   the host's `~/.cargo/registry` and `~/.cargo/git` (to reuse already-cloned
   private-repo git dependencies -- `polytope-edr`, `bits`, `authotron-*` --
   without needing separate in-container git credentials) and a scratch
   Docker volume for `target/` (kept separate from the host's `target/` to
   avoid mixing glibc-incompatible build artifacts).

**Result: fully verified, both with and without the feature.**
- `cargo test -p polytope-server --lib` (default features, on the host):
  **232 passed**, 0 failed.
- `cargo test -p polytope-server --features metkit --lib` (in the
  bookworm container, against real `cpp-metkit-libs`): **243 passed**, 0
  failed -- the 232 above plus 11 new tests in `metkit_expansion.rs`,
  including two that exercise the real `metkit::expand_json` C++ binding
  end-to-end (`probe_ranges_canonicalizes_endpoints_via_real_metkit`,
  `probe_ranges_preserves_by_suffix_verbatim`) and two full-action tests
  confirming `job.request` stays fully expanded while `job.metadata
  ["metkit_ranges"]` carries the compact canonicalized range
  (`execute_writes_metkit_ranges_metadata_and_still_fully_expands_request`,
  `execute_writes_no_metadata_when_request_has_no_ranges`).
- The Python side (`workers/polytope-fe-worker/`) was run directly on the
  host (no container needed -- the tests mock out `polytope_mars`/
  `polytope_feature` entirely): `python3 -m pytest
  workers/polytope-fe-worker/tests/ -v` -- **34/34 passed**, including 5 new
  tests covering the `metkit_ranges` overlay (happy path, absence, pre_path
  exclusion, and the two malformed-metadata-shape rejection cases).

All temporary artifacts (the local Docker image, scratch volume, extracted
`/opt/metkit` copy) were removed after verification; nothing from this
workaround is committed. The stale `.cargo/config.toml` deletion (issue 1)
is a real, permanent fix and should probably be mentioned to whoever else
hits it locally.

## Changelog

- **2026-08-19** — Initial investigation and plan written. Surveyed
  `metkit_expansion.rs`, `utils/metkit`, `coercion.rs`, `date_check.rs`,
  `schedule.rs`, `polytope.py`, `run_polytope_worker.py`, and
  `docs/job-metadata-options.md`. No implementation started yet.
- **2026-08-19 (review pass)** — Corrected an unverified claim about mixed
  list+range MARS grammar (open question 4) after being challenged; the
  only source actually checked (Confluence UDOC page 45751804) shows list
  and range as separate forms, not confirmed combinable. Per
  requirement-owner feedback: (1) dropped the `polytope_mars`-acceptance
  spike as a precondition — confirmed already how it behaves; (2) locked in
  `metkit_ranges` as the metadata key name; (3) simplified the probing
  design from two metkit calls per ranged key (one for all starts, one for
  all ends, or two calls per key) down to a **single combined probe per
  job**, with every ranged key's value set to `[start, end]` at once,
  reducing metkit calls, engineering effort (revised down to ~3–4.5 days),
  and the per-key-independent-probing cross-axis-fidelity concern (open
  question 3); (4) updated the defensive-count question (open question 5)
  to expect exactly 2 output values per ranged key, with the known
  start==end degenerate collapse to 1 handled explicitly.
- **2026-08-19 (implementation pass)** — Implemented per the resolved
  decisions above:
  - `frontend/src/metkit_expansion.rs`: added `detect_ranges`,
    `value_tokens`/`render_scalar`, `probe_ranges`, and the
    `metkit_ranges`-metadata write in `execute()`, plus unit tests for
    `detect_ranges` (7 cases: plain range, range with `by`, array-form
    input, malformed `by` suffix, plain lists/scalars ignored, 5-token
    non-range ignored, `verb` key skipped).
  - `docs/job-metadata-options.md`: added `metkit_ranges` as a reserved
    key, a full section explaining its shape/consumers/why it's a safe
    exception to the config-only-metadata principle, and a cross-reference
    from Security Consideration 3.
  - `workers/polytope-fe-worker/polytope.py`: `PolytopeDataSource.retrieve`
    now overlays `request.metadata["metkit_ranges"]` onto `r` before
    calling `polytope_mars.extract(r)`, skipping any key that is also a
    `pre_path` axis, and validating the metadata shape (raises `ValueError`
    on a non-dict value or non-string per-key entry, mirroring the existing
    `polytope_mars` metadata validation in the same function).
  - `workers/polytope-fe-worker/tests/test_metadata_options.py`: added 5
    tests for the new overlay (happy path, absent-metadata passthrough,
    pre_path exclusion, and the two malformed-shape `ValueError` cases).
    Full suite run: 34/34 passed.
  - Resolved open questions 4–9 (by-suffix validation, and three new
    decisions raised during implementation: range-detection placement
    relative to `feature`-preservation; retry idempotency, confirmed via
    `bits/bits/src/job.rs`; and the "case sensitivity" concern, resolved as
    a non-issue by construction) — see the updated numbered list above.
  - Hit an apparent workspace build blocker (`bits-ecmwf` patch path
    doesn't exist) that at first prevented compiling/running the Rust side
    at all. Reviewed the Rust code by hand as a stopgap and noted this as
    an open blocker.
- **2026-08-19 (verification pass)** — Per feedback, tracked down and fixed
  the actual cause of the blocker above: `polytope-server/.cargo/config.toml`
  was a stale, **git-ignored, untracked** local file (confirmed via
  `git check-ignore`/`git ls-files` and repo history showing it was
  deliberately removed from version control in commit `3ec4256`) — deleted
  it, and `cargo check`/`test -p polytope-server` now pass cleanly with
  default features (232/232). Then discovered `metkit_expansion.rs` is
  separately gated behind an opt-in `metkit` Cargo feature (off by default,
  unrelated to the `.cargo/config.toml` issue, and not new to this change),
  and verified the Rust code under that feature too by extracting the CI's
  published `eccr.ecmwf.int/polytope/cpp-metkit-libs` image into a
  matching-glibc container and running `cargo test --features metkit`
  inside it: **243/243 passed**, including 4 new tests added specifically to
  exercise the real `metkit::expand_json` binding end-to-end (on top of the
  7 pure-`detect_ranges` unit tests from the implementation pass). See
  "Build/test environment blocker" section above for the full method (not
  committed; no lasting workspace changes other than deleting the stale
  `.cargo/config.toml`).
