# `/chunks/v1` server pipeline performance and HTTP protocol analysis

<!-- markdownlint-disable MD013 -->

Measured on 2026-09-29 against `polytope-dev.mn5.apps.dte.destination-earth.eu` and the `mn5-polytope-dev` context. No deployment or image build was performed. Kubernetes access was read-only and limited to `polytope-dev`, `ingress-nginx` (which contained no controller resources), and the cluster-scoped IngressClass. No extract jobs were submitted for this pass; measurements reused existing BOBS objects and logs. The only API load added was metadata/health requests and bounded reads of existing objects.

## Executive recommendation

1. Ship the worker-common binary-payload encoding fix in this branch. It prevents an already-zstd chunk from being wrapped in HTTP gzip/zstd and preserves CoverageJSON negotiation.
2. Deploy and validate the existing FDB-location cache branch next. Its measured ~111 ms/chunk saving is larger and more certain than protocol work.
3. Prototype hybrid direct delivery for compressed results up to 4 MiB, retaining BOBS above that threshold. It should remove roughly 150–300 ms from a quiet small-chunk request and can save more when BOBS is contended, but it needs slow-client and compression-layer safeguards.
4. Benchmark zstd level 1 over a representative parameter corpus. On the measured 6 MiB raw field slice it was both 36% cheaper and 2.3% smaller than level 3. Do not change the production level on one field alone.
5. Prefer more 1-CPU worker replicas before increasing per-process concurrency. Then canary `worker_concurrency=2`: it can overlap gribjump/BOBS waits, but CPU work, the shared Python datasource, and BOBS contention prevent assuming a 2x gain.
6. Keep HTTP/2 enabled at the ingress (it already is), but treat an aiohttp-to-HTTPX migration as a client change. It improves cold/high-concurrency request bursts; it does not alter the dominant backend work. Do not pursue HTTP/3 on the current ingress-nginx controller.

## Measurements

### Summary table

| Area | Method | Result |
| --- | --- | --- |
| Ingress protocol | `curl -sv --http2` | ALPN selected `h2`; response was HTTP/2. Forced HTTP/1.1 also worked. |
| Fresh single small request | forced h1 then h2, unauthenticated 401 | h1 198 ms, h2 206 ms. One request gets no multiplexing benefit. TCP connect was ~58 ms and TLS complete at ~152–158 ms. |
| 16 small parallel requests | curl parallel, three cold runs/protocol | h1 median wall 390 ms, 16 connections; h2 237 ms, one connection: **39% lower wall time / 153 ms saved**. |
| 16 parallel BOBS GETs | 16 existing objects, 40.25 MB total (2.40–2.60 MB each), three cold runs/protocol | h1 median wall 1.303 s (range 1.104–2.100), 30.9 MB/s; h2 1.085 s (0.992–1.176), 37.1 MB/s: **17% lower median wall time / 20% higher median throughput**. h1 opened 16 TLS connections; h2 opened one. |
| Persistent HTTP baseline | 10 aiohttp GETs of public health endpoint after warm-up | p50 42.9 ms, range 42.5–43.1 ms, HTTP/1.1. |
| Hot metadata | warm-up + 10 aiohttp metadata POSTs on one persistent session | p50 53.2 ms, mean 53.4, range 52.1–54.7 ms; 1,085-byte response; HTTP/1.1. First request including connection setup was 202 ms. |
| Idle queue dispatch | correlate frontend `api.job.submitted` and worker `worker.job.started` timestamps | 15 idle samples: p50 18.9 ms, mean 21.2, range 7.8–35.8 ms. |
| Saturated queue wait | same correlation | 6 queued samples: p50 3.258 s, range 2.227–3.866 s. These were whole-field jobs waiting behind the four single-job workers, not fixed dispatch overhead. |
| 786,432-point worker jobs | 120 existing `chunks-profile` + common-worker completion records | process p50 194.5 ms (p95 340); extract p50 146.2 ms; assemble p50 2.0 ms; zstd p50 43.6 ms. BOBS delivery p50 was 556.5 ms (p95 711) in this burst-heavy sample, versus quiet individual observations of 104–135 ms and the earlier 140–220 ms baseline. |
| Accidental second encoding | local recompression of an existing 2,595,225-byte zstd frame | gzip-6 p50 76.8 CPU-ms and grew to 2,596,038 bytes; zstd-3 p50 0.30 ms and grew to 2,595,295 bytes. aiohttp 3.13.3 advertises `gzip, deflate` by default, so the old worker path selected the expensive gzip case unless the client forced identity. |

The parallel download result includes normal internet/path noise. The stable finding is connection count (16 versus one) and the consistent small-request saving. The medium-object h1 run had a long-tail outlier, so the median is more useful than the mean.

### Zstd level sweep

Input was a real chunk downloaded from BOBS and decompressed to 6,291,456 bytes (786,432 float64 values). Each level was warmed once, then measured seven times with Python zstandard 0.25.0. Times are median process CPU on this host, not pod CPU; use the relative result and the deployed level-3 median (43.6 ms) for capacity estimates.

| Level | Bytes | Raw ratio | Median CPU-ms | Delta from level 3 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,535,465 | 40.30% | 13.14 | 36% less CPU and 2.30% fewer bytes |
| 3 | 2,595,225 | 41.25% | 20.47 | baseline |
| 6 | 2,605,124 | 41.41% | 61.13 | worse size and ~3x CPU on this sample |
| 10 | 2,468,666 | 39.24% | 180.83 | 4.88% fewer bytes, ~9x CPU |
| 19 | 2,301,042 | 36.57% | 2,488.11 | 11.34% fewer bytes, ~122x CPU |

Level 1 is the only promising server setting from this field. Applying its local CPU ratio to the pod's 43.6 ms level-3 median suggests about 16 ms/chunk CPU saved, but parameter entropy varies; benchmark at least temperature, wind, pressure, precipitation and ocean fields before changing it.

## HTTP/1.1, HTTP/2 and HTTP/3

### What the dev ingress supports

The visible IngressClass is `nginx` with controller `k8s.io/ingress-nginx`. The application ingress and BOBS ingress both use it. There were no resources in namespace `ingress-nginx`, so the controller image/version and ConfigMap were not visible without searching other namespaces; this pass did not do that.

`curl --http2` offered `h2,http/1.1`; the server selected h2 via ALPN. This matches ingress-nginx's `use-http2: true` default. Responses did not advertise `Alt-Svc`.

The installed curl has HTTP/2 but no HTTP/3 transport, so `curl --http3` failed locally before making a request. This means there is no direct QUIC negotiation result from this host. Nevertheless, HTTP/3 is not a supported configurable feature in current upstream ingress-nginx: the current configuration model exposes `use-http2` but no HTTP/3/QUIC option, and proposed HTTP/3 PRs were closed unmerged. One proposed experimental design used controller `--enable-quic`, UDP 443 on the Service/container, and `Alt-Svc: h3=":443"`; those are not available as a production flag in current upstream ingress-nginx. The absent `Alt-Svc` header is consistent with HTTP/3 being unavailable here.

### What the current client uses

The installed aiohttp 3.13.3 `ClientSession` defaults to `HttpVersion(1, 1)` and exports only HTTP/1.0 and HTTP/1.1 constants. The live metadata measurement confirmed every response as HTTP/1.1. Its default `Accept-Encoding` is `gzip, deflate`, which also explains the old double-encoding path.

HTTPX supports HTTP/2 when installed as `httpx[http2]` and constructed with `httpx.AsyncClient(http2=True)`. On this host, plain HTTPX is installed without the `h2` extra and correctly rejects `http2=True`. A migration belongs in the polytope-zarr client, not this server repository. Keep `Accept-Encoding: identity` until all relevant server deployments carry the binary-payload fix.

### Expected value for the job pattern

A normal small chunk currently uses:

1. authenticated submit, held open for the 15-second v2 poll window;
2. zero or more authenticated long polls if queueing exceeds that window;
3. one unauthenticated BOBS GET after the result redirect.

With concurrency 16, aiohttp can reuse warm h1 connections, so HTTP/2 does not save an RTT on every steady-state request. It does reduce the cold connection set from up to 16 TCP/TLS connections to one and multiplexes submit/poll/download traffic without h1 connection-slot pressure. The measured bound is therefore:

- cold burst of small requests: about **153 ms / 39%** saved on this path;
- 16 medium downloads: about **218 ms / 17%** median makespan saved;
- warm single request: effectively no protocol-only saving.

HTTP/2 will reduce client/ingress sockets and TLS CPU even where wall time is unchanged. It will not improve FDB lookup, GRIB decode, zstd, worker availability, or BOBS upload. For that reason it ranks below location caching and hybrid inline delivery.

HTTP/3's QUIC handshake and stream-level loss recovery could improve first-use and tail latency for remote users on high-RTT or lossy links by avoiding TCP head-of-line blocking across multiplexed streams. In the datacentre/low-loss path, a persistent h2 connection already amortizes handshakes and loss is rare, so the expected gain is negligible. Adopting h3 would also require a different/supporting ingress controller or a future ingress-nginx implementation, UDP 443 exposure, Alt-Svc, load-balancer/firewall work, and an h3-capable client. Recommendation: **keep h2; do not make h3 a polytope server work item now**.

## Implemented in this pass

### Do not HTTP-encode binary worker payloads

`workers/common/src/lib.rs` now selects identity encoding whenever the response media type is `application/octet-stream` (case-insensitive and tolerant of parameters). Other content types still negotiate zstd/gzip exactly as before, preserving CoverageJSON behaviour.

This is both a CPU and a wire-contract fix. A `/chunks/v1` body is already one zstd application frame and the Zarr client must receive it verbatim. With aiohttp's default headers, the previous code gzip-compressed that frame again at about 77 local CPU-ms for no size benefit. The client workaround (`Accept-Encoding: identity`) remains harmless.

Tests added:

- octet-stream with `gzip, zstd` or parameterised/case-varied content type selects identity;
- CoverageJSON and JSON still select zstd/gzip.

Validation:

- `cargo test -p polytope-worker-common --lib`: 43 passed;
- targeted encoding tests: 2 passed;
- rust-analyzer diagnostics on `workers/common/src/lib.rs`: clean.

No parsed-qube cache change was needed. `CatalogueCache` already stores `Arc<Qube>` plus ETag and fetch time. JSON parsing occurs only after a TTL refresh returns a modified body, on `spawn_blocking`; hot requests clone the `Arc` and select against the parsed arena. The 10-call hot metadata distribution confirms there is no per-request 5 MB parse. For this deployment there is one configured catalogue source, so replacing the cached parsed arena on ETag change is equivalent to an ETag-keyed single-entry cache.

## Prioritised opportunities

| Priority | Opportunity | Estimated impact | Effort / risk | Next measurement |
| --- | --- | --- | --- | --- |
| P0 | Deploy this octet-stream encoding fix | Avoids ~77 local CPU-ms/chunk under aiohttp's default gzip negotiation; guarantees byte-for-byte zstd payload | Small; implemented and tested | Compare worker `process_ms`/CPU with default client headers after orchestrated deploy |
| P0 | Deploy `chunks-v1-loccache` | Existing measurement: ~111 ms/chunk saved on repeated locations | Already implemented on another branch; cache correctness/invalidations matter | A/B repeated same-field and cross-field jobs |
| P1 | Hybrid inline delivery at compressed size <= 4 MiB | Expected quiet saving ~150–300 ms/chunk (BOBS create/write/complete plus extra GET request); recent BOBS-contended sample indicates larger possible tail saving | Medium. BITS already supports streaming `200` within the 15 s submit poll. Worker delivery is pool-wide today, so add a hybrid delivery policy and decide after the worker has the compressed length. Bound slow-client worker occupancy and frontend bandwidth. | Canary 1/2/4 MiB thresholds; measure p50/p95 worker-slot time, frontend bytes/CPU, client cancellation and 1/10/100 Mbit/s readers |
| P1 | Validate zstd level 1, then switch if corpus agrees | About 16 pod-ms/chunk projected; 2.3% fewer bytes on this field | Code change is trivial, evidence is only one field | Multi-parameter corpus, 30+ chunks offline; compare level 1/3 size and CPU distributions |
| P1 | Scale replicas; canary `worker_concurrency=2` second | Replicas should scale near-linearly until gribjump/BOBS saturate. Two in-process jobs may overlap ~146 ms extraction and ~100–220 ms delivery waits; realistic expectation 1.4–1.8x/pod, not 2x | Replicas cost CPU/memory but isolate failures. Concurrency shares one CPU, Python interpreter/datasource and BOBS; thread safety must be confirmed | Orchestrated 1-vs-2 concurrency load test at same total CPU, measuring throughput, p95 and BOBS contention |
| P1 | Investigate BOBS burst delivery | Current 120-chunk sample: delivery p50 557 ms/p95 711 versus quiet 104–135 and earlier 140–220 | Measurement/tuning first. Fresh create connections are deliberate to spread spools; do not simply pool them | BOBS benchmark with 1/4/8/16 writers, disk/admission metrics and no client reads |
| P2 | Enable bounded auth result caching if revocation policy allows | Hot metadata is 53.2 ms external versus 42.9 ms public-health baseline, so total auth+metadata work is only ~10 ms here; larger value is reducing auth-o-tron request volume across submit/polls | Configuration-only, but changes credential/role revocation latency | 0/30/300 s TTL test, auth-o-tron request rate and p95; agree security TTL first |
| P2 | Add `Bits::active_job(id)` and stop full snapshots on poll | Avoids O(active jobs) iteration and deep clones of request/metadata in `local_pending_status` and ownership checks | Small code, but cross-repository bits API/release | CPU/allocation profile at 1k+ active jobs and high poll rate |
| P3 | Reduce Python-to-Rust result copy | Current PyO3 boundary copies the compressed Python `bytes` into a Rust `Vec` once (`to_vec`) | Low single-digit ms at these sizes; lifetime/ownership work is non-trivial | Microbenchmark 1/4/40 MiB payload boundary before redesign |

### Inline delivery design details

Chunks always use BOBS because `DeliveryConfig.delivery_type` constructs one `ResultDelivery` for the entire worker pool. `BobsPush` performs create, one HTTP/2 write, complete, then reports a redirect. BITS already has the necessary direct path: `/complete/data/{job_id}` converts the request body into a streaming `JobResult::Success`, and the frontend returns it as HTTP 200. The submit handler waits up to 15 seconds, so a normal 200–800 ms chunk completes on the original POST with no poll or download request.

A contained implementation would add `inline_max_bytes` to a hybrid BOBS delivery, expose the encoded byte length before selecting delivery (the fe worker already owns a complete compressed `Vec`), use direct completion at or below the cap, and retain BOBS otherwise. Start with 4 MiB compressed so the measured ~2.5 MiB chunks qualify.

Two safeguards are required:

1. Direct streaming keeps the worker completion request open while the client consumes data; the cap must protect worker slots from slow or disconnected clients.
2. The frontend's global tower-http `CompressionLayer::new()` compresses unknown-length `application/octet-stream` by default. Exclude octet-stream from that layer (or mark the response as already application-compressed) before enabling inline chunks. BITS currently does not preserve a worker `Content-Encoding` in `JobResult::Success`, so relying on that header is not sufficient.

### Worker concurrency details

The common worker loop and fe-worker CLI already implement `worker_concurrency`, including tests for two simultaneous jobs and the `POLYTOPE_WORKER_CONCURRENCY` override. Each job runs on Tokio's blocking pool and enters Python. pygribjump and zstandard C work can release the GIL, but request parsing, field enumeration and parts of assembly remain serialized by it. One-CPU limits also make the ~46 ms median assemble+zstd CPU portion contend.

More replicas are therefore the safer first scaling control: more CPU quotas, separate Python/GribJump state, and better failure isolation. Per-process concurrency is attractive only for hiding remote lookup/decode and delivery waits; test the shared GribJump datasource under concurrency rather than assuming it is thread-safe.

## Other code-path observations

- Worker broker clients are rebuilt after each long-poll cycle. This looks wasteful, but the test `empty_poll_rotates_connection_and_job_callbacks_stay_sticky` documents intentional connection rotation across broker replicas. Do not pool without replacing that load-balancing property.
- The BOBS create client deliberately disables idle pooling so kube-proxy spreads new spools across BOBS pods. Body write/complete use a pooled h2 client tied to the returned owner URL. Again, do not remove this without an owner-aware load-balancing design.
- Remote-pool direct completion is already zero-copy at the HTTP-frame level: BITS forwards `Bytes` frames through a bounded channel. This makes hybrid inline delivery substantially smaller than inventing a new endpoint.
- Metadata hot-path parsing is already off the async runtime and cached parsed. Remaining work is qube selection/tree construction and metkit expansion; the measured small-selection endpoint overhead above a persistent public health call is only about 10 ms. Profile the previously measured 203 KB/323 ms very-large selection before optimising allocations in `qube.rs`/`tree.rs`.
- `local_pending_status` and active-job ownership checks call `active_jobs()`, which builds a full snapshot and clones request/metadata for every active job. This is negligible at dev scale but is the clearest avoidable poll-path allocation at high active-job counts.

<!-- markdownlint-enable MD013 -->
