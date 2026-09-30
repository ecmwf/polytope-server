// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Minimal bindings to the stable gribjump 0.12 C API used by the chunks worker.
//!
//! The upstream Rust crate tracks a newer, source-built C++ API.  The worker image
//! already contains the released C library in the pygribjump wheel, so this module
//! deliberately loads only the small C surface needed by `extract_from_paths`.

use libloading::Library;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::{CStr, CString, c_char, c_int, c_ulong, c_void};
use std::path::{Path, PathBuf};
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

const DEFAULT_SUBBATCH: usize = 1024;
const LIBRARY_RELATIVE_PATH: &str = "gribjumplib/lib64/libgribjump.so";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PathRequest {
    pub path: String,
    pub offset: usize,
    pub host: String,
    pub port: c_int,
    pub scheme: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ExtractPlan {
    pub kind: String,
    #[serde(default)]
    pub paths: Vec<PathRequest>,
    #[serde(default)]
    pub ranges: Vec<[usize; 2]>,
    #[serde(default)]
    pub grid_hash: String,
    #[serde(default)]
    pub dtype: String,
    #[serde(default)]
    pub shuffle: bool,
    pub zstd_level: i32,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
    #[serde(default)]
    pub elements: Vec<ExtractElementPlan>,
    pub profile: PlanProfile,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ExtractElementPlan {
    #[serde(default)]
    pub status: u8,
    #[serde(default)]
    pub path_indices: Vec<usize>,
    pub ranges: Vec<[usize; 2]>,
    pub grid_hash: String,
    pub dtype: String,
    pub shuffle: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct PlanProfile {
    pub job: String,
    pub fields: usize,
    pub ranges: usize,
    pub points: usize,
    pub dtype: String,
    pub shuffle: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub fallbacks: usize,
    pub lookup_mode: String,
    pub lookup_fallbacks: usize,
    pub lookup_subbatches: usize,
    pub lookup_ms: f64,
    pub parse_ms: f64,
    pub enum_ms: f64,
    pub python_ms: f64,
    #[serde(default = "default_one")]
    pub chunks: usize,
    #[serde(default)]
    pub files: usize,
}

const fn default_one() -> usize {
    1
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExtractMetrics {
    pub gj_subbatches: usize,
    pub inflight: usize,
    pub extract_ms: f64,
    pub assemble_ms: f64,
    pub shuffle_ms: f64,
    pub zstd_ms: f64,
    pub raw_bytes: usize,
    pub payload_bytes: usize,
}

#[derive(Debug)]
pub struct ExtractOutput {
    pub payload: Vec<u8>,
    pub metrics: ExtractMetrics,
}

#[derive(Debug)]
struct BatchOutput {
    fields: Vec<Vec<f64>>,
    assemble_time: Duration,
}

#[derive(Debug)]
struct RequestBatch {
    requests: Vec<PathRequest>,
    original_indices: Vec<usize>,
}

/// Group requests by physical file and greedily pack whole files up to the
/// target. Oversized files remain intact. The mapping back to the caller's
/// order is carried alongside each reordered batch.
type FileRequests<'a> = BTreeMap<(&'a str, c_int, &'a str), Vec<(usize, &'a PathRequest)>>;

fn file_aligned_batches(requests: &[PathRequest], target: usize) -> Vec<RequestBatch> {
    let target = target.max(1);
    let mut files: FileRequests<'_> = BTreeMap::new();
    for (original_index, request) in requests.iter().enumerate() {
        files
            .entry((&request.host, request.port, &request.path))
            .or_default()
            .push((original_index, request));
    }

    let mut batches = Vec::new();
    let mut current = RequestBatch {
        requests: Vec::new(),
        original_indices: Vec::new(),
    };
    for mut file in files.into_values() {
        file.sort_by(|(left_index, left), (right_index, right)| {
            left.offset
                .cmp(&right.offset)
                .then_with(|| left_index.cmp(right_index))
        });
        if !current.requests.is_empty() && current.requests.len() + file.len() > target {
            batches.push(current);
            current = RequestBatch {
                requests: Vec::new(),
                original_indices: Vec::new(),
            };
        }
        for (original_index, request) in file {
            current.requests.push(request.clone());
            current.original_indices.push(original_index);
        }
        if current.requests.len() >= target {
            batches.push(current);
            current = RequestBatch {
                requests: Vec::new(),
                original_indices: Vec::new(),
            };
        }
    }
    if !current.requests.is_empty() {
        batches.push(current);
    }
    batches
}

trait ExtractionBackend: Send {
    fn extract_batch(
        &mut self,
        requests: &[PathRequest],
        ranges: &[[usize; 2]],
        grid_hash: &str,
        context: Option<&str>,
    ) -> Result<BatchOutput, String>;
}

struct BackendPool {
    available: Mutex<Vec<Box<dyn ExtractionBackend>>>,
    ready: Condvar,
    size: usize,
}

impl BackendPool {
    fn new(backends: Vec<Box<dyn ExtractionBackend>>) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("GribJump in-flight concurrency must be at least 1".to_string());
        }
        Ok(Self {
            size: backends.len(),
            available: Mutex::new(backends),
            ready: Condvar::new(),
        })
    }

    fn checkout(&self) -> BackendLease<'_> {
        let mut available = self
            .available
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while available.is_empty() {
            available = self
                .ready
                .wait(available)
                .unwrap_or_else(|error| error.into_inner());
        }
        BackendLease {
            pool: self,
            backend: available.pop(),
        }
    }

    fn warm_all(&self, requests: &[PathRequest], grid_hash: &str) -> Result<(), String> {
        let mut available = self
            .available
            .lock()
            .map_err(|_| "GribJump handle pool mutex poisoned".to_string())?;
        if available.len() != self.size {
            return Err("cannot warm GribJump handles while extraction is active".to_string());
        }
        let mut first_error = None;
        for backend in available.iter_mut() {
            if let Err(error) = backend
                .extract_batch(requests, &[[0, 1]], grid_hash, None)
                .map(|_| ())
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

struct BackendLease<'a> {
    pool: &'a BackendPool,
    backend: Option<Box<dyn ExtractionBackend>>,
}

impl BackendLease<'_> {
    fn extract_batch(
        &mut self,
        requests: &[PathRequest],
        ranges: &[[usize; 2]],
        grid_hash: &str,
        context: Option<&str>,
    ) -> Result<BatchOutput, String> {
        self.backend
            .as_mut()
            .expect("checked-out backend must be present")
            .extract_batch(requests, ranges, grid_hash, context)
    }
}

impl Drop for BackendLease<'_> {
    fn drop(&mut self) {
        if let Some(backend) = self.backend.take() {
            let mut available = self
                .pool
                .available
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            available.push(backend);
            self.pool.ready.notify_one();
        }
    }
}

pub struct GribJumpExtractor {
    pool: BackendPool,
    subbatch: usize,
}

impl std::fmt::Debug for GribJumpExtractor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GribJumpExtractor")
            .field("subbatch", &self.subbatch)
            .field("inflight", &self.pool.size)
            .finish_non_exhaustive()
    }
}

impl GribJumpExtractor {
    pub fn native(inflight: usize) -> Result<Self, String> {
        let backends = (0..inflight)
            .map(|_| {
                NativeBackend::load().map(|backend| Box::new(backend) as Box<dyn ExtractionBackend>)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            pool: BackendPool::new(backends)?,
            subbatch: DEFAULT_SUBBATCH,
        })
    }

    pub fn warm(&self, requests: &[PathRequest], grid_hash: &str) -> Result<(), String> {
        if requests.is_empty() {
            return Ok(());
        }
        self.pool.warm_all(requests, grid_hash)
    }

    pub fn extract(&self, plan: &ExtractPlan) -> Result<ExtractOutput, String> {
        validate_plan(plan)?;
        if plan.kind == "rust_gribjump_extract_v2" {
            return self.extract_multi(plan);
        }
        let context = plan
            .context
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| format!("invalid GribJump context: {error}"))?;
        let expected_per_field = plan
            .ranges
            .iter()
            .map(|[start, end]| end - start)
            .sum::<usize>();
        let requests = file_aligned_batches(&plan.paths, self.subbatch);
        let worker_count = self.pool.size.min(requests.len());
        let next = AtomicUsize::new(0);
        let cancelled = AtomicBool::new(false);
        let (sender, receiver) = std::sync::mpsc::channel();
        let extract_started = Instant::now();

        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                let sender = sender.clone();
                let next = &next;
                let cancelled = &cancelled;
                let requests = &requests;
                let context = context.as_deref();
                scope.spawn(move || {
                    loop {
                        if cancelled.load(Ordering::Acquire) {
                            break;
                        }
                        let index = next.fetch_add(1, Ordering::AcqRel);
                        let Some(batch) = requests.get(index) else {
                            break;
                        };
                        let result = self.pool.checkout().extract_batch(
                            &batch.requests,
                            &plan.ranges,
                            &plan.grid_hash,
                            context,
                        );
                        let failed = result.is_err();
                        if sender.send((index, result)).is_err() {
                            break;
                        }
                        if failed {
                            cancelled.store(true, Ordering::Release);
                            break;
                        }
                    }
                });
            }
            drop(sender);
        });

        let mut batches = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        let mut first_error = None;
        for (index, result) in receiver {
            match result {
                Ok(batch) => batches[index] = Some(batch),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if batches.iter().any(Option::is_none) {
            return Err("GribJump extraction stopped before every sub-batch completed".to_string());
        }

        let mut metrics = ExtractMetrics {
            gj_subbatches: batches.len(),
            inflight: worker_count,
            extract_ms: milliseconds(extract_started.elapsed()),
            ..ExtractMetrics::default()
        };
        let mut fields = std::iter::repeat_with(|| None)
            .take(plan.paths.len())
            .collect::<Vec<_>>();
        for (batch_index, batch) in batches.into_iter().enumerate() {
            let batch = batch.expect("all sub-batches checked above");
            metrics.assemble_ms += milliseconds(batch.assemble_time);
            let request_batch = &requests[batch_index];
            if batch.fields.len() != request_batch.requests.len() {
                return Err(format!(
                    "gribjump returned {} of {} fields",
                    batch.fields.len(),
                    request_batch.requests.len()
                ));
            }
            let assemble_started = Instant::now();
            for (original_index, field) in request_batch
                .original_indices
                .iter()
                .copied()
                .zip(batch.fields)
            {
                if field.len() != expected_per_field {
                    return Err(format!(
                        "gribjump field {original_index} returned {} values, expected {}",
                        field.len(),
                        expected_per_field
                    ));
                }
                fields[original_index] = Some(field);
            }
            metrics.assemble_ms += milliseconds(assemble_started.elapsed());
        }
        if fields.iter().any(Option::is_none) {
            return Err("GribJump did not return every requested field".to_string());
        }
        let flatten_started = Instant::now();
        let mut values = Vec::with_capacity(plan.paths.len() * expected_per_field);
        for field in fields {
            values.extend(field.expect("all fields checked above"));
        }
        metrics.assemble_ms += milliseconds(flatten_started.elapsed());

        let shuffle_started = Instant::now();
        let item_size = if plan.dtype == "float32" { 4 } else { 8 };
        let mut wire = Vec::with_capacity(values.len() * item_size);
        if item_size == 4 {
            for value in values {
                wire.extend_from_slice(&(value as f32).to_le_bytes());
            }
        } else {
            for value in values {
                wire.extend_from_slice(&value.to_le_bytes());
            }
        }
        metrics.raw_bytes = wire.len();
        if plan.shuffle {
            wire = byte_shuffle(&wire, item_size);
        }
        metrics.shuffle_ms = milliseconds(shuffle_started.elapsed());

        let zstd_started = Instant::now();
        let payload = compress_python_compatible(&wire, plan.zstd_level)?;
        metrics.zstd_ms = milliseconds(zstd_started.elapsed());
        metrics.payload_bytes = payload.len();
        metrics.extract_ms = round_tenth(metrics.extract_ms);
        metrics.assemble_ms = round_tenth(metrics.assemble_ms);
        metrics.shuffle_ms = round_tenth(metrics.shuffle_ms);
        metrics.zstd_ms = round_tenth(metrics.zstd_ms);
        Ok(ExtractOutput { payload, metrics })
    }

    fn extract_multi(&self, plan: &ExtractPlan) -> Result<ExtractOutput, String> {
        validate_plan(plan)?;
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for element in &plan.elements {
            if element.status == 1 {
                continue;
            }
            let indices = groups.entry(element.grid_hash.clone()).or_default();
            for &index in &element.path_indices {
                if !indices.contains(&index) {
                    indices.push(index);
                }
            }
        }

        let mut extracted: BTreeMap<(String, usize), Vec<f64>> = BTreeMap::new();
        let mut group_ranges: BTreeMap<String, Vec<[usize; 2]>> = BTreeMap::new();
        let mut metrics = ExtractMetrics::default();
        for (grid_hash, indices) in groups {
            let ranges = merged_ranges(
                plan.elements
                    .iter()
                    .filter(|element| element.status == 0 && element.grid_hash == grid_hash)
                    .flat_map(|element| element.ranges.iter().copied()),
            );
            let paths = indices
                .iter()
                .map(|&index| plan.paths[index].clone())
                .collect::<Vec<_>>();
            let subplan = ExtractPlan {
                kind: "rust_gribjump_extract_v1".to_string(),
                paths,
                ranges: ranges.clone(),
                grid_hash: grid_hash.clone(),
                dtype: "float64".to_string(),
                shuffle: false,
                zstd_level: plan.zstd_level,
                context: plan.context.clone(),
                elements: Vec::new(),
                profile: plan.profile.clone(),
            };
            let output = self.extract(&subplan)?;
            metrics.gj_subbatches += output.metrics.gj_subbatches;
            metrics.inflight = metrics.inflight.max(output.metrics.inflight);
            metrics.extract_ms += output.metrics.extract_ms;
            metrics.assemble_ms += output.metrics.assemble_ms;
            let raw = zstd::decode_all(std::io::Cursor::new(&output.payload))
                .map_err(|error| format!("cannot decode intermediate extract: {error}"))?;
            let per_field = ranges.iter().map(|[lo, hi]| hi - lo).sum::<usize>();
            if raw.len() != indices.len() * per_field * 8 {
                return Err("intermediate extract length mismatch".to_string());
            }
            for (field_offset, &path_index) in indices.iter().enumerate() {
                let start = field_offset * per_field * 8;
                let values = raw[start..start + per_field * 8]
                    .chunks_exact(8)
                    .map(|bytes| f64::from_le_bytes(bytes.try_into().expect("eight bytes")))
                    .collect();
                extracted.insert((grid_hash.clone(), path_index), values);
            }
            group_ranges.insert(grid_hash, ranges);
        }

        let assemble_started = Instant::now();
        let mut results = Vec::with_capacity(plan.elements.len());
        for element in &plan.elements {
            if element.status == 1 {
                results.push((1_u8, Vec::new()));
                continue;
            }
            let ranges = &group_ranges[&element.grid_hash];
            let expected = element
                .ranges
                .iter()
                .map(|[lo, hi]| hi - lo)
                .sum::<usize>();
            let mut values = Vec::with_capacity(element.path_indices.len() * expected);
            for &path_index in &element.path_indices {
                let field = &extracted[&(element.grid_hash.clone(), path_index)];
                append_selected_ranges(&mut values, field, ranges, &element.ranges)?;
            }
            let payload = encode_values(
                values,
                &element.dtype,
                element.shuffle,
                plan.zstd_level,
                &mut metrics,
            )?;
            results.push((0_u8, payload));
        }
        metrics.assemble_ms += milliseconds(assemble_started.elapsed());

        let mut payload = Vec::new();
        payload.extend_from_slice(b"PZMC");
        payload.push(1);
        payload.extend_from_slice(&(results.len() as u32).to_le_bytes());
        for (status, bytes) in &results {
            payload.push(*status);
            payload.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        }
        for (_status, bytes) in results {
            payload.extend_from_slice(&bytes);
        }
        metrics.payload_bytes = payload.len();
        metrics.assemble_ms = round_tenth(metrics.assemble_ms);
        Ok(ExtractOutput { payload, metrics })
    }
}

fn validate_plan(plan: &ExtractPlan) -> Result<(), String> {
    if plan.kind == "rust_gribjump_extract_v2" {
        if plan.elements.is_empty() {
            return Err("multi extract plan has no elements".to_string());
        }
        for (element_index, element) in plan.elements.iter().enumerate() {
            if element.status > 1 {
                return Err(format!("invalid status for element {element_index}"));
            }
            if element.status == 0 && element.path_indices.is_empty() {
                return Err(format!("element {element_index} has no paths"));
            }
            if element.path_indices.iter().any(|&index| index >= plan.paths.len()) {
                return Err(format!("element {element_index} has an invalid path index"));
            }
            validate_wire(&element.ranges, &element.dtype)?;
        }
        return Ok(());
    }
    if plan.kind != "rust_gribjump_extract_v1" {
        return Err(format!("unsupported extract plan kind {:?}", plan.kind));
    }
    if plan.paths.is_empty() {
        return Err("extract plan has no paths".to_string());
    }
    validate_wire(&plan.ranges, &plan.dtype)
}

fn validate_wire(ranges: &[[usize; 2]], dtype: &str) -> Result<(), String> {
    if ranges.is_empty() {
        return Err("extract plan has no ranges".to_string());
    }
    if dtype != "float32" && dtype != "float64" {
        return Err(format!("unsupported extract dtype {dtype:?}"));
    }
    for [start, end] in ranges {
        if end <= start {
            return Err(format!("invalid extract range [{start}, {end}]"));
        }
    }
    Ok(())
}

fn merged_ranges(ranges: impl Iterator<Item = [usize; 2]>) -> Vec<[usize; 2]> {
    let mut ranges = ranges.collect::<Vec<_>>();
    ranges.sort_unstable();
    let mut merged: Vec<[usize; 2]> = Vec::new();
    for [lo, hi] in ranges {
        if let Some(last) = merged.last_mut()
            && lo <= last[1]
        {
            last[1] = last[1].max(hi);
        } else {
            merged.push([lo, hi]);
        }
    }
    merged
}

fn append_selected_ranges(
    output: &mut Vec<f64>,
    field: &[f64],
    union_ranges: &[[usize; 2]],
    selected_ranges: &[[usize; 2]],
) -> Result<(), String> {
    for [selected_lo, selected_hi] in selected_ranges {
        let mut base = 0;
        let mut copied = 0;
        for [union_lo, union_hi] in union_ranges {
            let lo = (*selected_lo).max(*union_lo);
            let hi = (*selected_hi).min(*union_hi);
            if lo < hi {
                let start = base + lo - union_lo;
                output.extend_from_slice(&field[start..start + hi - lo]);
                copied += hi - lo;
            }
            base += union_hi - union_lo;
        }
        if copied != selected_hi - selected_lo {
            return Err("selected range is not covered by union extraction".to_string());
        }
    }
    Ok(())
}

fn encode_values(
    values: Vec<f64>,
    dtype: &str,
    shuffle: bool,
    zstd_level: i32,
    metrics: &mut ExtractMetrics,
) -> Result<Vec<u8>, String> {
    let shuffle_started = Instant::now();
    let item_size = if dtype == "float32" { 4 } else { 8 };
    let mut wire = Vec::with_capacity(values.len() * item_size);
    if item_size == 4 {
        for value in values {
            wire.extend_from_slice(&(value as f32).to_le_bytes());
        }
    } else {
        for value in values {
            wire.extend_from_slice(&value.to_le_bytes());
        }
    }
    metrics.raw_bytes += wire.len();
    if shuffle {
        wire = byte_shuffle(&wire, item_size);
    }
    metrics.shuffle_ms += milliseconds(shuffle_started.elapsed());
    let zstd_started = Instant::now();
    let payload = compress_python_compatible(&wire, zstd_level)?;
    metrics.zstd_ms += milliseconds(zstd_started.elapsed());
    Ok(payload)
}

fn byte_shuffle(wire: &[u8], item_size: usize) -> Vec<u8> {
    let elements = wire.len() / item_size;
    let mut shuffled = Vec::with_capacity(wire.len());
    for byte_index in 0..item_size {
        for element in 0..elements {
            shuffled.push(wire[element * item_size + byte_index]);
        }
    }
    shuffled
}

fn compress_python_compatible(wire: &[u8], level: i32) -> Result<Vec<u8>, String> {
    // python-zstandard's one-shot compress() uses one compressStream2(..., end)
    // call with the source size pledged, rather than ZSTD_compress2(). The two
    // entry points produce valid but byte-different frames for some small chunks.
    use zstd::zstd_safe::{CCtx, CParameter, InBuffer, OutBuffer};
    let mut context = CCtx::create();
    for parameter in [
        CParameter::CompressionLevel(level),
        CParameter::ContentSizeFlag(true),
        CParameter::ChecksumFlag(false),
        CParameter::DictIdFlag(true),
    ] {
        context
            .set_parameter(parameter)
            .map_err(|error| format!("zstd parameter setup failed: {error:?}"))?;
    }
    context
        .set_pledged_src_size(Some(wire.len() as u64))
        .map_err(|error| format!("zstd source-size setup failed: {error:?}"))?;

    let mut payload = Vec::with_capacity(zstd::zstd_safe::compress_bound(wire.len()));
    let mut input = InBuffer::around(wire);
    let remaining = {
        let mut output = OutBuffer::around(&mut payload);
        context
            .compress_stream2(
                &mut output,
                &mut input,
                zstd::zstd_safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )
            .map_err(|error| format!("zstd compression failed: {error:?}"))?
    };
    if remaining != 0 || input.pos() != wire.len() {
        return Err(format!(
            "zstd compression did not finish (remaining={remaining}, consumed={})",
            input.pos()
        ));
    }
    Ok(payload)
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn round_tenth(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

type Handle = c_void;
type NativePathRequest = c_void;
type IteratorHandle = c_void;
type ResultHandle = c_void;

type Initialise = unsafe extern "C" fn() -> c_int;
type ErrorString = unsafe extern "C" fn() -> *const c_char;
type NewHandle = unsafe extern "C" fn(*mut *mut Handle) -> c_int;
type DeleteHandle = unsafe extern "C" fn(*mut Handle) -> c_int;
type NewPathRequest = unsafe extern "C" fn(
    *mut *mut NativePathRequest,
    *const c_char,
    *const c_char,
    usize,
    *const c_char,
    c_int,
    *const usize,
    usize,
    *const c_char,
) -> c_int;
type DeletePathRequest = unsafe extern "C" fn(*mut NativePathRequest) -> c_int;
type ExtractFromPaths = unsafe extern "C" fn(
    *mut Handle,
    *const *mut NativePathRequest,
    c_ulong,
    *const c_char,
    *mut *mut IteratorHandle,
) -> c_int;
type IteratorNext = unsafe extern "C" fn(*mut IteratorHandle, *mut *mut ResultHandle) -> c_int;
type DeleteIterator = unsafe extern "C" fn(*const IteratorHandle) -> c_int;
type ResultValues = unsafe extern "C" fn(*mut ResultHandle, *mut *mut f64, usize) -> c_int;
type DeleteResult = unsafe extern "C" fn(*mut ResultHandle) -> c_int;

struct NativeApi {
    _library: Library,
    error_string: ErrorString,
    new_handle: NewHandle,
    delete_handle: DeleteHandle,
    new_path_request: NewPathRequest,
    delete_path_request: DeletePathRequest,
    extract_from_paths: ExtractFromPaths,
    iterator_next: IteratorNext,
    delete_iterator: DeleteIterator,
    result_values: ResultValues,
    delete_result: DeleteResult,
}

impl NativeApi {
    fn load() -> Result<Self, String> {
        let path = locate_library();
        let library = Library::new(&path)
            .map_err(|error| format!("cannot load {}: {error}", path.display()))?;
        unsafe {
            let initialise = load_symbol::<Initialise>(&library, b"gribjump_initialise\0")?;
            check_raw(
                load_symbol::<ErrorString>(&library, b"gribjump_error_string\0")?,
                initialise(),
                "gribjump_initialise",
            )?;
            Ok(Self {
                error_string: load_symbol(&library, b"gribjump_error_string\0")?,
                new_handle: load_symbol(&library, b"gribjump_new_handle\0")?,
                delete_handle: load_symbol(&library, b"gribjump_delete_handle\0")?,
                new_path_request: load_symbol(&library, b"gribjump_new_request_from_path\0")?,
                delete_path_request: load_symbol(&library, b"gribjump_delete_path_request\0")?,
                extract_from_paths: load_symbol(&library, b"gribjump_extract_from_paths\0")?,
                iterator_next: load_symbol(&library, b"gribjump_extractioniterator_next\0")?,
                delete_iterator: load_symbol(&library, b"gribjump_extractioniterator_delete\0")?,
                result_values: load_symbol(&library, b"gribjump_result_values\0")?,
                delete_result: load_symbol(&library, b"gribjump_delete_result\0")?,
                _library: library,
            })
        }
    }

    fn check(&self, code: c_int, operation: &str) -> Result<(), String> {
        // SAFETY: error_string is a retained library symbol and returns either null or
        // a library-owned NUL-terminated message.
        unsafe { check_raw(self.error_string, code, operation) }
    }
}

unsafe fn load_symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, String> {
    // SAFETY: callers provide the exact symbol signature from gribjump_c.h.
    unsafe { library.get::<T>(name) }
        .map(|symbol| *symbol)
        .map_err(|error| {
            format!(
                "cannot load gribjump symbol {}: {error}",
                String::from_utf8_lossy(name).trim_end_matches('\0')
            )
        })
}

unsafe fn check_raw(error_string: ErrorString, code: c_int, operation: &str) -> Result<(), String> {
    if code == 0 {
        return Ok(());
    }
    // SAFETY: the C API documents this as a library-owned NUL-terminated string.
    let pointer = unsafe { error_string() };
    let message = if pointer.is_null() {
        "unknown gribjump error".to_string()
    } else {
        // SAFETY: checked non-null and guaranteed NUL-terminated by the C API.
        unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned()
    };
    Err(format!("{operation}: {message}"))
}

fn locate_library() -> PathBuf {
    if let Ok(directory) = std::env::var("GRIBJUMP_LIB_DIR") {
        return Path::new(&directory).join("libgribjump.so");
    }
    let fixed = PathBuf::from("/opt/venv/lib/python3.11/site-packages").join(LIBRARY_RELATIVE_PATH);
    if fixed.exists() {
        return fixed;
    }
    if let Ok(entries) = std::fs::read_dir("/opt/venv/lib") {
        for entry in entries.flatten() {
            let candidate = entry
                .path()
                .join("site-packages")
                .join(LIBRARY_RELATIVE_PATH);
            if candidate.exists() {
                return candidate;
            }
        }
    }
    PathBuf::from("libgribjump.so")
}

struct NativeBackend {
    api: NativeApi,
    handle: *mut Handle,
}

// GribJump handles are used behind the extractor's process-wide mutex.  The C API
// is never entered concurrently for this handle, even if tokio moves jobs between
// blocking threads.
unsafe impl Send for NativeBackend {}

impl NativeBackend {
    fn load() -> Result<Self, String> {
        let api = NativeApi::load()?;
        let mut handle = std::ptr::null_mut();
        // SAFETY: handle points to writable storage and the API has been initialised.
        api.check(
            unsafe { (api.new_handle)(&mut handle) },
            "gribjump_new_handle",
        )?;
        if handle.is_null() {
            return Err("gribjump_new_handle returned a null handle".to_string());
        }
        Ok(Self { api, handle })
    }
}

impl Drop for NativeBackend {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // SAFETY: this backend uniquely owns the handle.
            let _ = unsafe { (self.api.delete_handle)(self.handle) };
        }
    }
}

struct OwnedRequests<'a> {
    api: &'a NativeApi,
    requests: Vec<*mut NativePathRequest>,
}

impl Drop for OwnedRequests<'_> {
    fn drop(&mut self) {
        for request in self.requests.drain(..) {
            // SAFETY: each request was created by this API and is owned exactly once.
            let _ = unsafe { (self.api.delete_path_request)(request) };
        }
    }
}

struct OwnedResults<'a> {
    api: &'a NativeApi,
    results: Vec<*mut ResultHandle>,
}

impl Drop for OwnedResults<'_> {
    fn drop(&mut self) {
        for result in self.results.drain(..) {
            // SAFETY: each result was yielded by the iterator and is owned exactly once.
            let _ = unsafe { (self.api.delete_result)(result) };
        }
    }
}

impl ExtractionBackend for NativeBackend {
    fn extract_batch(
        &mut self,
        requests: &[PathRequest],
        ranges: &[[usize; 2]],
        grid_hash: &str,
        context: Option<&str>,
    ) -> Result<BatchOutput, String> {
        let flat_ranges = ranges.iter().flatten().copied().collect::<Vec<_>>();
        let expected_values = ranges.iter().map(|[start, end]| end - start).sum::<usize>();
        let grid_hash = CString::new(grid_hash).map_err(|error| error.to_string())?;
        let mut owned = OwnedRequests {
            api: &self.api,
            requests: Vec::with_capacity(requests.len()),
        };
        for request in requests {
            let path = CString::new(request.path.as_str()).map_err(|error| error.to_string())?;
            let scheme =
                CString::new(request.scheme.as_str()).map_err(|error| error.to_string())?;
            let host = CString::new(request.host.as_str()).map_err(|error| error.to_string())?;
            let mut native_request = std::ptr::null_mut();
            // SAFETY: every pointer remains valid for the duration of this call and the
            // returned request is transferred to OwnedRequests immediately.
            let code = unsafe {
                (self.api.new_path_request)(
                    &mut native_request,
                    path.as_ptr(),
                    scheme.as_ptr(),
                    request.offset,
                    host.as_ptr(),
                    request.port,
                    flat_ranges.as_ptr(),
                    flat_ranges.len(),
                    grid_hash.as_ptr(),
                )
            };
            self.api.check(code, "gribjump_new_request_from_path")?;
            owned.requests.push(native_request);
        }
        let context = context
            .map(CString::new)
            .transpose()
            .map_err(|error| error.to_string())?;
        let context_pointer = context
            .as_ref()
            .map_or(std::ptr::null(), |value| value.as_ptr());
        let mut iterator = std::ptr::null_mut();
        // SAFETY: handle is exclusively borrowed; request pointers and output storage
        // are valid for the call.
        self.api.check(
            unsafe {
                (self.api.extract_from_paths)(
                    self.handle,
                    owned.requests.as_ptr(),
                    owned.requests.len() as c_ulong,
                    context_pointer,
                    &mut iterator,
                )
            },
            "gribjump_extract_from_paths",
        )?;
        let mut results = OwnedResults {
            api: &self.api,
            results: Vec::with_capacity(requests.len()),
        };
        loop {
            let mut result = std::ptr::null_mut();
            // SAFETY: iterator is owned here and result points to writable storage.
            match unsafe { (self.api.iterator_next)(iterator, &mut result) } {
                0 => results.results.push(result),
                1 => break,
                code => {
                    // SAFETY: iterator was returned by extract_from_paths and is owned here.
                    let _ = unsafe { (self.api.delete_iterator)(iterator) };
                    let error = self
                        .api
                        .check(code, "gribjump_extractioniterator_next")
                        .expect_err("non-zero iterator status must be an error");
                    return Err(error);
                }
            }
        }
        // SAFETY: iterator was returned by extract_from_paths and has not been deleted.
        self.api.check(
            unsafe { (self.api.delete_iterator)(iterator) },
            "gribjump_extractioniterator_delete",
        )?;

        let assemble_started = Instant::now();
        let mut fields = Vec::with_capacity(results.results.len());
        for result in &results.results {
            let mut values = vec![0.0_f64; expected_values];
            let mut values_pointer = values.as_mut_ptr();
            // SAFETY: result is live and values has exactly the expected writable length.
            self.api.check(
                unsafe { (self.api.result_values)(*result, &mut values_pointer, expected_values) },
                "gribjump_result_values",
            )?;
            fields.push(values);
        }
        Ok(BatchOutput {
            fields,
            assemble_time: assemble_started.elapsed(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct FakeState {
        calls: Mutex<Vec<Vec<String>>>,
        completions: Mutex<Vec<usize>>,
        active: AtomicUsize,
        max_active: AtomicUsize,
        fail_offset_once: AtomicUsize,
        delay: AtomicBool,
    }

    #[derive(Clone)]
    struct FakeBackend {
        state: Arc<FakeState>,
    }

    impl ExtractionBackend for FakeBackend {
        fn extract_batch(
            &mut self,
            requests: &[PathRequest],
            ranges: &[[usize; 2]],
            _grid_hash: &str,
            _context: Option<&str>,
        ) -> Result<BatchOutput, String> {
            self.state.calls.lock().unwrap().push(
                requests
                    .iter()
                    .map(|request| request.path.clone())
                    .collect(),
            );
            let active = self.state.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.state.max_active.fetch_max(active, Ordering::AcqRel);
            let first_offset = requests[0].offset;
            if self.state.delay.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis((6 - first_offset.min(5)) as u64 * 5));
            }
            self.state.active.fetch_sub(1, Ordering::AcqRel);
            self.state.completions.lock().unwrap().push(first_offset);
            if self
                .state
                .fail_offset_once
                .compare_exchange(first_offset, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Err(format!("fake failure at offset {first_offset}"));
            }
            let count = ranges.iter().map(|[lo, hi]| hi - lo).sum::<usize>();
            let fields = requests
                .iter()
                .map(|request| {
                    let base = request.offset as f64 * 10.0;
                    (0..count).map(|index| base + index as f64).collect()
                })
                .collect();
            Ok(BatchOutput {
                fields,
                assemble_time: Duration::from_micros(20),
            })
        }
    }

    fn extractor(state: &Arc<FakeState>, inflight: usize, subbatch: usize) -> GribJumpExtractor {
        let backends = (0..inflight)
            .map(|_| {
                Box::new(FakeBackend {
                    state: Arc::clone(state),
                }) as Box<dyn ExtractionBackend>
            })
            .collect();
        GribJumpExtractor {
            pool: BackendPool::new(backends).unwrap(),
            subbatch,
        }
    }

    fn plan(dtype: &str, shuffle: bool) -> ExtractPlan {
        ExtractPlan {
            kind: "rust_gribjump_extract_v1".to_string(),
            paths: (1..=5)
                .map(|offset| PathRequest {
                    path: format!("/field-{offset}"),
                    offset,
                    host: "store".to_string(),
                    port: 9000,
                    scheme: "fdb".to_string(),
                })
                .collect(),
            ranges: vec![[0, 2], [4, 5]],
            grid_hash: "hash".to_string(),
            dtype: dtype.to_string(),
            shuffle,
            zstd_level: 3,
            context: None,
            elements: Vec::new(),
            profile: PlanProfile::default(),
        }
    }

    fn decoded_f64(output: &ExtractOutput, field_count: usize) -> Vec<f64> {
        zstd::bulk::decompress(&output.payload, field_count * 3 * 8)
            .unwrap()
            .chunks_exact(8)
            .map(|bytes| f64::from_le_bytes(bytes.try_into().unwrap()))
            .collect()
    }

    fn request(path: &str, offset: usize) -> PathRequest {
        PathRequest {
            path: path.to_string(),
            offset,
            host: "store".to_string(),
            port: 9000,
            scheme: "fdb".to_string(),
        }
    }

    #[test]
    fn file_aligned_batches_do_not_split_files_that_fit() {
        let paths = vec![
            request("/b", 2),
            request("/a", 2),
            request("/b", 1),
            request("/a", 3),
            request("/a", 1),
        ];
        let batches = file_aligned_batches(&paths, 3);
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0]
                .requests
                .iter()
                .map(|request| (request.path.as_str(), request.offset))
                .collect::<Vec<_>>(),
            vec![("/a", 1), ("/a", 2), ("/a", 3)]
        );
        assert_eq!(
            batches[1]
                .requests
                .iter()
                .map(|request| (request.path.as_str(), request.offset))
                .collect::<Vec<_>>(),
            vec![("/b", 1), ("/b", 2)]
        );
    }

    #[test]
    fn file_aligned_batches_keep_oversized_file_intact() {
        let paths = (1..=4)
            .rev()
            .map(|offset| request("/oversized", offset))
            .collect::<Vec<_>>();
        let batches = file_aligned_batches(&paths, 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .requests
                .iter()
                .map(|request| request.offset)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn regrouping_preserves_payload_order_with_inflight_pool() {
        let state = Arc::new(FakeState::default());
        state.delay.store(true, Ordering::Release);
        let grouped_extractor = extractor(&state, 2, 3);
        let mut input = plan("float64", false);
        input.paths = vec![
            request("/b", 5),
            request("/a", 1),
            request("/b", 3),
            request("/a", 2),
            request("/b", 4),
        ];
        let output = grouped_extractor.extract(&input).unwrap();
        assert_eq!(
            decoded_f64(&output, 5),
            vec![
                50.0, 51.0, 52.0, 10.0, 11.0, 12.0, 30.0, 31.0, 32.0, 20.0, 21.0, 22.0, 40.0, 41.0,
                42.0,
            ]
        );
        let mut baseline = input.clone();
        baseline.paths = input
            .paths
            .iter()
            .enumerate()
            .map(|(index, path_request)| {
                request(&format!("/baseline-{index}"), path_request.offset)
            })
            .collect();
        let baseline_state = Arc::new(FakeState::default());
        let baseline_output = extractor(&baseline_state, 1, 3).extract(&baseline).unwrap();
        assert_eq!(output.payload, baseline_output.payload);
        assert_eq!(output.metrics.gj_subbatches, 2);
        assert_eq!(output.metrics.inflight, 2);
        assert_eq!(state.max_active.load(Ordering::Acquire), 2);
        let calls = state.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(|call| call == &["/a", "/a"]));
        assert!(calls.iter().any(|call| call == &["/b", "/b", "/b"]));
    }

    #[test]
    fn out_of_order_completion_preserves_assembly_order_and_bounds_concurrency() {
        let state = Arc::new(FakeState::default());
        state.delay.store(true, Ordering::Release);
        let extractor = extractor(&state, 3, 1);
        let output = extractor.extract(&plan("float64", false)).unwrap();
        assert_eq!(
            decoded_f64(&output, 5),
            vec![
                10.0, 11.0, 12.0, 20.0, 21.0, 22.0, 30.0, 31.0, 32.0, 40.0, 41.0, 42.0, 50.0, 51.0,
                52.0,
            ]
        );
        assert_ne!(*state.completions.lock().unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(state.max_active.load(Ordering::Acquire), 3);
        assert_eq!(output.metrics.gj_subbatches, 5);
        assert_eq!(output.metrics.inflight, 3);
    }

    #[test]
    fn inflight_one_degenerates_to_sequential() {
        let state = Arc::new(FakeState::default());
        let extractor = extractor(&state, 1, 2);
        let output = extractor.extract(&plan("float64", false)).unwrap();
        assert_eq!(
            *state.calls.lock().unwrap(),
            vec![
                vec!["/field-1".to_string(), "/field-2".to_string()],
                vec!["/field-3".to_string(), "/field-4".to_string()],
                vec!["/field-5".to_string()],
            ]
        );
        assert_eq!(state.max_active.load(Ordering::Acquire), 1);
        assert_eq!(output.metrics.inflight, 1);
    }

    #[test]
    fn failure_propagates_and_handle_returns_to_pool() {
        let state = Arc::new(FakeState::default());
        state.fail_offset_once.store(3, Ordering::Release);
        let extractor = extractor(&state, 2, 1);
        let error = extractor.extract(&plan("float64", false)).unwrap_err();
        assert!(error.contains("fake failure at offset 3"), "{error}");
        let output = extractor.extract(&plan("float64", false)).unwrap();
        assert_eq!(decoded_f64(&output, 5).len(), 15);
        assert_eq!(extractor.pool.available.lock().unwrap().len(), 2);
    }

    #[test]
    fn f32_shuffle_matches_python_golden_vector() {
        let state = Arc::new(FakeState::default());
        let extractor = extractor(&state, 1, DEFAULT_SUBBATCH);
        let mut input = plan("float32", true);
        input.paths.truncate(2);
        let output = extractor.extract(&input).unwrap();
        let shuffled = zstd::bulk::decompress(&output.payload, 2 * 3 * 4).unwrap();
        // np.array([10,11,12,20,21,22], dtype='<f4').view('u1')
        //     .reshape(-1,4).T.tobytes()
        assert_eq!(
            shuffled,
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 48, 64, 160, 168, 176, 65, 65, 65, 65, 65,
                65,
            ]
        );
    }

    #[test]
    fn multi_elements_match_single_payloads_and_share_file_call() {
        let state = Arc::new(FakeState::default());
        let extractor = extractor(&state, 1, DEFAULT_SUBBATCH);
        let paths = (1..=3)
            .map(|offset| PathRequest {
                path: "/shared-file".to_string(),
                offset,
                host: "store".to_string(),
                port: 9000,
                scheme: "fdb".to_string(),
            })
            .collect::<Vec<_>>();
        let single = |indices: &[usize], dtype: &str, shuffle: bool| ExtractPlan {
            kind: "rust_gribjump_extract_v1".to_string(),
            paths: indices.iter().map(|&index| paths[index].clone()).collect(),
            ranges: vec![[0, 3]],
            grid_hash: "hash".to_string(),
            dtype: dtype.to_string(),
            shuffle,
            zstd_level: 3,
            context: None,
            elements: Vec::new(),
            profile: PlanProfile::default(),
        };
        let expected0 = extractor.extract(&single(&[0, 1], "float32", true)).unwrap().payload;
        let expected1 = extractor.extract(&single(&[2], "float64", false)).unwrap().payload;
        state.calls.lock().unwrap().clear();

        let multi = ExtractPlan {
            kind: "rust_gribjump_extract_v2".to_string(),
            paths,
            ranges: Vec::new(),
            grid_hash: String::new(),
            dtype: String::new(),
            shuffle: false,
            zstd_level: 3,
            context: None,
            elements: vec![
                ExtractElementPlan {
                    status: 0, path_indices: vec![0, 1], ranges: vec![[0, 3]],
                    grid_hash: "hash".to_string(), dtype: "float32".to_string(), shuffle: true,
                },
                ExtractElementPlan {
                    status: 0, path_indices: vec![2], ranges: vec![[0, 3]],
                    grid_hash: "hash".to_string(), dtype: "float64".to_string(), shuffle: false,
                },
                ExtractElementPlan {
                    status: 1, path_indices: Vec::new(), ranges: vec![[0, 3]],
                    grid_hash: "hash".to_string(), dtype: "float32".to_string(), shuffle: true,
                },
            ],
            profile: PlanProfile::default(),
        };
        let output = extractor.extract(&multi).unwrap();
        assert_eq!(*state.calls.lock().unwrap(), vec![vec![
            "/shared-file".to_string(), "/shared-file".to_string(), "/shared-file".to_string(),
        ]]);
        assert_eq!(&output.payload[..9], b"PZMC\x01\x03\x00\x00\x00");
        let mut cursor = 9;
        let mut entries = Vec::new();
        for _ in 0..3 {
            let status = output.payload[cursor];
            let length = u64::from_le_bytes(output.payload[cursor + 1..cursor + 9].try_into().unwrap()) as usize;
            entries.push((status, length));
            cursor += 9;
        }
        assert_eq!(entries, vec![(0, expected0.len()), (0, expected1.len()), (1, 0)]);
        assert_eq!(&output.payload[cursor..cursor + expected0.len()], expected0);
        cursor += expected0.len();
        assert_eq!(&output.payload[cursor..], expected1);
    }


    #[test]
    fn rejects_invalid_ranges_before_ffi() {
        let state = Arc::new(FakeState::default());
        let extractor = extractor(&state, 1, DEFAULT_SUBBATCH);
        let mut input = plan("float64", false);
        input.ranges = vec![[3, 3]];
        assert!(
            extractor
                .extract(&input)
                .unwrap_err()
                .contains("invalid extract range")
        );
        assert!(state.calls.lock().unwrap().is_empty());
    }
}
