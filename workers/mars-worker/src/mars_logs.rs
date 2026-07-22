// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::ffi::{CStr, c_char, c_void};
use std::fmt::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const CHUNK_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CHUNK_BYTES: usize = 64 * 1024;

static BRIDGE: OnceLock<MarsLogBridge> = OnceLock::new();

unsafe extern "C" {
    fn eckit_install_log_bridge(
        callback: unsafe extern "C" fn(*mut c_void, u8, *const c_char),
        context: *mut c_void,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn from_native(level: u8) -> Self {
        match level {
            0 => Self::Debug,
            1 => Self::Info,
            2 => Self::Warn,
            _ => Self::Error,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkReason {
    Interval,
    RequestComplete,
    SizeLimit,
}

impl ChunkReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Interval => "interval",
            Self::RequestComplete => "request_complete",
            Self::SizeLimit => "size_limit",
        }
    }
}

#[derive(Debug, Default)]
struct ChunkBuffer {
    request_id: Option<String>,
    message: String,
    line_count: usize,
    max_level: Option<LogLevel>,
}

impl ChunkBuffer {
    fn push(&mut self, level: LogLevel, message: &str, max_bytes: usize) -> Vec<Chunk> {
        let (rendered, line_count) = render_message(level, message);
        let mut chunks = Vec::with_capacity(2);

        if !self.message.is_empty() && self.message.len() + rendered.len() > max_bytes {
            if let Some(chunk) = self.drain(ChunkReason::SizeLimit) {
                chunks.push(chunk);
            }
        }

        self.message.push_str(&rendered);
        self.line_count += line_count;
        self.max_level = Some(self.max_level.map_or(level, |current| current.max(level)));

        if self.message.len() >= max_bytes {
            if let Some(chunk) = self.drain(ChunkReason::SizeLimit) {
                chunks.push(chunk);
            }
        }

        chunks
    }

    fn drain(&mut self, reason: ChunkReason) -> Option<Chunk> {
        if self.message.is_empty() {
            return None;
        }

        let message = std::mem::take(&mut self.message);
        let chunk_bytes = message.len();
        Some(Chunk {
            request_id: self.request_id.clone(),
            message,
            line_count: std::mem::take(&mut self.line_count),
            chunk_bytes,
            max_level: self
                .max_level
                .take()
                .expect("non-empty MARS log chunk must have a level"),
            reason,
        })
    }
}

fn render_message(level: LogLevel, message: &str) -> (String, usize) {
    let message = message.trim_end_matches(['\r', '\n']);
    let mut rendered = String::new();
    let mut line_count = 0;

    if message.is_empty() {
        writeln!(rendered, "[{}]", level.label()).expect("writing to String cannot fail");
        return (rendered, 1);
    }

    for line in message.split('\n') {
        writeln!(
            rendered,
            "[{}] {}",
            level.label(),
            line.trim_end_matches('\r')
        )
        .expect("writing to String cannot fail");
        line_count += 1;
    }

    (rendered, line_count)
}

#[derive(Debug)]
struct Chunk {
    request_id: Option<String>,
    message: String,
    line_count: usize,
    chunk_bytes: usize,
    max_level: LogLevel,
    reason: ChunkReason,
}

impl Chunk {
    fn emit(self) {
        let request_id = self.request_id.as_deref().unwrap_or("unscoped");
        let reason = self.reason.as_str();
        match self.max_level {
            LogLevel::Debug => tracing::debug!(
                "event.name" = "mars.logs",
                "request.id" = request_id,
                line_count = self.line_count,
                chunk_bytes = self.chunk_bytes,
                chunk_reason = reason,
                "{}",
                self.message
            ),
            LogLevel::Info => tracing::info!(
                "event.name" = "mars.logs",
                "request.id" = request_id,
                line_count = self.line_count,
                chunk_bytes = self.chunk_bytes,
                chunk_reason = reason,
                "{}",
                self.message
            ),
            LogLevel::Warn => tracing::warn!(
                "event.name" = "mars.logs",
                "request.id" = request_id,
                line_count = self.line_count,
                chunk_bytes = self.chunk_bytes,
                chunk_reason = reason,
                "{}",
                self.message
            ),
            LogLevel::Error => tracing::error!(
                "event.name" = "mars.logs",
                "request.id" = request_id,
                line_count = self.line_count,
                chunk_bytes = self.chunk_bytes,
                chunk_reason = reason,
                "{}",
                self.message
            ),
        }
    }
}

struct Inner {
    buffer: Mutex<ChunkBuffer>,
    chunks: tokio::sync::mpsc::UnboundedSender<Chunk>,
    max_chunk_bytes: usize,
}

#[derive(Clone)]
pub(crate) struct MarsLogBridge {
    inner: Arc<Inner>,
}

impl MarsLogBridge {
    fn new(max_chunk_bytes: usize) -> (Self, tokio::sync::mpsc::UnboundedReceiver<Chunk>) {
        let (chunks, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                inner: Arc::new(Inner {
                    buffer: Mutex::new(ChunkBuffer::default()),
                    chunks,
                    max_chunk_bytes,
                }),
            },
            receiver,
        )
    }

    fn record(&self, level: LogLevel, message: &str) {
        let mut buffer = self
            .inner
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let chunks = buffer.push(level, message, self.inner.max_chunk_bytes);
        for chunk in chunks {
            let _ = self.inner.chunks.send(chunk);
        }
    }

    fn flush(&self, reason: ChunkReason) {
        let mut buffer = self
            .inner
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(chunk) = buffer.drain(reason) {
            let _ = self.inner.chunks.send(chunk);
        }
    }

    pub(crate) fn begin_request(&self, request_id: String) -> RequestLogGuard {
        self.flush(ChunkReason::RequestComplete);
        self.inner
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .request_id = Some(request_id.clone());
        RequestLogGuard {
            bridge: self.clone(),
            request_id,
        }
    }

    fn finish_request(&self, request_id: &str) {
        self.flush(ChunkReason::RequestComplete);
        let mut buffer = self
            .inner
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if buffer.request_id.as_deref() == Some(request_id) {
            buffer.request_id = None;
        }
    }
}

pub(crate) struct RequestLogGuard {
    bridge: MarsLogBridge,
    request_id: String,
}

impl Drop for RequestLogGuard {
    fn drop(&mut self) {
        self.bridge.finish_request(&self.request_id);
    }
}

unsafe extern "C" fn eckit_log_callback(_context: *mut c_void, level: u8, message: *const c_char) {
    if message.is_null() {
        return;
    }
    let Some(bridge) = BRIDGE.get() else {
        return;
    };
    // SAFETY: eckit invokes the callback with a non-null, NUL-terminated string
    // that remains valid for the duration of this call.
    let message = unsafe { CStr::from_ptr(message) }.to_string_lossy();
    bridge.record(LogLevel::from_native(level), &message);
}

async fn emit_chunks(mut chunks: tokio::sync::mpsc::UnboundedReceiver<Chunk>) {
    while let Some(chunk) = chunks.recv().await {
        chunk.emit();
    }
}

async fn flush_periodically(bridge: MarsLogBridge, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        bridge.flush(ChunkReason::Interval);
    }
}

pub(crate) fn init() -> MarsLogBridge {
    if let Some(bridge) = BRIDGE.get() {
        return bridge.clone();
    }

    let (bridge, chunks) = MarsLogBridge::new(MAX_CHUNK_BYTES);
    if BRIDGE.set(bridge.clone()).is_err() {
        return BRIDGE.get().expect("MARS log bridge initialized").clone();
    }

    tokio::spawn(emit_chunks(chunks));
    tokio::spawn(flush_periodically(bridge.clone(), CHUNK_INTERVAL));

    // SAFETY: the callback has C ABI and remains valid for the process lifetime;
    // it uses the process-lifetime BRIDGE rather than the null context pointer.
    unsafe { eckit_install_log_bridge(eckit_log_callback, std::ptr::null_mut()) };

    bridge
}

#[cfg(test)]
pub(crate) fn test_instance() -> MarsLogBridge {
    let (bridge, _chunks) = MarsLogBridge::new(MAX_CHUNK_BYTES);
    bridge
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bridge(
        max_chunk_bytes: usize,
    ) -> (MarsLogBridge, tokio::sync::mpsc::UnboundedReceiver<Chunk>) {
        MarsLogBridge::new(max_chunk_bytes)
    }

    fn take_chunk(chunks: &mut tokio::sync::mpsc::UnboundedReceiver<Chunk>) -> Chunk {
        match chunks.try_recv() {
            Ok(chunk) => chunk,
            Err(error) => panic!("expected a MARS log chunk: {error}"),
        }
    }

    async fn wait_for_chunk(chunks: &mut tokio::sync::mpsc::UnboundedReceiver<Chunk>) -> Chunk {
        match tokio::time::timeout(Duration::from_millis(200), chunks.recv()).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => panic!("MARS log chunk sender closed"),
            Err(_) => panic!("periodic MARS log flush timed out"),
        }
    }

    #[test]
    fn chunk_preserves_order_and_uses_highest_level() {
        let (bridge, mut chunks) = test_bridge(1024);
        bridge.record(LogLevel::Info, "first\n");
        bridge.record(LogLevel::Debug, "second");
        bridge.record(LogLevel::Error, "third");
        bridge.flush(ChunkReason::Interval);

        let chunk = take_chunk(&mut chunks);
        assert_eq!(
            chunk.message,
            "[INFO] first\n[DEBUG] second\n[ERROR] third\n"
        );
        assert_eq!(chunk.line_count, 3);
        assert_eq!(chunk.max_level, LogLevel::Error);
        assert_eq!(chunk.reason, ChunkReason::Interval);
    }

    #[test]
    fn size_limit_flushes_early() {
        let (bridge, mut chunks) = test_bridge(20);
        bridge.record(LogLevel::Info, "1234567890");
        bridge.record(LogLevel::Info, "abcdefghij");

        let first = take_chunk(&mut chunks);
        assert_eq!(first.message, "[INFO] 1234567890\n");
        assert_eq!(first.reason, ChunkReason::SizeLimit);

        bridge.flush(ChunkReason::Interval);
        let second = take_chunk(&mut chunks);
        assert_eq!(second.message, "[INFO] abcdefghij\n");
    }

    #[test]
    fn request_completion_flushes_with_request_id() {
        let (bridge, mut chunks) = test_bridge(1024);
        let guard = bridge.begin_request("request-123".to_string());
        bridge.record(LogLevel::Warn, "slow request");
        drop(guard);

        let chunk = take_chunk(&mut chunks);
        assert_eq!(chunk.request_id.as_deref(), Some("request-123"));
        assert_eq!(chunk.reason, ChunkReason::RequestComplete);
    }

    #[test]
    fn empty_flush_emits_nothing() {
        let (bridge, mut chunks) = test_bridge(1024);
        bridge.flush(ChunkReason::Interval);
        assert!(matches!(
            chunks.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn periodic_flush_does_not_wait_for_request_completion() {
        let (bridge, mut chunks) = test_bridge(1024);
        let task = tokio::spawn(flush_periodically(
            bridge.clone(),
            Duration::from_millis(10),
        ));
        let _guard = bridge.begin_request("hung-request".to_string());
        bridge.record(LogLevel::Info, "MARS is still working");

        let chunk = wait_for_chunk(&mut chunks).await;
        assert_eq!(chunk.request_id.as_deref(), Some("hung-request"));
        assert_eq!(chunk.reason, ChunkReason::Interval);
        task.abort();
    }

    #[tokio::test]
    async fn continuous_logging_does_not_postpone_periodic_flush() {
        let (bridge, mut chunks) = test_bridge(4096);
        let flush_task = tokio::spawn(flush_periodically(
            bridge.clone(),
            Duration::from_millis(20),
        ));
        let producer_bridge = bridge.clone();
        let producer = tokio::spawn(async move {
            for _ in 0..100 {
                producer_bridge.record(LogLevel::Info, "still working");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let chunk = wait_for_chunk(&mut chunks).await;
        assert_eq!(chunk.reason, ChunkReason::Interval);
        assert!(!producer.is_finished());

        producer.abort();
        flush_task.abort();
    }
}
