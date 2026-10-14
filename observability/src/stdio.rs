// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Turns output that native code writes straight to fd 1/2 into log events.
//!
//! C/C++ libraries linked into a service (eckit, gribjump, ...) and Python
//! `warnings`/`print` write to stdout/stderr directly. That text lands right
//! after the service's JSON log lines, and a collector that parses each line
//! as JSON then fails on the combined line and drops all its fields.
//!
//! Output still in the pipe when the process dies without running atexit
//! handlers or raising SIGABRT (`_exit`, SIGKILL, e.g. eckit's `PANIC`) is lost.

use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once};
use std::time::{Duration, Instant};

use crate::active::sole_active_request;

// Lines written in a burst (e.g. a multi-line library message) become one event.
const QUIET_PERIOD: Duration = Duration::from_millis(50);
// Steady trickles are still reported at least this often.
const MAX_BATCH_AGE: Duration = Duration::from_secs(1);
const MAX_BATCH_BYTES: usize = 8 * 1024;

#[derive(Clone, Copy)]
enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    fn name(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

#[derive(Default)]
struct Pending {
    bytes: Vec<u8>,
    since: Option<Instant>,
    request_id: Option<String>,
}

struct Capture {
    stream: Stream,
    pipe: File,
    pending: Mutex<Pending>,
}

static CAPTURES: Mutex<Vec<Arc<Capture>>> = Mutex::new(Vec::new());
static INSTALL_HOOKS: Once = Once::new();
static ORIGINAL_STDERR: AtomicI32 = AtomicI32::new(-1);
static INSTALLED_IN_PID: AtomicU32 = AtomicU32::new(0);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Points fd 1 and fd 2 at pipes that background threads drain into `tracing`
/// events (stdout at INFO, stderr at WARN), and returns the original stdout so
/// the logger can keep writing JSON lines to it. On error fd 1/2 are left as
/// they were.
pub fn redirect_native_stdio() -> io::Result<File> {
    let original_out = dup(1)?;
    let original_err = dup(2)?;
    let (out_read, out_write) = pipe()?;
    let (err_read, err_write) = pipe()?;

    // Start draining before anything is pointed at the pipes.
    let captures = [
        Arc::new(Capture::new(Stream::Stdout, out_read)),
        Arc::new(Capture::new(Stream::Stderr, err_read)),
    ];
    for capture in &captures {
        spawn_drain(capture.clone())?;
    }

    let redirected = dup2(&out_write, 1).and_then(|()| dup2(&err_write, 2));
    if let Err(err) = redirected {
        let _ = dup2(&original_out, 1);
        let _ = dup2(&original_err, 2);
        return Err(err);
    }
    drop((out_write, err_write));

    lock(&CAPTURES).extend(captures);
    INSTALL_HOOKS.call_once(|| {
        ORIGINAL_STDERR.store(
            original_err
                .try_clone()
                .map_or(-1, std::os::fd::IntoRawFd::into_raw_fd),
            Ordering::Relaxed,
        );
        INSTALLED_IN_PID.store(std::process::id(), Ordering::Relaxed);
        unsafe {
            libc::atexit(flush_at_exit);
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = dump_on_abort as extern "C" fn(libc::c_int) as usize;
            action.sa_flags = libc::SA_RESETHAND;
            libc::sigemptyset(&mut action.sa_mask);
            libc::sigaction(libc::SIGABRT, &action, std::ptr::null_mut());
        }
    });
    Ok(File::from(original_out))
}

extern "C" fn flush_at_exit() {
    if std::process::id() != INSTALLED_IN_PID.load(Ordering::Relaxed) {
        return; // a forked child must not touch the parent's pipes
    }
    // C stdio buffers are flushed by `exit` only after the atexit handlers.
    unsafe { libc::fflush(std::ptr::null_mut()) };
    // On a fresh thread: the exiting thread's thread-locals (which `tracing`
    // uses) may already be destroyed, and a panic here would abort the process.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let spawned = std::thread::Builder::new()
        .name("native-stdio-exit".into())
        .spawn(move || {
            let captures = lock(&CAPTURES).clone();
            for capture in captures {
                capture.flush_everything_written();
            }
            let _ = done_tx.send(());
        });
    if spawned.is_ok() {
        let _ = done_rx.recv_timeout(Duration::from_secs(2));
    }
}

/// `abort()` (C++ `terminate`, Rust allocation failures, ...) kills the process
/// without running atexit handlers, so whatever the dying process printed
/// last ("terminate called after throwing ...") would be lost in the pipes.
/// Copies it, raw, to the original stderr. Never blocks for long: it only try-locks.
extern "C" fn dump_on_abort(_signal: libc::c_int) {
    let fd = ORIGINAL_STDERR.load(Ordering::Relaxed);
    if fd < 0 {
        return;
    }
    let Ok(captures) = CAPTURES.try_lock() else {
        return;
    };
    for capture in captures.iter() {
        // The drain thread holds this lock for the instant between reading the
        // dying process's last words and batching them; wait that out, but not
        // forever (the aborting thread may itself hold it).
        for _ in 0..100 {
            if let Ok(pending) = capture.pending.try_lock() {
                write_raw(fd, &pending.bytes);
                break;
            }
            unsafe { libc::usleep(500) };
        }
        let pipe = capture.pipe.as_raw_fd();
        let mut chunk = [0u8; 4096];
        unsafe {
            let flags = libc::fcntl(pipe, libc::F_GETFL);
            libc::fcntl(pipe, libc::F_SETFL, flags | libc::O_NONBLOCK);
            loop {
                let n = libc::read(pipe, chunk.as_mut_ptr().cast(), chunk.len());
                if n <= 0 {
                    break;
                }
                write_raw(fd, &chunk[..n as usize]);
            }
        }
    }
}

fn write_raw(fd: libc::c_int, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let n = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if n <= 0 {
            return;
        }
        bytes = &bytes[n as usize..];
    }
}

fn cvt(ret: libc::c_int) -> io::Result<libc::c_int> {
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

// Descriptors are kept >= 3 so a closed stdin/stdout/stderr can't make a pipe
// end land on fd 0-2 and be overwritten by the redirect.
fn dup(fd: libc::c_int) -> io::Result<OwnedFd> {
    let new = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(new) })
}

fn dup2(from: &OwnedFd, to: libc::c_int) -> io::Result<()> {
    cvt(unsafe { libc::dup2(from.as_raw_fd(), to) }).map(|_| ())
}

fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as libc::c_int; 2];
    cvt(unsafe { libc::pipe(fds.as_mut_ptr()) })?;
    let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    Ok((dup(read.as_raw_fd())?, dup(write.as_raw_fd())?))
}

fn spawn_drain(capture: Arc<Capture>) -> io::Result<()> {
    std::thread::Builder::new()
        .name(format!("native-{}", capture.stream.name()))
        .spawn(move || capture.drain())
        .map(|_| ())
}

impl Capture {
    fn new(stream: Stream, read: OwnedFd) -> Self {
        Self {
            stream,
            pipe: File::from(read),
            pending: Mutex::new(Pending::default()),
        }
    }

    fn drain(&self) {
        loop {
            let mut pollfd = libc::pollfd {
                fd: self.pipe.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let ready = unsafe { libc::poll(&mut pollfd, 1, self.poll_timeout_ms()) };
            if ready < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                break;
            }
            if ready == 0 {
                self.flush();
                continue;
            }
            if !self.read_available() {
                break;
            }
        }
        self.flush();
    }

    fn poll_timeout_ms(&self) -> i32 {
        let pending = lock(&self.pending);
        let Some(since) = pending.since else {
            return -1;
        };
        let age_left = MAX_BATCH_AGE.saturating_sub(since.elapsed());
        QUIET_PERIOD.min(age_left).as_millis() as i32
    }

    /// Reads what is in the pipe into the pending batch. `false` at EOF/error.
    fn read_available(&self) -> bool {
        let mut pending = lock(&self.pending);
        let mut chunk = [0u8; 8192];
        loop {
            match (&self.pipe).read(&mut chunk) {
                Ok(0) => return false,
                Ok(n) => {
                    self.append(&mut pending, &chunk[..n]);
                    return true;
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return true,
                Err(_) => return false,
            }
        }
    }

    fn append(&self, pending: &mut Pending, data: &[u8]) {
        // Output arriving while a different request is in progress must not be
        // folded into the previous request's batch.
        let request_id = sole_active_request();
        if !pending.bytes.is_empty() && pending.request_id != request_id {
            self.emit_all(pending);
        }
        if pending.bytes.is_empty() {
            pending.since = Some(Instant::now());
            pending.request_id = request_id;
        }
        pending.bytes.extend_from_slice(data);

        while pending.bytes.len() >= MAX_BATCH_BYTES {
            let cut = split_point(&pending.bytes, MAX_BATCH_BYTES);
            let head: Vec<u8> = pending.bytes.drain(..cut).collect();
            self.emit(pending.request_id.as_deref(), &head);
        }
        if pending.since.is_some_and(|s| s.elapsed() >= MAX_BATCH_AGE) {
            self.emit_all(pending);
        }
    }

    fn emit_all(&self, pending: &mut Pending) {
        let bytes = std::mem::take(&mut pending.bytes);
        pending.since = None;
        self.emit(pending.request_id.as_deref(), &bytes);
    }

    fn flush(&self) {
        let mut pending = lock(&self.pending);
        self.emit_all(&mut pending);
    }

    /// At process exit nothing drains the pipe any more (and it never reaches
    /// EOF, since this process holds the write end): emit whatever is left.
    fn flush_everything_written(&self) {
        unsafe {
            let fd = self.pipe.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFL);
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        let mut pending = lock(&self.pending);
        let mut chunk = [0u8; 8192];
        loop {
            match (&self.pipe).read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.append(&mut pending, &chunk[..n]),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        self.emit_all(&mut pending);
    }

    fn emit(&self, request_id: Option<&str>, bytes: &[u8]) {
        let text = String::from_utf8_lossy(bytes);
        let text = text.trim_matches(|c: char| c == '\n' || c == '\r' || c == ' ');
        if text.is_empty() {
            return;
        }
        let stream = self.stream.name();
        // `event!` needs a constant level, hence one arm per level.
        macro_rules! log {
            ($level:expr) => {
                tracing::event!(
                    target: "native_stdio",
                    $level,
                    { "event.name" = "process.native_output", "log.iostream" = stream, request.id = request_id },
                    "{text}"
                )
            };
        }
        match self.stream {
            Stream::Stdout => log!(tracing::Level::INFO),
            Stream::Stderr => log!(tracing::Level::WARN),
        }
    }
}

/// Where to cut a batch of at least `max` bytes: after the last newline within
/// `max`, else at `max` (backed up so a UTF-8 character isn't split).
fn split_point(bytes: &[u8], max: usize) -> usize {
    if let Some(newline) = bytes[..max].iter().rposition(|b| *b == b'\n') {
        return newline + 1;
    }
    let mut cut = max;
    while cut > 0 && bytes.get(cut).is_some_and(|b| b & 0xC0 == 0x80) {
        cut -= 1;
    }
    if cut == 0 { max } else { cut }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_are_cut_at_a_line_boundary() {
        let bytes = b"aaaa\nbbbb\ncc";
        assert_eq!(split_point(bytes, 11), 10);
    }

    #[test]
    fn unbroken_batches_are_cut_on_a_character_boundary() {
        let bytes = "ééééé".as_bytes(); // 2 bytes each
        assert_eq!(split_point(bytes, 5), 4);
        assert_eq!(split_point(&[b'x'; 10], 5), 5);
    }
}
