// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Turns output that native code writes straight to fd 1/2 into log events.
//!
//! C/C++ libraries linked into a service (eckit, gribjump, ...) and Python
//! `warnings`/`print` write to stdout/stderr directly. That text lands right
//! after the service's JSON log lines, and a collector that parses each line
//! as JSON then fails on the combined line and drops all its fields.

use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use crate::active::sole_active_request;

// Lines written in a burst (e.g. a multi-line library message) become one event.
const QUIET_PERIOD: Duration = Duration::from_millis(50);
// Steady trickles are still reported at least this often.
const MAX_BATCH_AGE: Duration = Duration::from_secs(1);
const MAX_BATCH_BYTES: usize = 8 * 1024;

// eckit constructs (and prints) this exception for the first out-of-range value
// when metkit expands a `time=a/to/b` list, then catches it to end the list.
const HANDLED_EXCEPTIONS: &[&str] = &["Exception: Bad value: Wrong input for time: 24 hours"];

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
static EXIT_FLUSH: Once = Once::new();

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

    CAPTURES.lock().unwrap().extend(captures);
    EXIT_FLUSH.call_once(|| unsafe {
        libc::atexit(flush_at_exit);
    });
    Ok(File::from(original_out))
}

extern "C" fn flush_at_exit() {
    // On a fresh thread: the exiting thread's thread-locals (which `tracing`
    // uses) may already be destroyed, and a panic here would abort the process.
    let _ = std::thread::spawn(|| {
        let captures = CAPTURES.lock().map(|c| c.clone()).unwrap_or_default();
        for capture in captures {
            capture.flush_everything_written();
        }
    })
    .join();
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
        let pending = self.pending.lock().unwrap();
        let Some(since) = pending.since else {
            return -1;
        };
        let age_left = MAX_BATCH_AGE.saturating_sub(since.elapsed());
        QUIET_PERIOD.min(age_left).as_millis() as i32
    }

    /// Reads what is in the pipe into the pending batch. `false` at EOF/error.
    fn read_available(&self) -> bool {
        let mut pending = self.pending.lock().unwrap();
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
        let mut pending = self.pending.lock().unwrap();
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
        let mut pending = self.pending.lock().unwrap();
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
        match level_for(self.stream, text) {
            tracing::Level::DEBUG => log!(tracing::Level::DEBUG),
            tracing::Level::INFO => log!(tracing::Level::INFO),
            _ => log!(tracing::Level::WARN),
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

// eckit prints "Exception: ..." whenever an exception object is constructed,
// including ones its callers catch. Known handled ones are noise; any other
// is worth seeing whichever fd it came through.
fn level_for(stream: Stream, text: &str) -> tracing::Level {
    let is_handled = |line: &str| HANDLED_EXCEPTIONS.iter().any(|h| line.starts_with(h));
    if text.lines().all(is_handled) {
        return tracing::Level::DEBUG;
    }
    if text.lines().any(|line| line.starts_with("Exception: ")) {
        return tracing::Level::WARN;
    }
    match stream {
        Stream::Stdout => tracing::Level::INFO,
        Stream::Stderr => tracing::Level::WARN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    #[test]
    fn known_handled_eckit_exception_is_debug() {
        let text = "Exception: Bad value: Wrong input for time: 24 hours 0 minutes 0 seconds";
        assert_eq!(level_for(Stream::Stdout, text), Level::DEBUG);
        assert_eq!(level_for(Stream::Stderr, text), Level::DEBUG);
    }

    #[test]
    fn other_exceptions_are_not_hidden() {
        let text = "Exception: SeriousBug: RequestEnvironment not initialized";
        assert_eq!(level_for(Stream::Stdout, text), Level::WARN);
        let mixed = "Exception: Bad value: Wrong input for time: 24 hours\nSerious bug: y";
        assert_eq!(level_for(Stream::Stdout, mixed), Level::WARN);
    }

    #[test]
    fn other_output_keeps_the_stream_level() {
        assert_eq!(level_for(Stream::Stdout, "Starting 7 threads"), Level::INFO);
        assert_eq!(
            level_for(Stream::Stderr, "Duplicate values in date [1,1] 1"),
            Level::WARN
        );
    }

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
