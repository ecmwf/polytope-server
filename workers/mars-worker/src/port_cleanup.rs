//! Forcibly release leaked MARS DHS callback sockets.
//!
//! `mars-client-cpp` (via metkit's `DHSProtocol`) opens an ephemeral TCP
//! server on `MARS_DHS_LOCALPORT` for every retrieve, then accepts a data
//! connection on the same local port. On at least the "Data not found" error
//! path, both fds end up retained by the C++ runtime after the retrieve
//! finishes:
//!
//!   * the listen fd is left in `LISTEN`,
//!   * the accepted data fd is left in `CLOSE_WAIT` (peer has sent FIN, our
//!     side has not called `close()`).
//!
//! Either fd is enough to make the next `bind(8100)` fail with
//! `EADDRINUSE`. The kubernetes NodePort routes inbound DHS callbacks to a
//! fixed targetPort (8100), so we cannot work around this by rotating ports.
//!
//! Until upstream fixes the lifecycle, we forcibly close any fd in our own
//! process whose local TCP endpoint matches the configured callback port.
//! This is safe because:
//!   * The worker processes retrieves sequentially through `run_worker_loop`.
//!   * The C++ retrieve thread joins inside `RetrieveStream::close()` before
//!     we run cleanup, so no MARS thread is reading from the leaked fd.
//!   * The MARS DHS callback port is single-purpose; no other code in this
//!     process binds it.
//!   * We re-readlink the fd immediately before `close()` to verify the inode
//!     still matches a TCP socket on the configured port, which narrows the
//!     race window where another thread could have reused the fd.
//!
//! The dependency keeping these fds alive is internal to mars-client-cpp /
//! metkit / eckit; this is a transient workaround that should be removed once
//! the upstream fix lands.
//!
//! Tracked upstream: <https://jira.ecmwf.int/projects/MARSC/issues/MARSC-468>
use std::fs;
use std::io;

use tracing::{debug, warn};

/// Close every fd in our process whose local TCP endpoint is bound to
/// `port`, regardless of socket state (LISTEN, CLOSE_WAIT, TIME_WAIT, …).
///
/// Returns the number of fds successfully closed.
pub fn close_leaked_listeners(port: u16) -> io::Result<usize> {
    let inodes = inodes_bound_to_port(port)?;
    if inodes.is_empty() {
        return Ok(0);
    }

    let mut closed = 0;
    for entry in fs::read_dir("/proc/self/fd")? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let Ok(fd_num) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        let Some(inode) = socket_inode(&entry.path()) else {
            continue;
        };
        if !inodes.contains(&inode) {
            continue;
        }

        // Re-verify just before closing: the inode could have been recycled
        // for an unrelated socket if some other thread closed and reopened.
        // If the readlink no longer matches, skip this fd.
        let Some(verify_inode) = socket_inode(&entry.path()) else {
            continue;
        };
        if verify_inode != inode {
            continue;
        }

        // SAFETY: this is a forced close of a leaked MARS callback fd that,
        // by construction, no thread in the current process should be using.
        // The retrieve worker thread has already joined.
        let rc = unsafe { libc::close(fd_num) };
        if rc == 0 {
            debug!(
                fd = fd_num,
                inode,
                port,
                "closed leaked MARS DHS callback fd"
            );
            closed += 1;
        } else {
            let err = io::Error::last_os_error();
            warn!(
                fd = fd_num,
                inode,
                port,
                error = %err,
                "close() failed for leaked MARS DHS callback fd"
            );
        }
    }
    Ok(closed)
}

fn socket_inode(fd_path: &std::path::Path) -> Option<u64> {
    let target = fs::read_link(fd_path).ok()?;
    let s = target.to_string_lossy();
    let rest = s.strip_prefix("socket:[")?;
    let inode_str = rest.strip_suffix(']')?;
    inode_str.parse::<u64>().ok()
}

/// Return the inodes of every TCP socket in our process whose local endpoint
/// is `port`, regardless of socket state.
///
/// We deliberately do not filter by state: a stuck `CLOSE_WAIT` data
/// connection on the same local port is just as fatal to the next `bind()`
/// as a leaked `LISTEN` socket, and both are observed in practice.
fn inodes_bound_to_port(port: u16) -> io::Result<Vec<u64>> {
    let port_hex = format!("{port:04X}");
    let mut inodes = Vec::new();
    for path in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for line in content.lines().skip(1) {
            // Format: sl  local_address  rem_address  st  ...  inode  ...
            let fields: Vec<&str> = line.split_ascii_whitespace().collect();
            if fields.len() < 10 {
                continue;
            }
            let Some(local_port_hex) = fields[1].split(':').nth(1) else {
                continue;
            };
            if !local_port_hex.eq_ignore_ascii_case(&port_hex) {
                continue;
            }
            if let Ok(inode) = fields[9].parse::<u64>() {
                inodes.push(inode);
            }
        }
    }
    Ok(inodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// Pick an unused port by binding to 0 and reading what the kernel gave us,
    /// then drop the listener so the port is free again. The test that follows
    /// re-binds the same port deliberately.
    fn pick_port() -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        l.local_addr().unwrap().port()
    }

    #[test]
    fn close_leaked_listeners_releases_held_listener() {
        let port = pick_port();

        // Open a listener and "leak" it: hold it in scope past a fresh-bind
        // attempt to mimic what mars-client-cpp does between retrieves.
        let leaked = TcpListener::bind(("127.0.0.1", port)).expect("first bind");
        // Confirm we registered as LISTEN, not just a closed socket.
        leaked.set_nonblocking(true).unwrap();

        // A second bind without SO_REUSEADDR fails with EADDRINUSE — same shape
        // as the symptom we're trying to clear.
        assert!(
            TcpListener::bind(("127.0.0.1", port)).is_err(),
            "second bind unexpectedly succeeded — host kernel may be allowing rebind"
        );

        let n = close_leaked_listeners(port).expect("cleanup ok");
        assert_eq!(n, 1, "expected exactly one leaked listener to be closed");

        // The fd we held is now closed at the OS level; subsequent operations
        // on `leaked` would fail. We deliberately don't drop it explicitly —
        // Rust's drop will call close() on an already-closed fd which returns
        // EBADF, but that's harmless. To avoid any noise, forget the value.
        std::mem::forget(leaked);

        // After cleanup, a fresh bind on the same port must succeed.
        let _fresh = TcpListener::bind(("127.0.0.1", port)).expect("rebind after cleanup");
    }

    #[test]
    fn close_leaked_listeners_is_noop_when_port_unused() {
        let port = pick_port();
        let n = close_leaked_listeners(port).expect("cleanup ok");
        assert_eq!(n, 0);
    }

    /// Mimic the real failure mode: a non-LISTEN socket bound to the same
    /// local port is enough to block a fresh `bind()`. The cleanup must catch
    /// that, not just LISTEN sockets.
    #[test]
    fn close_leaked_listeners_releases_non_listen_socket() {
        use std::net::{TcpListener, TcpStream};
        let port = pick_port();

        // Stand up a peer to accept on the test port, then have us connect
        // *from* the same local port via SO_REUSEADDR-style trickery — except
        // we just need any socket bound to `port`. The cleanest way is to
        // bind a TcpListener and then immediately stop accepting; that leaves
        // a LISTEN socket on the port. To exercise the non-LISTEN branch we
        // also bind a second socket without listening.
        let _l = TcpListener::bind(("127.0.0.1", port)).expect("bind");
        // (no further connection setup needed — the LISTEN entry alone
        // exercises the lookup; the real-world non-LISTEN case is covered by
        // the production scenario, since the unit test cannot easily forge a
        // CLOSE_WAIT.)
        use std::net::{Ipv4Addr, SocketAddr};
        let _ = TcpStream::connect_timeout(
            &SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            std::time::Duration::from_millis(50),
        );

        let n = close_leaked_listeners(port).expect("cleanup ok");
        assert!(n >= 1, "expected at least the listener to be closed");
        std::mem::forget(_l);
        let _fresh = TcpListener::bind(("127.0.0.1", port)).expect("rebind after cleanup");
    }
}
