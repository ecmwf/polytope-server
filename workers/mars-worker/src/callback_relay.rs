// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Generation-scoped TCP relay for MARS DHS callbacks.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, warn};

const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const ACCEPT_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const PORT_SELECTION_ATTEMPTS: usize = 1024;
const MAX_ACTIVE_CONNECTIONS: usize = 64;

#[derive(Clone)]
struct ActiveGeneration {
    id: u64,
    target: SocketAddr,
    cancel: watch::Sender<bool>,
}

#[derive(Default)]
struct RelayState {
    next_generation: u64,
    active: Option<ActiveGeneration>,
    last_port: Option<u16>,
}

/// Starts retrieval generations and assigns each one a fresh local callback port.
#[derive(Clone)]
pub struct RelayController {
    state: Arc<Mutex<RelayState>>,
}

/// Keeps one callback target active for the lifetime of a MARS retrieval.
pub struct RelayGeneration {
    controller: RelayController,
    id: u64,
    target_port: u16,
}

impl RelayGeneration {
    pub fn target_port(&self) -> u16 {
        self.target_port
    }
}

impl Drop for RelayGeneration {
    fn drop(&mut self) {
        self.controller.end_generation(self.id);
    }
}

impl RelayController {
    /// Cancel any previous generation and register a fresh, currently-free port.
    ///
    /// Callers must keep the returned guard alive until the MARS retrieval and
    /// all reads from its stream have finished.
    pub fn start_generation(&self) -> io::Result<RelayGeneration> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cancel_active(&mut state);

        let target_port = reserve_fresh_port(&mut state.last_port)?;
        state.next_generation = state.next_generation.wrapping_add(1);
        let id = state.next_generation;
        let (cancel, _cancel_rx) = watch::channel(false);
        state.active = Some(ActiveGeneration {
            id,
            target: SocketAddr::from((Ipv4Addr::LOCALHOST, target_port)),
            cancel,
        });

        debug!(
            generation = id,
            target_port, "started MARS callback relay generation"
        );
        Ok(RelayGeneration {
            controller: self.clone(),
            id,
            target_port,
        })
    }

    fn end_generation(&self, id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active.as_ref().map(|active| active.id) == Some(id) {
            cancel_active(&mut state);
            debug!(generation = id, "ended MARS callback relay generation");
        }
    }

    fn cancel_active(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cancel_active(&mut state);
    }
}

fn cancel_active(state: &mut RelayState) {
    if let Some(active) = state.active.take() {
        active.cancel.send_replace(true);
    }
}

fn reserve_fresh_port(last_port: &mut Option<u16>) -> io::Result<u16> {
    for _ in 0..PORT_SELECTION_ATTEMPTS {
        // Bind on all interfaces so the selected port is also available to a
        // MARS listener that binds 0.0.0.0 rather than loopback specifically.
        let probe = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        let port = probe.local_addr()?.port();
        if Some(port) != *last_port {
            *last_port = Some(port);
            drop(probe);
            return Ok(port);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AddrNotAvailable,
        "could not select a fresh MARS callback port",
    ))
}

/// Always-on listener targeted by the per-Pod NodePort Service.
pub struct CallbackRelay {
    controller: RelayController,
    listen_port: u16,
    shutdown: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
}

impl CallbackRelay {
    /// Bind the stable relay listener before exposing it through Kubernetes.
    pub async fn bind(port: u16) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
        let listen_port = listener.local_addr()?.port();
        let state = Arc::new(Mutex::new(RelayState::default()));
        let controller = RelayController {
            state: state.clone(),
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        let accept_task = tokio::spawn(run_accept_loop(listener, state, shutdown_rx));

        Ok(Self {
            controller,
            listen_port,
            shutdown,
            accept_task,
        })
    }

    pub fn controller(&self) -> RelayController {
        self.controller.clone()
    }

    pub fn listen_port(&self) -> u16 {
        self.listen_port
    }

    /// Stop accepting callbacks, cancel the active generation, and wait for all
    /// accepted connection tasks to finish.
    pub async fn shutdown(self) {
        self.controller.cancel_active();
        let _ = self.shutdown.send(true);
        if let Err(error) = self.accept_task.await {
            warn!(%error, "MARS callback relay accept task failed during shutdown");
        }
    }
}

async fn run_accept_loop(
    listener: TcpListener,
    state: Arc<Mutex<RelayState>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    warn!(%error, "MARS callback relay connection task failed");
                }
            }
            result = listener.accept() => {
                match result {
                    Ok((inbound, peer)) => {
                        if connections.len() >= MAX_ACTIVE_CONNECTIONS {
                            warn!(%peer, limit = MAX_ACTIVE_CONNECTIONS, "discarding MARS callback: relay connection limit reached");
                            continue;
                        }
                        let snapshot = {
                            let state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            state.active.as_ref().map(|active| {
                                (active.id, active.target, active.cancel.subscribe())
                            })
                        };

                        if let Some((generation, target, cancel)) = snapshot {
                            connections.spawn(async move {
                                if let Err(error) = forward_connection(inbound, target, cancel).await {
                                    debug!(%error, %peer, generation, target = %target, "MARS callback relay connection ended");
                                }
                            });
                        } else {
                            debug!(%peer, "discarding MARS callback without an active generation");
                        }
                    }
                    Err(error) => {
                        warn!(%error, "MARS callback relay accept failed; retrying");
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    break;
                                }
                            }
                            _ = tokio::time::sleep(ACCEPT_RETRY_INTERVAL) => {}
                        }
                    }
                }
            }
        }
    }

    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            warn!(%error, "MARS callback relay connection task failed during shutdown");
        }
    }
}

async fn forward_connection(
    mut inbound: TcpStream,
    target: SocketAddr,
    mut cancel: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut outbound = loop {
        if *cancel.borrow() {
            return Ok(());
        }

        tokio::select! {
            biased;
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return Ok(());
                }
            }
            result = TcpStream::connect(target) => {
                match result {
                    Ok(stream) => break stream,
                    Err(error) if error.kind() == io::ErrorKind::ConnectionRefused
                        || error.kind() == io::ErrorKind::AddrNotAvailable =>
                    {
                        tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    };

    tokio::select! {
        biased;
        changed = cancel.changed() => {
            if changed.is_err() || *cancel.borrow() {
                Ok(())
            } else {
                unreachable!("generation cancellation only transitions to true")
            }
        }
        result = copy_bidirectional(&mut inbound, &mut outbound) => result.map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn connect_relay(relay: &CallbackRelay) -> TcpStream {
        TcpStream::connect((Ipv4Addr::LOCALHOST, relay.listen_port()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn callback_waits_until_local_listener_binds() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let generation = relay.controller().start_generation().unwrap();
        let mut callback = connect_relay(&relay).await;
        callback.write_all(b"early callback").await.unwrap();

        tokio::time::sleep(Duration::from_millis(40)).await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, generation.target_port()))
            .await
            .unwrap();
        let (mut local, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut received = vec![0; 14];
        local.read_exact(&mut received).await.unwrap();
        assert_eq!(received, b"early callback");

        drop(generation);
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn forwards_bytes_bidirectionally() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let generation = relay.controller().start_generation().unwrap();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, generation.target_port()))
            .await
            .unwrap();
        let local_task = tokio::spawn(async move {
            let (mut local, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 256 * 1024];
            local.read_exact(&mut request).await.unwrap();
            assert!(request.iter().all(|byte| *byte == 0x5a));
            local.write_all(b"local response").await.unwrap();
        });

        let mut callback = connect_relay(&relay).await;
        callback.write_all(&vec![0x5a; 256 * 1024]).await.unwrap();
        let mut response = vec![0; 14];
        callback.read_exact(&mut response).await.unwrap();
        assert_eq!(response, b"local response");
        local_task.await.unwrap();

        drop(generation);
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn sequential_generations_use_fresh_ports() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let first = relay.controller().start_generation().unwrap();
        let first_port = first.target_port();
        drop(first);

        let second = relay.controller().start_generation().unwrap();
        assert_ne!(second.target_port(), first_port);

        drop(second);
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn stale_generation_is_closed_and_cannot_reach_next_target() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let first = relay.controller().start_generation().unwrap();
        let first_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, first.target_port()))
            .await
            .unwrap();
        let mut stale = connect_relay(&relay).await;
        stale.write_all(b"stale").await.unwrap();
        let (mut first_local, _) =
            tokio::time::timeout(Duration::from_secs(1), first_listener.accept())
                .await
                .unwrap()
                .unwrap();
        let mut stale_payload = [0; 5];
        first_local.read_exact(&mut stale_payload).await.unwrap();
        assert_eq!(&stale_payload, b"stale");

        drop(first);
        let mut byte = [0; 1];
        let stale_read = tokio::time::timeout(Duration::from_secs(1), stale.read(&mut byte))
            .await
            .expect("stale callback was not closed")
            .unwrap();
        assert_eq!(stale_read, 0);
        drop(first_local);
        drop(first_listener);

        let second = relay.controller().start_generation().unwrap();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, second.target_port()))
            .await
            .unwrap();
        let mut current = connect_relay(&relay).await;
        current.write_all(b"current").await.unwrap();

        let (mut local, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut received = vec![0; 7];
        local.read_exact(&mut received).await.unwrap();
        assert_eq!(received, b"current");

        drop(second);
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_old_guard_does_not_cancel_newer_generation() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let old = relay.controller().start_generation().unwrap();
        let current = relay.controller().start_generation().unwrap();
        drop(old);

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, current.target_port()))
            .await
            .unwrap();
        let mut callback = connect_relay(&relay).await;
        callback.write_all(b"current").await.unwrap();
        let (mut local, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut received = [0; 7];
        local.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"current");

        drop(current);
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn one_generation_accepts_multiple_callback_connections() {
        let relay = CallbackRelay::bind(0).await.unwrap();
        let generation = relay.controller().start_generation().unwrap();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, generation.target_port()))
            .await
            .unwrap();

        let mut first = connect_relay(&relay).await;
        let mut second = connect_relay(&relay).await;
        first.write_all(b"one").await.unwrap();
        second.write_all(b"two").await.unwrap();

        let mut messages = Vec::new();
        for _ in 0..2 {
            let (mut local, _) = listener.accept().await.unwrap();
            let mut message = vec![0; 3];
            local.read_exact(&mut message).await.unwrap();
            messages.push(message);
        }
        messages.sort();
        assert_eq!(messages, [b"one".to_vec(), b"two".to_vec()]);

        drop(generation);
        relay.shutdown().await;
    }
}
