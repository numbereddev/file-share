use std::{
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    time::Duration,
};

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade, close_code},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::Deserialize;
use tokio::{
    sync::{OwnedSemaphorePermit, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout},
};
use uuid::Uuid;

use super::{AppState, HELLO_TIMEOUT, PING_INTERVAL, Role, WRITE_TIMEOUT, close, send};

pub(super) const MAX_CONNECTIONS: usize = 512;
const MAX_PAYLOAD: usize = 64 * 1024;
const PAIR_TIMEOUT: Duration = Duration::from_secs(15);
const PING: &[u8] = b"relay";
const READY: &str = r#"{"type":"relay-ready"}"#;

#[derive(Deserialize)]
pub(super) struct Hello {
    pub(super) room: Uuid,
    pub(super) guest: Uuid,
    pub(super) token: Uuid,
}

pub(super) struct Socket {
    ws: WebSocket,
    _permit: OwnedSemaphorePermit,
}

pub(super) struct Slot {
    phase: Phase,
    deadline: Instant,
    stop: watch::Sender<()>,
}

enum Phase {
    Open,
    Waiting {
        role: Role,
        handoff: oneshot::Sender<Socket>,
    },
    Paired,
}

pub(super) struct Pairing {
    first: Socket,
    role: Role,
    partner: oneshot::Receiver<Socket>,
    deadline: Instant,
    stop: watch::Receiver<()>,
}

impl Slot {
    pub(super) fn new() -> Self {
        let (stop, _) = watch::channel(());
        Self {
            phase: Phase::Open,
            deadline: Instant::now() + PAIR_TIMEOUT,
            stop,
        }
    }

    pub(super) fn is_active(&self) -> bool {
        match &self.phase {
            Phase::Open => Instant::now() < self.deadline,
            Phase::Waiting { handoff, .. } => {
                Instant::now() < self.deadline && !handoff.is_closed()
            }
            Phase::Paired => self.stop.receiver_count() != 0,
        }
    }

    pub(super) fn attach(&mut self, role: Role, socket: Socket) -> Result<Option<Pairing>, Socket> {
        if !self.is_active() {
            return Err(socket);
        }
        match &self.phase {
            Phase::Open => {
                let (handoff, partner) = oneshot::channel();
                self.phase = Phase::Waiting { role, handoff };
                Ok(Some(Pairing {
                    first: socket,
                    role,
                    partner,
                    deadline: self.deadline,
                    stop: self.stop.subscribe(),
                }))
            }
            Phase::Waiting { role: first, .. } if *first != role => {
                let Phase::Waiting { handoff, .. } =
                    std::mem::replace(&mut self.phase, Phase::Paired)
                else {
                    unreachable!("phase was checked above")
                };
                handoff.send(socket).map(|()| None)
            }
            _ => Err(socket),
        }
    }
}

pub(super) fn upgrade(ws: WebSocketUpgrade, app: AppState) -> Response {
    let Ok(permit) = app.0.relay_connections.clone().try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "relay connection limit reached",
        )
            .into_response();
    };
    ws.read_buffer_size(4 * 1024)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * MAX_PAYLOAD)
        .max_message_size(MAX_PAYLOAD)
        .max_frame_size(MAX_PAYLOAD)
        .on_upgrade(move |ws| {
            handle(
                Socket {
                    ws,
                    _permit: permit,
                },
                app,
            )
        })
}

async fn handle(mut socket: Socket, app: AppState) {
    let hello = match timeout(HELLO_TIMEOUT, read_hello(&mut socket.ws)).await {
        Ok(Ok(Some(hello))) => hello,
        Ok(Ok(None)) => return,
        Ok(Err(())) | Err(_) => {
            close(&mut socket.ws, close_code::POLICY, "expected a relay hello").await;
            return;
        }
    };
    match app.attach_relay(hello, socket) {
        Ok(Some(pairing)) => pairing.run().await,
        Ok(None) => {}
        Err(mut socket) => close(&mut socket.ws, close_code::POLICY, "relay attach rejected").await,
    }
}

async fn read_hello(socket: &mut WebSocket) -> Result<Option<Hello>, ()> {
    loop {
        match socket.recv().await {
            Some(Ok(Message::Text(text))) if text.len() <= 1024 => {
                return serde_json::from_str(&text).map(Some).map_err(|_| ());
            }
            Some(Ok(Message::Text(_) | Message::Binary(_))) => return Err(()),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) => {
                let _ = socket.flush().await;
                return Ok(None);
            }
            Some(Err(_)) | None => return Ok(None),
        }
    }
}

impl Pairing {
    async fn run(mut self) {
        let deadline = sleep_until(self.deadline);
        tokio::pin!(deadline);
        let second = loop {
            tokio::select! {
                biased;
                _ = self.stop.changed() => {
                    close(&mut self.first.ws, close_code::NORMAL, "relay membership ended").await;
                    return;
                }
                partner = &mut self.partner => match partner {
                    Ok(socket) => break socket,
                    Err(_) => return,
                },
                _ = &mut deadline => {
                    close(&mut self.first.ws, close_code::AWAY, "relay pairing timed out").await;
                    return;
                }
                message = self.first.ws.recv() => match message {
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {},
                    Some(Ok(Message::Close(_))) => {
                        let _ = timeout(WRITE_TIMEOUT, self.first.ws.flush()).await;
                        return;
                    }
                    Some(Ok(_)) => {
                        close(&mut self.first.ws, close_code::POLICY, "relay is not ready").await;
                        return;
                    }
                    Some(Err(_)) | None => return,
                }
            }
        };
        let (mut host, mut guest) = match self.role {
            Role::Host => (self.first, second),
            Role::Guest => (second, self.first),
        };
        let end = tokio::select! {
            biased;
            _ = self.stop.changed() => return,
            end = transfer(&mut host.ws, &mut guest.ws) => end,
        };
        match end {
            End::PeerClosed(role) => {
                let (closed, other) = match role {
                    Role::Host => (&mut host.ws, &mut guest.ws),
                    Role::Guest => (&mut guest.ws, &mut host.ws),
                };
                let _ = tokio::join!(
                    timeout(WRITE_TIMEOUT, closed.flush()),
                    close(other, close_code::NORMAL, "relay peer closed"),
                );
            }
            End::Policy | End::Idle => {
                let (code, reason) = match end {
                    End::Policy => (
                        close_code::POLICY,
                        "only the host may send relay binary data",
                    ),
                    _ => (close_code::AWAY, "relay heartbeat timed out"),
                };
                tokio::join!(
                    close(&mut host.ws, code, reason),
                    close(&mut guest.ws, code, reason)
                );
            }
            End::Disconnected => {}
        }
    }
}

enum End {
    PeerClosed(Role),
    Policy,
    Idle,
    Disconnected,
}

async fn transfer(host: &mut WebSocket, guest: &mut WebSocket) -> End {
    let (host_ready, guest_ready) = tokio::join!(
        send(host, Message::text(READY)),
        send(guest, Message::text(READY)),
    );
    if !host_ready || !guest_ready {
        return End::Disconnected;
    }
    let (host_tx, host_rx) = host.split();
    let (guest_tx, guest_rx) = guest.split();
    let host_pong = AtomicBool::new(true);
    let guest_pong = AtomicBool::new(true);
    // Each direction reads only after the previous write completes. The opposite
    // reader handles pongs for this direction's writer without a transfer queue.
    tokio::select! {
        end = forward(host_rx, guest_tx, Role::Host, &host_pong, &guest_pong) => end,
        end = forward(guest_rx, host_tx, Role::Guest, &guest_pong, &host_pong) => end,
    }
}

async fn forward(
    mut source: SplitStream<&mut WebSocket>,
    mut dest: SplitSink<&mut WebSocket, Message>,
    role: Role,
    source_pong: &AtomicBool,
    dest_pong: &AtomicBool,
) -> End {
    let mut heartbeat = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let message = tokio::select! {
            _ = heartbeat.tick() => {
                if !dest_pong.swap(false, Relaxed) {
                    return End::Idle;
                }
                Message::Ping(PING.into())
            }
            message = source.next() => match message {
                Some(Ok(message @ Message::Text(_))) => message,
                Some(Ok(message @ Message::Binary(_))) if role == Role::Host => message,
                Some(Ok(Message::Binary(_))) => return End::Policy,
                Some(Ok(Message::Pong(bytes))) if bytes.as_ref() == PING => {
                    source_pong.store(true, Relaxed);
                    continue;
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) => return End::PeerClosed(role),
                Some(Err(_)) | None => return End::Disconnected,
            },
        };
        if !send(&mut dest, message).await {
            return End::Disconnected;
        }
    }
}
