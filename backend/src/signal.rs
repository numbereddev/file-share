mod relay;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{
        RawQuery, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{Sink, SinkExt};
use hmac::{Hmac, Mac, digest::CtOutput};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha1::Sha1;
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};
use uuid::Uuid;

use crate::turn::{IceServer, Turn};

const MAX_CONNECTIONS: usize = 512;
const MAX_ROOMS: usize = 256;
const MAX_GUESTS: usize = 8;
const MAX_PAYLOAD: usize = 16 * 1024;
// Allow ready, the initial roster, and a burst of joins with one event and roster each.
const OUTBOX_CAPACITY: usize = 2 + 2 * MAX_GUESTS;
const RATE_INTERVAL: Duration = Duration::from_millis(250);
const MAX_RATE: u64 = (1_u64 << 53) - 1; // Largest exact integer in a browser's JSON number range.
const RATE_CONTROL_RESERVE: usize = 2;
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING: &[u8] = b"signal";

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Hello {
    Create { password: Option<String> },
    Join { room: Uuid },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Incoming {
    Signal {
        to: Uuid,
        signal: Map<String, Value>,
    },
    Relay {
        to: Uuid,
    },
    Rates {
        #[serde(default)]
        rates: Value,
    },
}

#[derive(Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    Host,
    Guest,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Outgoing<'a> {
    Ready {
        room: Uuid,
        peer: Uuid,
        role: Role,
        #[serde(rename = "relayToken")]
        relay_token: Uuid,
        #[serde(rename = "iceServers", skip_serializing_if = "Option::is_none")]
        ice_servers: Option<[IceServer<'a>; 1]>,
    },
    PeerJoined {
        peer: Uuid,
    },
    PeerLeft {
        peer: Uuid,
    },
    PasswordRequired {
        incorrect: bool,
    },
    Relay {
        guest: Uuid,
    },
    RoomClosed,
    Roster {
        host: Uuid,
        peers: Vec<Uuid>,
    },
    Rates {
        rates: &'a Map<String, Value>,
    },
    Error {
        message: &'static str,
    },
    Signal {
        from: Uuid,
        signal: Map<String, Value>,
    },
}

impl Outgoing<'_> {
    fn text(self) -> String {
        serde_json::to_string(&self).expect("server messages are serializable")
    }
}

#[derive(Clone, Copy)]
enum End {
    Disconnected,
    PeerClosed,
    Policy,
    Idle,
    Overloaded,
    RoomClosed,
}

type SecretTag = CtOutput<Hmac<Sha1>>;

fn tag(key: Uuid, domain: &[u8], value: &[u8]) -> SecretTag {
    let mut mac =
        Hmac::<Sha1>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(domain);
    mac.update(value);
    mac.finalize()
}

struct Peer {
    outbox: mpsc::Sender<String>,
    stop: oneshot::Sender<End>,
    relay_tag: SecretTag,
}

impl Peer {
    fn enqueue(&self, message: Outgoing<'_>) -> bool {
        self.enqueue_text(message.text())
    }

    fn enqueue_text(&self, text: String) -> bool {
        self.outbox.try_send(text).is_ok()
    }

    fn has_space(&self, slots: usize) -> bool {
        !self.outbox.is_closed() && self.outbox.capacity() >= slots
    }

    fn stop(self, reason: End) {
        let _ = self.stop.send(reason);
    }
}

struct Guest {
    peer: Peer,
    relay: Option<relay::Slot>,
}

struct Room {
    host_id: Uuid,
    host: Peer,
    guests: HashMap<Uuid, Guest>,
    last_rates: Option<Instant>,
}

impl Room {
    fn broadcast_roster(&mut self) -> bool {
        // All producers hold the room lock. Remove stalled guests before sending a
        // single snapshot; available queue capacity can only increase as readers drain.
        let stalled: Vec<_> = self
            .guests
            .iter()
            .filter_map(|(&id, guest)| (!guest.peer.has_space(1)).then_some(id))
            .collect();
        for peer in stalled {
            self.guests
                .remove(&peer)
                .expect("guest was just checked")
                .peer
                .stop(End::Overloaded);
            if !self.host.enqueue(Outgoing::PeerLeft { peer }) {
                return false;
            }
        }

        let mut peers: Vec<_> = self.guests.keys().copied().collect();
        peers.sort_unstable();
        let text = Outgoing::Roster {
            host: self.host_id,
            peers,
        }
        .text();
        if !self.host.enqueue_text(text.clone()) {
            return false;
        }
        for guest in self.guests.values() {
            // If a receiver closes after preflight, its session will remove it.
            guest.peer.enqueue_text(text.clone());
        }
        true
    }
}

type Rooms = HashMap<Uuid, Room>;

struct Inner {
    rooms: Mutex<Rooms>,
    connections: Arc<Semaphore>,
    relay_connections: Arc<Semaphore>,
    secret_key: Uuid,
    password: Option<SecretTag>,
    turn: Option<Turn>,
}

#[derive(Clone)]
pub(crate) struct AppState(Arc<Inner>);

struct Session {
    app: AppState,
    room: Uuid,
    peer: Uuid,
    role: Role,
}

struct Connected {
    session: Session,
    outbox: mpsc::Receiver<String>,
    stop: oneshot::Receiver<End>,
}

impl AppState {
    pub(crate) fn new(turn: Option<Turn>, password: Option<&str>) -> Self {
        let secret_key = Uuid::new_v4();
        let password = password
            .filter(|password| !password.is_empty())
            .map(|password| tag(secret_key, b"password:", password.as_bytes()));
        Self(Arc::new(Inner {
            rooms: Mutex::new(HashMap::new()),
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            relay_connections: Arc::new(Semaphore::new(relay::MAX_CONNECTIONS)),
            secret_key,
            password,
            turn,
        }))
    }

    fn register(&self, hello: Hello) -> Result<Connected, &'static str> {
        let mut rooms = self.0.rooms.lock().expect("rooms lock poisoned");
        let (room_id, id, role) = match hello {
            Hello::Create { .. } => {
                if rooms.len() >= MAX_ROOMS {
                    return Err("room limit reached");
                }
                (
                    unused_uuid(|id| rooms.contains_key(id)),
                    Uuid::new_v4(),
                    Role::Host,
                )
            }
            Hello::Join { room } => {
                let entry = rooms.get(&room).ok_or("room not found")?;
                if entry.guests.len() >= MAX_GUESTS {
                    return Err("room is full");
                }
                let peer = unused_uuid(|id| *id == entry.host_id || entry.guests.contains_key(id));
                (room, peer, Role::Guest)
            }
        };

        let ice_servers = self
            .0
            .turn
            .as_ref()
            .map(|turn| turn.ice_server(id).map(|server| [server]))
            .transpose()
            .map_err(|_| "server clock unavailable")?;
        let (tx, outbox) = mpsc::channel(OUTBOX_CAPACITY);
        let (stop_tx, stop) = oneshot::channel();
        let relay_token = Uuid::new_v4();
        let peer = Peer {
            outbox: tx,
            stop: stop_tx,
            relay_tag: tag(self.0.secret_key, b"relay:", relay_token.as_bytes()),
        };
        // Queue ready before publishing this peer or notifying its host.
        if !peer.enqueue(Outgoing::Ready {
            room: room_id,
            peer: id,
            role,
            relay_token,
            ice_servers,
        }) {
            return Err("could not initialize signaling");
        }
        match role {
            Role::Host => {
                rooms.insert(
                    room_id,
                    Room {
                        host_id: id,
                        host: peer,
                        guests: HashMap::new(),
                        last_rates: None,
                    },
                );
            }
            Role::Guest => {
                let room = rooms.get_mut(&room_id).expect("room was checked above");
                if !room.host.has_space(2) || !room.host.enqueue(Outgoing::PeerJoined { peer: id })
                {
                    close_room(&mut rooms, room_id, Some(End::Overloaded));
                    return Err("room host unavailable");
                }
                room.guests.insert(id, Guest { peer, relay: None });
            }
        }
        if !rooms
            .get_mut(&room_id)
            .expect("room was just registered")
            .broadcast_roster()
        {
            close_room(&mut rooms, room_id, Some(End::Overloaded));
            return Err("room host unavailable");
        }
        Ok(Connected {
            session: Session {
                app: self.clone(),
                room: room_id,
                peer: id,
                role,
            },
            outbox,
            stop,
        })
    }

    fn relay(&self, session: &Session, to: Uuid, signal: Map<String, Value>) {
        let mut rooms = self.0.rooms.lock().expect("rooms lock poisoned");
        let Some(room) = rooms.get(&session.room) else {
            return;
        };
        let target = match session.role {
            Role::Host if room.host_id == session.peer => {
                room.guests.get(&to).map(|guest| &guest.peer)
            }
            Role::Guest if room.host_id == to && room.guests.contains_key(&session.peer) => {
                Some(&room.host)
            }
            _ => None,
        };
        let Some(target) = target else { return };
        if target.enqueue(Outgoing::Signal {
            from: session.peer,
            signal,
        }) {
            return;
        }
        match session.role {
            Role::Host => remove_guest(&mut rooms, session.room, to, Some(End::Overloaded)),
            Role::Guest => close_room(&mut rooms, session.room, Some(End::Overloaded)),
        }
    }

    fn authorize_relay(&self, session: &Session, to: Uuid) {
        if !matches!(session.role, Role::Host) {
            return;
        }
        let mut rooms = self.0.rooms.lock().expect("rooms lock poisoned");
        let Some(room) = rooms
            .get_mut(&session.room)
            .filter(|room| room.host_id == session.peer)
        else {
            return;
        };
        let Some(guest) = room.guests.get_mut(&to) else {
            return;
        };
        if guest.relay.as_ref().is_some_and(relay::Slot::is_active) {
            return;
        }
        if !room.host.has_space(1) {
            close_room(&mut rooms, session.room, Some(End::Overloaded));
        } else if !guest.peer.enqueue(Outgoing::Relay { guest: to }) {
            remove_guest(&mut rooms, session.room, to, Some(End::Overloaded));
        } else if !room.host.enqueue(Outgoing::Relay { guest: to }) {
            close_room(&mut rooms, session.room, Some(End::Overloaded));
        } else {
            guest.relay = Some(relay::Slot::new());
        }
    }

    fn attach_relay(
        &self,
        hello: relay::Hello,
        socket: relay::Socket,
    ) -> Result<Option<relay::Pairing>, relay::Socket> {
        let presented = tag(self.0.secret_key, b"relay:", hello.token.as_bytes());
        let mut rooms = self.0.rooms.lock().expect("rooms lock poisoned");
        let Some(room) = rooms.get_mut(&hello.room) else {
            return Err(socket);
        };
        let Some(guest) = room.guests.get_mut(&hello.guest) else {
            return Err(socket);
        };
        let role = match (
            presented == room.host.relay_tag,
            presented == guest.peer.relay_tag,
        ) {
            (true, false) => Role::Host,
            (false, true) => Role::Guest,
            _ => return Err(socket),
        };
        let Some(slot) = &mut guest.relay else {
            return Err(socket);
        };
        slot.attach(role, socket)
    }

    fn broadcast_rates(&self, session: &Session, rates: Value, now: Instant) {
        if !matches!(session.role, Role::Host) {
            return;
        }
        let Some(rates) = rates.as_object().filter(|rates| rates.len() <= MAX_GUESTS) else {
            return;
        };
        let mut rooms = self.0.rooms.lock().expect("rooms lock poisoned");
        let Some(room) = rooms.get_mut(&session.room) else {
            return;
        };
        if room.host_id != session.peer
            || room.guests.is_empty()
            || room
                .last_rates
                .is_some_and(|last| now.duration_since(last) < RATE_INTERVAL)
            || !rates.iter().all(|(key, value)| {
                Uuid::try_parse(key).is_ok_and(|id| {
                    room.guests.contains_key(&id)
                        && id.as_hyphenated().encode_lower(&mut Uuid::encode_buffer())
                            == key.as_str()
                }) && value.as_u64().is_some_and(|rate| rate <= MAX_RATE)
            })
        {
            return;
        }
        room.last_rates = Some(now);
        let text = Outgoing::Rates { rates }.text();
        for guest in room.guests.values() {
            if guest.peer.has_space(RATE_CONTROL_RESERVE + 1) {
                guest.peer.enqueue_text(text.clone());
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut rooms = self.app.0.rooms.lock().expect("rooms lock poisoned");
        match self.role {
            Role::Host
                if rooms
                    .get(&self.room)
                    .is_some_and(|room| room.host_id == self.peer) =>
            {
                close_room(&mut rooms, self.room, None);
            }
            Role::Guest => remove_guest(&mut rooms, self.room, self.peer, None),
            _ => {}
        }
    }
}

fn unused_uuid(mut in_use: impl FnMut(&Uuid) -> bool) -> Uuid {
    loop {
        let id = Uuid::new_v4();
        if !in_use(&id) {
            return id;
        }
    }
}

fn close_room(rooms: &mut Rooms, room_id: Uuid, host_reason: Option<End>) {
    if let Some(room) = rooms.remove(&room_id) {
        if let Some(reason) = host_reason {
            room.host.stop(reason);
        }
        for guest in room.guests.into_values() {
            guest.peer.stop(End::RoomClosed);
        }
    }
}

fn remove_guest(rooms: &mut Rooms, room_id: Uuid, peer: Uuid, reason: Option<End>) {
    let Some(room) = rooms.get_mut(&room_id) else {
        return;
    };
    let Some(guest) = room.guests.remove(&peer) else {
        return;
    };
    if let Some(reason) = reason {
        guest.peer.stop(reason);
    }
    if !room.host.enqueue(Outgoing::PeerLeft { peer }) || !room.broadcast_roster() {
        close_room(rooms, room_id, Some(End::Overloaded));
    }
}

pub(crate) async fn upgrade(
    State(app): State<AppState>,
    RawQuery(query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    if query.is_some_and(|query| query.split('&').any(|entry| entry == "relay=1")) {
        return relay::upgrade(ws, app);
    }
    let Ok(permit) = app.0.connections.clone().try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "signaling connection limit reached",
        )
            .into_response();
    };
    ws.read_buffer_size(4 * 1024)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * MAX_PAYLOAD)
        .max_message_size(MAX_PAYLOAD)
        .max_frame_size(MAX_PAYLOAD)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            handle(socket, app).await;
        })
}

async fn handle(mut socket: WebSocket, app: AppState) {
    let hello = match timeout(HELLO_TIMEOUT, read_hello(&mut socket)).await {
        Ok(Ok(Some(hello))) => hello,
        Ok(Ok(None)) => return,
        Ok(Err(message)) => return reject(&mut socket, message).await,
        Err(_) => return reject(&mut socket, "timed out waiting for create or join").await,
    };
    if let (Some(expected), Hello::Create { password }) = (&app.0.password, &hello) {
        if !password.as_ref().is_some_and(|password| {
            tag(app.0.secret_key, b"password:", password.as_bytes()) == *expected
        }) {
            let text = Outgoing::PasswordRequired {
                incorrect: password.is_some(),
            }
            .text();
            if send(&mut socket, Message::text(text)).await {
                close(&mut socket, close_code::POLICY, "password required").await;
            }
            return;
        }
    }
    let Connected {
        session,
        mut outbox,
        mut stop,
    } = match app.register(hello) {
        Ok(connected) => connected,
        Err(message) => return reject(&mut socket, message).await,
    };

    // Even if the room closes immediately, send ready and the initial roster first.
    for _ in 0..2 {
        let Some(text) = outbox.recv().await else {
            return;
        };
        if !send(&mut socket, Message::text(text)).await {
            return;
        }
    }
    let end = run(&mut socket, &session, &mut outbox, &mut stop).await;
    drop(session);
    drop(outbox);
    match end {
        End::Disconnected => {}
        End::PeerClosed => {
            // Tungstenite queues the close reply when it reads the peer's close.
            let _ = timeout(WRITE_TIMEOUT, socket.flush()).await;
        }
        End::Policy => {
            close(
                &mut socket,
                close_code::POLICY,
                "expected a text signal, rates, or relay message",
            )
            .await
        }
        End::Idle => close(&mut socket, close_code::AWAY, "heartbeat timed out").await,
        End::Overloaded => {
            close(&mut socket, close_code::AGAIN, "signaling queue overloaded").await
        }
        End::RoomClosed => {
            if send(&mut socket, Message::text(Outgoing::RoomClosed.text())).await {
                close(&mut socket, close_code::NORMAL, "room closed").await;
            }
        }
    }
}

async fn read_hello(socket: &mut WebSocket) -> Result<Option<Hello>, &'static str> {
    loop {
        match socket.recv().await {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text)
                    .map(Some)
                    .map_err(|_| "expected create or join as the first message");
            }
            Some(Ok(Message::Binary(_))) => return Err("expected a text create or join message"),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) => {
                let _ = socket.flush().await;
                return Ok(None);
            }
            Some(Err(_)) | None => return Ok(None),
        }
    }
}

async fn run(
    socket: &mut WebSocket,
    session: &Session,
    outbox: &mut mpsc::Receiver<String>,
    stop: &mut oneshot::Receiver<End>,
) -> End {
    let mut heartbeat = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut awaiting_pong = false;
    loop {
        if let Ok(reason) = stop.try_recv() {
            return reason;
        }
        tokio::select! {
            reason = &mut *stop => return reason.unwrap_or(End::Disconnected),
            outgoing = outbox.recv() => {
                let Some(text) = outgoing else { return stop.await.unwrap_or(End::Disconnected) };
                if !send(socket, Message::text(text)).await {
                    return End::Disconnected;
                }
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => match serde_json::from_str(&text) {
                    Ok(Incoming::Signal { to, signal }) => session.app.relay(session, to, signal),
                    Ok(Incoming::Relay { to }) => session.app.authorize_relay(session, to),
                    Ok(Incoming::Rates { rates }) => session.app.broadcast_rates(session, rates, Instant::now()),
                    Err(_) => return End::Policy,
                },
                Some(Ok(Message::Binary(_))) => return End::Policy,
                Some(Ok(Message::Pong(bytes))) if bytes.as_ref() == PING => awaiting_pong = false,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) => return End::PeerClosed,
                Some(Err(_)) | None => return End::Disconnected,
            },
            _ = heartbeat.tick() => {
                if awaiting_pong {
                    return End::Idle;
                }
                if !send(socket, Message::Ping(PING.into())).await {
                    return End::Disconnected;
                }
                awaiting_pong = true;
            }
        }
    }
}

async fn send(socket: &mut (impl Sink<Message> + Unpin), message: Message) -> bool {
    matches!(
        timeout(WRITE_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
}

async fn close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    send(
        socket,
        Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })),
    )
    .await;
}

async fn reject(socket: &mut WebSocket, message: &'static str) {
    if send(socket, Message::text(Outgoing::Error { message }.text())).await {
        close(
            socket,
            close_code::POLICY,
            "signaling registration rejected",
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn queued(peer: &mut Connected) -> Value {
        serde_json::from_str(&peer.outbox.try_recv().unwrap()).unwrap()
    }

    #[test]
    fn overflow_evicts_guest_or_closes_room_without_waiting_for_a_writer() {
        let app = AppState::new(None, None);
        let mut host = app.register(Hello::Create { password: None }).unwrap();
        let room = host.session.room;
        let mut guest = app.register(Hello::Join { room }).unwrap();
        let mut healthy_guest = app.register(Hello::Join { room }).unwrap();
        assert_eq!(queued(&mut host)["type"], "ready");
        assert_eq!(queued(&mut host)["type"], "roster");
        assert_eq!(
            queued(&mut host),
            json!({"type": "peer-joined", "peer": guest.session.peer})
        );
        assert_eq!(queued(&mut host)["type"], "roster");
        assert_eq!(
            queued(&mut host),
            json!({"type": "peer-joined", "peer": healthy_guest.session.peer})
        );
        assert_eq!(queued(&mut host)["type"], "roster");
        assert_eq!(queued(&mut guest)["type"], "ready");

        for _ in 0..=OUTBOX_CAPACITY {
            app.relay(&host.session, guest.session.peer, Map::new());
        }
        assert_eq!(guest.outbox.len(), OUTBOX_CAPACITY);
        assert!(matches!(guest.stop.try_recv(), Ok(End::Overloaded)));
        assert_eq!(
            queued(&mut host),
            json!({"type": "peer-left", "peer": guest.session.peer})
        );
        assert_eq!(
            queued(&mut host),
            json!({"type": "roster", "host": host.session.peer, "peers": [healthy_guest.session.peer]})
        );
        drop(guest);
        assert!(host.outbox.is_empty());

        for _ in 0..=OUTBOX_CAPACITY {
            app.relay(&healthy_guest.session, host.session.peer, Map::new());
        }
        assert!(matches!(host.stop.try_recv(), Ok(End::Overloaded)));
        assert!(matches!(healthy_guest.stop.try_recv(), Ok(End::RoomClosed)));
        // Ready and the initial roster are queued even if the room closes immediately.
        assert_eq!(queued(&mut healthy_guest)["type"], "ready");
        assert_eq!(queued(&mut healthy_guest)["type"], "roster");
        assert!(matches!(
            app.register(Hello::Join { room }),
            Err("room not found")
        ));
    }

    #[test]
    fn limits_rooms_and_guests_and_reclaims_capacity() {
        let app = AppState::new(None, None);
        let mut hosts = (0..MAX_ROOMS)
            .map(|_| app.register(Hello::Create { password: None }).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            app.register(Hello::Create { password: None }),
            Err("room limit reached")
        ));
        drop(hosts.pop().unwrap());
        let mut host = app.register(Hello::Create { password: None }).unwrap();
        let room = host.session.room;
        let mut guests = (0..MAX_GUESTS)
            .map(|_| app.register(Hello::Join { room }).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            app.register(Hello::Join { room }),
            Err("room is full")
        ));
        assert_eq!(queued(&mut host)["type"], "ready");
        assert_eq!(queued(&mut host)["type"], "roster");
        for guest in &guests {
            assert_eq!(
                queued(&mut host),
                json!({"type": "peer-joined", "peer": guest.session.peer})
            );
            assert_eq!(queued(&mut host)["type"], "roster");
        }
        drop(guests.pop().unwrap());
        assert_eq!(queued(&mut host)["type"], "peer-left");
        assert!(app.register(Hello::Join { room }).is_ok());
    }

    #[test]
    fn roster_excludes_stalled_guests_before_sending_to_healthy_peers() {
        let app = AppState::new(None, None);
        let mut host = app.register(Hello::Create { password: None }).unwrap();
        let room = host.session.room;
        let mut stalled = app.register(Hello::Join { room }).unwrap();
        let mut healthy = app.register(Hello::Join { room }).unwrap();
        while host.outbox.try_recv().is_ok() {}
        for (guest, slots) in [(&stalled, 0), (&healthy, 1)] {
            for _ in guest.outbox.len()..(OUTBOX_CAPACITY - slots) {
                app.relay(&host.session, guest.session.peer, Map::new());
            }
        }

        let mut newcomer = app.register(Hello::Join { room }).unwrap();
        assert!(matches!(stalled.stop.try_recv(), Ok(End::Overloaded)));
        let mut peers = [healthy.session.peer, newcomer.session.peer];
        peers.sort_unstable();
        let expected = json!({"type": "roster", "host": host.session.peer, "peers": peers});
        assert_eq!(
            queued(&mut host),
            json!({"type": "peer-joined", "peer": newcomer.session.peer})
        );
        assert_eq!(
            queued(&mut host),
            json!({"type": "peer-left", "peer": stalled.session.peer})
        );
        assert_eq!(queued(&mut host), expected);
        assert!(host.outbox.is_empty());
        for _ in 1..OUTBOX_CAPACITY {
            assert_ne!(queued(&mut healthy), expected);
        }
        assert_eq!(queued(&mut healthy), expected);
        assert_eq!(queued(&mut newcomer)["type"], "ready");
        assert_eq!(queued(&mut newcomer), expected);
    }

    #[test]
    fn rates_are_throttled_and_leave_queue_space_for_control_messages() {
        let app = AppState::new(None, None);
        let host = app.register(Hello::Create { password: None }).unwrap();
        let room = host.session.room;
        let mut slow = app.register(Hello::Join { room }).unwrap();
        let mut fast = app.register(Hello::Join { room }).unwrap();
        while fast.outbox.try_recv().is_ok() {}
        let congested = OUTBOX_CAPACITY - RATE_CONTROL_RESERVE;
        for _ in slow.outbox.len()..congested {
            app.relay(&host.session, slow.session.peer, Map::new());
        }

        let t = Instant::now();
        let first = json!({(slow.session.peer): 12});
        let next = json!({(slow.session.peer): 20});
        app.broadcast_rates(&host.session, first.clone(), t);
        app.broadcast_rates(&host.session, next.clone(), t + RATE_INTERVAL / 2);
        app.broadcast_rates(&host.session, next.clone(), t + RATE_INTERVAL);
        assert_eq!(queued(&mut fast), json!({"type": "rates", "rates": first}));
        assert_eq!(queued(&mut fast), json!({"type": "rates", "rates": next}));
        assert!(fast.outbox.is_empty());
        assert_eq!(slow.outbox.len(), congested);
        assert!(matches!(
            slow.stop.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
}
