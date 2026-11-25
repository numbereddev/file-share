use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::frame::coding::CloseCode},
};
use uuid::Uuid;

use crate::{router, signal::AppState};

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    url: String,
    task: JoinHandle<()>,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/api/signal", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router(AppState::new(None, None), None))
                .await
                .unwrap();
        });
        Self { url, task }
    }

    async fn connect(&self) -> Client {
        timeout(Duration::from_secs(2), connect_async(&self.url))
            .await
            .unwrap()
            .unwrap()
            .0
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn send(client: &mut Client, value: Value) {
    client.send(Message::text(value.to_string())).await.unwrap();
}

async fn frame(client: &mut Client) -> Message {
    timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn receive(client: &mut Client) -> Value {
    serde_json::from_str(frame(client).await.to_text().unwrap()).unwrap()
}

fn uuid_field(value: &Value, key: &str) -> Uuid {
    let id: Uuid = serde_json::from_value(value[key].clone()).unwrap();
    assert_eq!(id.get_version_num(), 4);
    id
}

fn roster(host: Uuid, mut peers: Vec<Uuid>) -> Value {
    peers.sort_unstable();
    json!({"type": "roster", "host": host, "peers": peers})
}

async fn create(server: &Server) -> (Client, Uuid, Uuid) {
    let mut client = server.connect().await;
    send(&mut client, json!({"type": "create"})).await;
    let ready = receive(&mut client).await;
    let room = uuid_field(&ready, "room");
    let host = uuid_field(&ready, "peer");
    let token = uuid_field(&ready, "relayToken");
    assert_eq!(
        ready,
        json!({"type": "ready", "room": room, "peer": host, "role": "host", "relayToken": token})
    );
    assert_eq!(receive(&mut client).await, roster(host, vec![]));
    (client, room, host)
}

async fn join(
    server: &Server,
    host: &mut Client,
    room: Uuid,
    host_id: Uuid,
    existing: &[Uuid],
) -> (Client, Uuid) {
    let mut guest = server.connect().await;
    send(&mut guest, json!({"type": "join", "room": room})).await;
    let ready = receive(&mut guest).await;
    let peer = uuid_field(&ready, "peer");
    let token = uuid_field(&ready, "relayToken");
    assert_eq!(
        ready,
        json!({"type": "ready", "room": room, "peer": peer, "role": "guest", "relayToken": token})
    );
    assert_eq!(
        receive(host).await,
        json!({"type": "peer-joined", "peer": peer})
    );
    let expected = roster(host_id, [existing, &[peer]].concat());
    assert_eq!(receive(host).await, expected);
    assert_eq!(receive(&mut guest).await, expected);
    (guest, peer)
}

#[tokio::test]
async fn relays_only_between_room_host_and_guests_and_cleans_up() {
    let server = Server::start().await;
    let (mut host, room, host_id) = create(&server).await;
    let (mut guest_a, guest_a_id) = join(&server, &mut host, room, host_id, &[]).await;
    let (mut guest_b, guest_b_id) = join(&server, &mut host, room, host_id, &[guest_a_id]).await;
    assert_eq!(
        receive(&mut guest_a).await,
        roster(host_id, vec![guest_a_id, guest_b_id])
    );
    let (mut other_host, other_room, other_host_id) = create(&server).await;
    let (mut other_guest, other_guest_id) =
        join(&server, &mut other_host, other_room, other_host_id, &[]).await;
    assert_ne!(host_id, guest_a_id);
    assert_ne!(guest_a_id, guest_b_id);

    let ice = json!({"candidate": {"candidate": "candidate:example", "sdpMid": "0"}});
    for to in [guest_b_id, other_host_id, host_id] {
        send(
            &mut guest_a,
            json!({"type": "signal", "to": to, "signal": ice}),
        )
        .await;
    }
    assert_eq!(
        receive(&mut host).await,
        json!({"type": "signal", "from": guest_a_id, "signal": ice})
    );

    let offer = json!({"description": {"type": "offer", "sdp": "v=0\r\n"}});
    send(
        &mut host,
        json!({"type": "signal", "to": guest_b_id, "signal": offer}),
    )
    .await;
    assert_eq!(
        receive(&mut guest_b).await,
        json!({"type": "signal", "from": host_id, "signal": offer})
    );
    send(
        &mut other_guest,
        json!({"type": "signal", "to": other_host_id, "signal": ice}),
    )
    .await;
    assert_eq!(
        receive(&mut other_host).await,
        json!({"type": "signal", "from": other_guest_id, "signal": ice})
    );

    guest_a.close(None).await.unwrap();
    assert_eq!(
        receive(&mut host).await,
        json!({"type": "peer-left", "peer": guest_a_id})
    );
    let expected = roster(host_id, vec![guest_b_id]);
    assert_eq!(receive(&mut host).await, expected);
    assert_eq!(receive(&mut guest_b).await, expected);
    host.close(None).await.unwrap();
    assert_eq!(receive(&mut guest_b).await, json!({"type": "room-closed"}));
    assert!(
        matches!(frame(&mut guest_b).await, Message::Close(Some(f)) if f.code == CloseCode::Normal)
    );

    let mut late_guest = server.connect().await;
    send(&mut late_guest, json!({"type": "join", "room": room})).await;
    assert_eq!(
        receive(&mut late_guest).await,
        json!({"type": "error", "message": "room not found"})
    );
    assert!(
        matches!(frame(&mut late_guest).await, Message::Close(Some(f)) if f.code == CloseCode::Policy)
    );
}

#[tokio::test]
async fn rejects_wrong_first_message_and_binary_signaling() {
    let server = Server::start().await;
    let mut client = server.connect().await;
    send(
        &mut client,
        json!({"type": "signal", "to": Uuid::nil(), "signal": {}}),
    )
    .await;
    assert_eq!(receive(&mut client).await["type"], "error");
    assert!(
        matches!(frame(&mut client).await, Message::Close(Some(f)) if f.code == CloseCode::Policy)
    );

    let (mut host, _, _) = create(&server).await;
    host.send(Message::binary(vec![0, 1])).await.unwrap();
    assert!(
        matches!(frame(&mut host).await, Message::Close(Some(f)) if f.code == CloseCode::Policy)
    );
}

#[tokio::test]
async fn ready_precedes_an_immediate_signal_and_oversized_messages_disconnect() {
    let server = Server::start().await;
    let (mut host, room, host_id) = create(&server).await;
    let mut guest = server.connect().await;
    send(&mut guest, json!({"type": "join", "room": room})).await;
    let joined = receive(&mut host).await;
    assert_eq!(joined["type"], "peer-joined");
    let guest_id = uuid_field(&joined, "peer");
    let expected = roster(host_id, vec![guest_id]);
    assert_eq!(receive(&mut host).await, expected);
    let offer = json!({"description": {"type": "offer", "sdp": "v=0\r\n"}});
    send(
        &mut host,
        json!({"type": "signal", "to": guest_id, "signal": offer}),
    )
    .await;
    let ready = receive(&mut guest).await;
    let token = uuid_field(&ready, "relayToken");
    assert_eq!(
        ready,
        json!({"type": "ready", "room": room, "peer": guest_id, "role": "guest", "relayToken": token})
    );
    assert_eq!(receive(&mut guest).await, expected);
    assert_eq!(
        receive(&mut guest).await,
        json!({"type": "signal", "from": host_id, "signal": offer})
    );

    send(&mut guest, json!({"type": "signal", "to": host_id, "signal": {"description": {"type": "answer", "sdp": "x".repeat(16 * 1024)}}})).await;
    assert_eq!(
        receive(&mut host).await,
        json!({"type": "peer-left", "peer": guest_id})
    );
    assert_eq!(receive(&mut host).await, roster(host_id, vec![]));
}

#[tokio::test]
async fn broadcasts_only_valid_host_rates_to_current_guests() {
    let server = Server::start().await;
    let (mut host, room, host_id) = create(&server).await;
    let (mut guest_a, a) = join(&server, &mut host, room, host_id, &[]).await;
    let (mut guest_b, b) = join(&server, &mut host, room, host_id, &[a]).await;
    assert_eq!(receive(&mut guest_a).await, roster(host_id, vec![a, b]));
    let (mut departed, c) = join(&server, &mut host, room, host_id, &[a, b]).await;
    assert_eq!(receive(&mut guest_a).await, roster(host_id, vec![a, b, c]));
    assert_eq!(receive(&mut guest_b).await, roster(host_id, vec![a, b, c]));
    departed.close(None).await.unwrap();
    assert_eq!(
        receive(&mut host).await,
        json!({"type": "peer-left", "peer": c})
    );
    for client in [&mut host, &mut guest_a, &mut guest_b] {
        assert_eq!(receive(client).await, roster(host_id, vec![a, b]));
    }

    send(&mut guest_a, json!({"type": "rates", "rates": {(a): 123}})).await;
    // A signal on the same socket establishes that the guest's rates were processed.
    send(
        &mut guest_a,
        json!({"type": "signal", "to": host_id, "signal": {}}),
    )
    .await;
    assert_eq!(
        receive(&mut host).await,
        json!({"type": "signal", "from": a, "signal": {}})
    );

    for rates in [
        json!([]),
        json!({"not-a-uuid": 1}),
        json!({(a.to_string().replace('-', "")): 1}),
        json!({(host_id): 2}),
        json!({(a): 10, (c): 4}),
        json!({(a): -1}),
        json!({(a): 0.5}),
        json!({(a): "12"}),
        json!({(a): 9_007_199_254_740_992_u64}),
    ] {
        send(&mut host, json!({"type": "rates", "rates": rates})).await;
    }
    let valid = json!({"type": "rates", "rates": {(a): 0, (b): 9_007_199_254_740_991_u64}});
    send(&mut host, valid.clone()).await;
    assert_eq!(receive(&mut guest_a).await, valid);
    assert_eq!(receive(&mut guest_b).await, valid);
    send(
        &mut guest_b,
        json!({"type": "signal", "to": host_id, "signal": {}}),
    )
    .await;
    assert_eq!(
        receive(&mut host).await,
        json!({"type": "signal", "from": b, "signal": {}})
    );
}
