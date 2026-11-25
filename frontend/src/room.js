import { Receiver, Sender } from "./transfer.js";

const defaultStun = "stun:stun.l.google.com:19302";
const stunUrls = (import.meta.env.VITE_STUN_URLS ?? defaultStun)
    .split(",")
    .map((url) => url.trim())
    .filter(Boolean);
export const ROOM_CODE = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

export class Room {
    constructor(code, selection, onUpdate) {
        this.selection = selection;
        this.onUpdate = onUpdate;
        this.peers = new Map();
        this.members = [];
        this.rates = {};
        this.status = "opening";
        const url = new URL("/api/signal", window.location.href);
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
        const socket = (this.socket = new WebSocket(url));
        socket.onopen = () => this.send(code ? { type: "join", room: code } : { type: "create" });
        socket.onmessage = ({ data }) => {
            try {
                this.message(JSON.parse(data));
            } catch (error) {
                this.end(error.message);
            }
        };
        socket.onclose = () =>
            this.end(this.role ? "Room connection closed." : "Could not open the room.");
        this.timer = setInterval(() => this.sample(), 1000);
    }

    send(message) {
        if (this.socket.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify(message));
    }

    publish() {
        this.onUpdate({
            status: this.status,
            error: this.error,
            code: this.code,
            me: this.me,
            role: this.role,
            host: this.host,
            members: this.members,
            rates: { ...this.rates },
            summary: this.summary,
            peers: Object.fromEntries(
                [...this.peers].map(([id, { state, phase, bytes, error, route, download }]) => [
                    id,
                    { state, phase, bytes, error, route, download },
                ]),
            ),
        });
    }

    message(message) {
        if (this.status === "closed") return;
        switch (message.type) {
            case "ready":
                this.status = "open";
                this.code = message.room;
                this.me = message.peer;
                this.role = message.role;
                this.host = this.role === "host" ? this.me : undefined;
                this.config = {
                    iceServers: [
                        ...(stunUrls.length ? [{ urls: stunUrls }] : []),
                        ...(message.iceServers ?? []),
                    ],
                };
                this.publish();
                break;
            case "roster":
                this.host = message.host;
                this.members = message.peers;
                if (this.role === "guest")
                    this.rates = Object.fromEntries(
                        this.members.map((id) => [id, this.rates[id] ?? 0]),
                    );
                this.publish();
                break;
            case "peer-joined": {
                const peer = this.setup(message.peer);
                this.handle(peer, async () => {
                    const channel = peer.pc.createDataChannel("file-share");
                    const transfer = (peer.transfer = new Sender(channel, peer.pc, (state) =>
                        this.transferUpdate(peer, state),
                    ));
                    channel.onopen = () => {
                        if (this.selection) transfer.offer(this.selection);
                    };
                    await peer.pc.setLocalDescription(await peer.pc.createOffer());
                    this.signal(peer, { description: peer.pc.localDescription.toJSON() });
                });
                break;
            }
            case "peer-left": {
                const peer = this.peers.get(message.peer);
                this.peers.delete(message.peer);
                delete this.rates[message.peer];
                if (peer) this.closePeer(peer);
                this.publish();
                break;
            }
            case "signal": {
                const peer =
                    this.peers.get(message.from) ??
                    (this.role === "guest" ? this.setup(message.from) : null);
                if (peer) this.handle(peer, () => this.handleSignal(peer, message.signal));
                break;
            }
            case "rates":
                this.rates = message.rates;
                this.publish();
                break;
            case "room-closed":
                this.end("The sender closed the room.");
                break;
            case "error":
                this.end(message.message);
                break;
        }
    }

    setup(id) {
        const pc = new RTCPeerConnection(this.config);
        const peer = {
            id,
            pc,
            state: pc.connectionState,
            bytes: 0,
            sampled: 0,
            queue: Promise.resolve(),
            candidates: [],
        };
        this.peers.set(id, peer);
        pc.onicecandidate = ({ candidate }) => {
            if (candidate) this.signal(peer, { candidate: candidate.toJSON() });
        };
        pc.onconnectionstatechange = () => {
            if (peer.closed) return;
            peer.state = pc.connectionState;
            if (peer.state === "failed")
                this.closePeer(peer, "No network path. Try another network or configure TURN.");
            this.publish();
        };
        if (this.role === "guest") {
            pc.ondatachannel = ({ channel }) => {
                if (channel.label !== "file-share" || peer.transfer) return channel.close();
                peer.transfer = new Receiver(
                    channel,
                    (state) => this.transferUpdate(peer, state),
                    (summary) => {
                        this.summary = summary;
                    },
                );
            };
        }
        return peer;
    }

    signal(peer, signal) {
        if (!peer.closed) this.send({ type: "signal", to: peer.id, signal });
    }

    handle(peer, task) {
        peer.queue = peer.queue
            .then(async () => {
                if (!peer.closed) await task();
            })
            .catch((error) => {
                if (!peer.closed) this.closePeer(peer, error.message);
            });
    }

    async handleSignal(peer, { description, candidate }) {
        const { pc } = peer;
        if (description) {
            await pc.setRemoteDescription(description);
            for (const pending of peer.candidates.splice(0)) await pc.addIceCandidate(pending);
            if (description.type === "offer") {
                await pc.setLocalDescription(await pc.createAnswer());
                this.signal(peer, { description: pc.localDescription.toJSON() });
            }
        } else if (candidate) {
            if (pc.remoteDescription) await pc.addIceCandidate(candidate);
            else if (peer.candidates.length < 64) peer.candidates.push(candidate);
        }
    }

    transferUpdate(peer, update) {
        const changed = peer.phase !== update.phase || peer.error !== update.error;
        Object.assign(peer, update);
        if (changed && this.status !== "closed") this.publish();
    }

    share(selection) {
        this.selection = selection;
        for (const peer of this.peers.values()) peer.transfer?.offer(selection);
    }

    accept(destination) {
        const receiver = this.peers.get(this.host)?.transfer;
        if (!receiver) throw new Error("No connection to the sender.");
        receiver.accept(destination);
    }

    async findRoute(peer) {
        try {
            const stats = await peer.pc.getStats();
            const transport = [...stats.values()].find((report) => report.selectedCandidatePairId);
            const pair = stats.get(transport?.selectedCandidatePairId);
            const local = stats.get(pair?.localCandidateId);
            const remote = stats.get(pair?.remoteCandidateId);
            if (local && remote)
                peer.route = [local, remote].some((item) => item.candidateType === "relay")
                    ? "TURN"
                    : "Direct";
        } catch {
            /* Stats are optional; the transfer can continue. */
        }
    }

    sample() {
        const now = performance.now();
        const elapsed = now - (this.sampledAt ?? now - 1000);
        this.sampledAt = now;
        for (const peer of this.peers.values()) {
            if (peer.state === "connected" && !peer.route) void this.findRoute(peer);
            if (this.role === "host") {
                this.rates[peer.id] = Math.max(
                    0,
                    Math.round(((peer.bytes - peer.sampled) * 1000) / elapsed),
                );
                peer.sampled = peer.bytes;
            }
        }
        if (this.role === "host" && this.members.length)
            this.send({ type: "rates", rates: this.rates });
        if (this.status === "open") this.publish();
    }

    closePeer(peer, reason = "Connection closed.") {
        if (peer.closed) return;
        peer.closed = true;
        this.transferUpdate(peer, {
            phase: peer.phase === "complete" ? "complete" : "error",
            bytes: peer.bytes,
            error: peer.phase === "complete" ? undefined : reason,
        });
        peer.transfer?.stop(reason);
        peer.pc.close();
    }

    end(reason) {
        if (this.status === "closed") return;
        this.status = "closed";
        this.error = reason;
        this.rates = {};
        clearInterval(this.timer);
        this.socket.close();
        for (const peer of this.peers.values()) this.closePeer(peer, reason);
        this.publish();
    }
}
