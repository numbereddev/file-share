import { Receiver, Sender } from "./transfer.js";
import { relayChannel } from "./relay.js";

const defaultStun = "stun:stun.l.google.com:19302";
const stunUrls = (import.meta.env.VITE_STUN_URLS ?? defaultStun)
    .split(",")
    .map((url) => url.trim())
    .filter(Boolean);
export const ROOM_CODE = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
export const PROGRESS_MAX = 1000;
const RTC_TIMEOUT = 8000;
const RELAY_TIMEOUT = 18000;
const RELAY_FAILED =
    "Could not connect through the server. Check that the proxy allows WebSockets.";

export class Room {
    constructor(code, selection, onUpdate, password) {
        this.selection = selection;
        this.onUpdate = onUpdate;
        this.peers = new Map();
        this.members = [];
        this.rates = {};
        this.progress = {};
        this.status = "opening";
        const url = (this.signalUrl = new URL("/api/signal", window.location.href));
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
        const socket = (this.socket = new WebSocket(url));
        socket.onopen = () => {
            this.send(code ? { type: "join", room: code } : { type: "create", password });
            socket.onopen = null;
        };
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
            passwordRequired: this.passwordRequired,
            code: this.code,
            me: this.me,
            role: this.role,
            host: this.host,
            members: this.members,
            rates: { ...this.rates },
            progress: this.role === "host" ? this.recipientProgress() : this.progress,
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
                this.relayToken = message.relayToken;
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
                peer.timeout = setTimeout(() => this.requestRelay(peer), RTC_TIMEOUT);
                this.handle(peer, async () => {
                    const pc = this.openRtc(peer);
                    const channel = pc.createDataChannel("file-share");
                    channel.onopen = () => {
                        if (peer.closed || peer.mode !== "negotiating") return channel.close();
                        clearTimeout(peer.timeout);
                        peer.mode = "direct";
                        peer.state = "connected";
                        this.bindTransfer(peer, channel, pc);
                        this.publish();
                    };
                    await pc.setLocalDescription(await pc.createOffer());
                    if (peer.pc === pc)
                        this.signal(peer, { description: pc.localDescription.toJSON() });
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
                if (peer) {
                    if (message.signal.relay === true && this.role === "host")
                        this.requestRelay(peer);
                    else this.handle(peer, () => this.handleSignal(peer, message.signal));
                }
                break;
            }
            case "relay": {
                const id = this.role === "host" ? message.guest : this.host;
                if (!id || (this.role === "guest" && message.guest !== this.me)) break;
                const peer = this.peers.get(id) ?? this.setup(id);
                this.openRelay(peer, message.guest);
                break;
            }
            case "password-required":
                this.passwordRequired = true;
                this.end(message.incorrect ? "Incorrect room password." : undefined);
                break;
            case "rates":
                this.rates = message.rates;
                this.progress = message.progress ?? {};
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
        const peer = {
            id,
            mode: "negotiating",
            state: "connecting",
            bytes: 0,
            sampled: 0,
            queue: Promise.resolve(),
            candidates: [],
        };
        this.peers.set(id, peer);
        return peer;
    }

    openRtc(peer) {
        const pc = (peer.pc = new RTCPeerConnection(this.config));
        pc.onicecandidate = ({ candidate }) => {
            if (candidate && peer.pc === pc) this.signal(peer, { candidate: candidate.toJSON() });
        };
        pc.onconnectionstatechange = () => {
            if (peer.closed || peer.pc !== pc) return;
            peer.state = pc.connectionState;
            if (peer.state === "failed") this.rtcFailed(peer);
            this.publish();
        };
        if (this.role === "guest") {
            pc.ondatachannel = ({ channel }) => {
                if (
                    peer.closed ||
                    peer.pc !== pc ||
                    channel.label !== "file-share" ||
                    peer.transfer
                )
                    return channel.close();
                this.bindTransfer(peer, channel, pc);
            };
        }
        return pc;
    }

    bindTransfer(peer, channel, pc) {
        const update = (state) => {
            if (!peer.closed && peer.transfer === transfer) this.transferUpdate(peer, state);
        };
        const transfer =
            this.role === "host"
                ? new Sender(channel, pc, update)
                : new Receiver(channel, update, (summary) => {
                      if (pc) peer.mode = "direct";
                      this.summary = summary;
                  });
        peer.transfer = transfer;
        if (this.role === "host" && this.selection) transfer.offer(this.selection);
    }

    rtcFailed(peer) {
        if (peer.closed) return;
        if (peer.mode === "direct") {
            this.closePeer(peer, "Connection lost. Leave and rejoin to retry.");
        } else if (peer.mode === "negotiating") {
            if (this.role === "host") this.requestRelay(peer);
            else {
                this.waitForRelay(peer);
                this.signal(peer, { relay: true });
            }
        }
    }

    requestRelay(peer) {
        if (peer.closed || peer.mode !== "negotiating") return;
        this.waitForRelay(peer);
        this.send({ type: "relay", to: peer.id });
    }

    waitForRelay(peer) {
        clearTimeout(peer.timeout);
        peer.mode = "relay-requested";
        const pc = peer.pc;
        const transfer = peer.transfer;
        peer.pc = null;
        peer.transfer = null;
        transfer?.stop();
        pc?.close();
        Object.assign(peer, {
            state: "connecting",
            phase: "waiting",
            error: undefined,
            route: "Server relay",
        });
        peer.timeout = setTimeout(() => this.closePeer(peer, RELAY_FAILED), RELAY_TIMEOUT);
        this.publish();
    }

    openRelay(peer, guest) {
        if (peer.closed || peer.relay || peer.mode === "direct") return;
        this.waitForRelay(peer);
        let channel;
        try {
            channel = peer.relay = relayChannel(this.signalUrl, {
                room: this.code,
                guest,
                token: this.relayToken,
            });
        } catch {
            this.closePeer(peer, RELAY_FAILED);
            return;
        }
        const failed = () => this.closePeer(peer, RELAY_FAILED);
        channel.onclose = failed;
        channel.onerror = failed;
        channel.onopen = () => {
            clearTimeout(peer.timeout);
            peer.mode = "relay";
            peer.state = "connected";
            this.bindTransfer(peer, channel, null);
            this.publish();
        };
    }

    signal(peer, signal) {
        if (!peer.closed) this.send({ type: "signal", to: peer.id, signal });
    }

    handle(peer, task) {
        peer.queue = peer.queue
            .then(async () => {
                if (!peer.closed && (peer.mode === "negotiating" || peer.mode === "direct"))
                    await task();
            })
            .catch(() => this.rtcFailed(peer));
    }

    async handleSignal(peer, { description, candidate }) {
        if (!description && !candidate) return;
        const pc = peer.pc ?? this.openRtc(peer);
        if (description) {
            await pc.setRemoteDescription(description);
            for (const pending of peer.candidates.splice(0)) await pc.addIceCandidate(pending);
            if (description.type === "offer") {
                await pc.setLocalDescription(await pc.createAnswer());
                if (peer.pc === pc)
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
            const pc = peer.pc;
            const stats = await pc.getStats();
            if (peer.pc !== pc || peer.closed) return;
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

    recipientProgress() {
        const progress = {};
        const total = this.selection?.summary.bytes ?? 0;
        for (const { id, phase, bytes } of this.peers.values()) {
            if (phase === "complete") progress[id] = PROGRESS_MAX;
            else if (phase === "transferring") {
                progress[id] =
                    total > 0
                        ? Math.min(PROGRESS_MAX - 1, Math.floor((bytes / total) * PROGRESS_MAX))
                        : 0;
            }
        }
        return progress;
    }

    sample() {
        const now = performance.now();
        const elapsed = now - (this.sampledAt ?? now - 1000);
        this.sampledAt = now;
        for (const peer of this.peers.values()) {
            if (peer.pc && peer.state === "connected" && !peer.route) void this.findRoute(peer);
            if (this.role === "host") {
                this.rates[peer.id] = Math.max(
                    0,
                    Math.round(((peer.bytes - peer.sampled) * 1000) / elapsed),
                );
                peer.sampled = peer.bytes;
            }
        }
        if (this.role === "host" && this.members.length)
            this.send({ type: "rates", rates: this.rates, progress: this.recipientProgress() });
        if (this.status === "open") this.publish();
    }

    closePeer(peer, reason = "Connection closed.") {
        if (peer.closed) return;
        peer.closed = true;
        clearTimeout(peer.timeout);
        this.transferUpdate(peer, {
            phase: peer.phase === "complete" ? "complete" : "error",
            bytes: peer.bytes,
            error: peer.phase === "complete" ? undefined : reason,
        });
        peer.transfer?.stop(reason);
        peer.relay?.close();
        peer.pc?.close();
    }

    end(reason) {
        if (this.status === "closed") return;
        this.status = "closed";
        this.error = reason;
        this.rates = {};
        this.progress = {};
        clearInterval(this.timer);
        this.socket.close();
        for (const peer of this.peers.values()) this.closePeer(peer, reason);
        this.publish();
    }
}
