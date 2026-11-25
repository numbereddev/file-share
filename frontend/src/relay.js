// Present the same ordered text/binary interface as an RTCDataChannel.
export function relayChannel(signalUrl, credentials) {
    const url = new URL(signalUrl);
    url.searchParams.set("relay", "1");
    const socket = new WebSocket(url);
    socket.binaryType = "arraybuffer";
    const channel = {
        readyState: "connecting",
        send: (data) => socket.send(data),
        close() {
            if (channel.readyState === "closed" || channel.readyState === "closing") return;
            channel.readyState = "closing";
            socket.close();
        },
    };
    socket.onopen = () => socket.send(JSON.stringify(credentials));
    socket.onmessage = (event) => {
        if (channel.readyState === "open") {
            channel.onmessage?.(event);
        } else if (channel.readyState === "connecting") {
            if (event.data !== '{"type":"relay-ready"}') {
                channel.onerror?.();
                channel.close();
                return;
            }
            channel.readyState = "open";
            channel.onopen?.();
        }
    };
    socket.onclose = () => {
        channel.readyState = "closed";
        channel.onclose?.();
    };
    socket.onerror = () => channel.onerror?.();
    return channel;
}
