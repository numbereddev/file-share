import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fromFiles, pathParts } from "./selection.js";
import { CHUNK, WINDOW, Receiver, Sender } from "./transfer.js";

function channels() {
    function send(data) {
        queueMicrotask(() => this.peer.onmessage?.({ data }));
    }
    function close() {
        this.readyState = this.peer.readyState = "closed";
        queueMicrotask(() => this.peer.onclose?.());
    }
    const a = { readyState: "open", send, close };
    const b = { readyState: "open", send, close, peer: a };
    a.peer = b;
    return [a, b];
}

function destination() {
    const output = new Map();
    function directory(prefix) {
        const dirs = new Map();
        const files = new Map();
        return {
            async getDirectoryHandle(name, { create } = {}) {
                if (!dirs.has(name) && create) dirs.set(name, directory(prefix + name + "/"));
                if (!dirs.has(name)) throw new DOMException("Missing", "NotFoundError");
                return dirs.get(name);
            },
            async getFileHandle(name, { create } = {}) {
                if (!files.has(name) && create)
                    files.set(name, {
                        async createWritable() {
                            const chunks = [];
                            return {
                                async write(bytes) {
                                    chunks.push(new Uint8Array(bytes));
                                },
                                async close() {
                                    output.set(prefix + name, Buffer.concat(chunks));
                                },
                                async abort() {
                                    chunks.length = 0;
                                },
                            };
                        },
                    });
                if (!files.has(name)) throw new DOMException("Missing", "NotFoundError");
                return files.get(name);
            },
            async removeEntry(name) {
                files.delete(name);
            },
        };
    }
    return { output, parent: directory("") };
}

async function until(condition) {
    for (let i = 0; i < 200; i++) {
        if (condition()) return;
        await delay(5);
    }
    assert.fail("Transfer did not reach expected state");
}

test("streams nested and empty entries with negotiated chunk size and commits exact bytes", async () => {
    const [outgoing, incoming] = channels();
    const { output, parent } = destination();
    let largest = 0;
    const originalSend = outgoing.send;
    outgoing.send = function (data) {
        if (data instanceof ArrayBuffer) largest = Math.max(largest, data.byteLength);
        originalSend.call(this, data);
    };
    const original = Buffer.alloc(WINDOW + CHUNK, 37);
    const file = new File([original], "large.bin");
    Object.defineProperty(file, "webkitRelativePath", { value: "nested/large.bin" });
    const selection = fromFiles([file, new File([], "empty.txt")]);
    selection.entries.push({ path: "empty folder" });
    selection.summary.entries++;
    const sender = new Sender(outgoing, { sctp: { maxMessageSize: 32 * 1024 } }, () => {});
    const receiver = new Receiver(
        incoming,
        () => {},
        () => {},
    );
    try {
        sender.offer(selection);
        await until(() => receiver.summary);
        assert.equal(output.size, 0);
        const folder = await receiver.accept(parent);
        await until(() => sender.phase === "complete" && receiver.phase === "complete");
        assert.equal(largest, 32 * 1024);
        assert.equal(sender.bytes, original.byteLength);
        assert.deepEqual(output.get(`${folder}/nested/large.bin`), original);
        assert.equal(output.get(`${folder}/empty.txt`).length, 0);
        assert.ok(
            await (await parent.getDirectoryHandle(folder)).getDirectoryHandle("empty folder"),
        );
    } finally {
        sender.stop();
    }
});

test("reads at most one 4 MiB window before the recipient acknowledges disk writes", async () => {
    const [outgoing, incoming] = channels();
    let read = 0;
    let flush = false;
    const file = {
        name: "100GB.bin",
        size: 100_000_000_000,
        slice(start, end) {
            read += end - start;
            return {
                async arrayBuffer() {
                    return new ArrayBuffer(end - start);
                },
            };
        },
    };
    incoming.onmessage = ({ data }) => {
        if (typeof data !== "string") return;
        switch (JSON.parse(data).type) {
            case "summary":
                incoming.send('{"type":"accept"}');
                break;
            case "file":
                incoming.send('{"type":"ok"}');
                break;
            case "flush":
                flush = true;
                break;
        }
    };
    const sender = new Sender(outgoing, { sctp: { maxMessageSize: CHUNK } }, () => {});
    try {
        sender.offer(fromFiles([file]));
        await until(() => flush);
        assert.equal(read, WINDOW);
        assert.equal(sender.bytes, 0);
    } finally {
        sender.stop();
    }
});

test("rejects paths outside the destination and unannounced entries", async () => {
    for (const path of ["/absolute", "../outside", "foo/../outside", "foo\\bar", "foo//bar"]) {
        assert.throws(() => pathParts(path), /Invalid relative path/);
    }
    const [outgoing, incoming] = channels();
    const { parent, output } = destination();
    const receiver = new Receiver(
        incoming,
        () => {},
        () => {},
    );
    outgoing.send(
        JSON.stringify({ type: "summary", label: "invalid", bytes: 1, files: 1, entries: 1 }),
    );
    await until(() => receiver.summary);
    await receiver.accept(parent);
    outgoing.send(JSON.stringify({ type: "file", path: "undeclared-parent/file", size: 1 }));
    await until(() => receiver.phase === "error");
    assert.equal(output.size, 0);
});

test("disconnect discards an unfinished file", async () => {
    const [outgoing, incoming] = channels();
    const { parent, output } = destination();
    const receiver = new Receiver(
        incoming,
        () => {},
        () => {},
    );
    outgoing.send(
        JSON.stringify({ type: "summary", label: "interrupted", bytes: 4, files: 1, entries: 1 }),
    );
    await until(() => receiver.summary);
    const folder = await receiver.accept(parent);
    outgoing.send(JSON.stringify({ type: "file", path: "partial", size: 4 }));
    await until(() => receiver.file);
    outgoing.send(new Uint8Array([1, 2]).buffer);
    await until(() => receiver.bytes === 2);
    outgoing.close();
    await until(() => receiver.phase === "error");
    assert.equal(output.size, 0);
    const root = await parent.getDirectoryHandle(folder);
    await assert.rejects(root.getFileHandle("partial"), { name: "NotFoundError" });
});
