import { MAX_ENTRIES, pathParts } from "./selection.js";

export const CHUNK = 64 * 1024;
export const WINDOW = 4 * 1024 * 1024;
const CONTROL_LIMIT = 32 * 1024;
const ACK_TIMEOUT = 120_000;
const integer = (value) => Number.isSafeInteger(value) && value >= 0;
const controlSize = (text) => new TextEncoder().encode(text).byteLength;

function readControl(text) {
    if (
        typeof text !== "string" ||
        text.length > CONTROL_LIMIT ||
        controlSize(text) > CONTROL_LIMIT
    ) {
        throw new Error("Invalid transfer message.");
    }
    const value = JSON.parse(text);
    if (!value || typeof value.type !== "string") throw new Error("Invalid transfer message.");
    return value;
}

class Transfer {
    constructor(channel, update) {
        this.channel = channel;
        this.update = update;
        this.bytes = 0;
        this.phase = "waiting";
        channel.binaryType = "arraybuffer";
        channel.onclose = () => this.stop("Connection closed.");
        channel.onerror = () => this.stop("Data channel failed.");
    }

    check() {
        if (this.stopped || this.channel.readyState !== "open")
            throw new Error("Connection closed.");
    }

    send(message) {
        this.check();
        this.channel.send(JSON.stringify(message));
    }

    report(phase, error) {
        this.phase = phase;
        this.update({ phase, bytes: this.bytes, error });
    }

    stop(reason) {
        if (this.stopped) return;
        this.stopped = true;
        this.cleanup?.();
        if (this.phase !== "complete") this.report("error", reason);
        this.channel.close();
    }
}

export class Sender extends Transfer {
    constructor(channel, connection, update) {
        super(channel, update);
        this.connection = connection;
        channel.onmessage = ({ data }) => {
            if (this.stopped) return;
            try {
                const message = readControl(data);
                if (message.type === "accept" && this.selection && !this.started) {
                    this.started = true;
                    this.run().catch((error) => this.stop(error.message));
                } else if (message.type === "ok" && this.pending) {
                    this.pending.resolve();
                } else {
                    throw new Error("Unexpected response from recipient.");
                }
            } catch (error) {
                this.stop(error.message);
            }
        };
    }

    offer(selection) {
        if (this.selection || this.stopped || this.channel.readyState !== "open") return;
        try {
            this.selection = selection;
            this.send({ type: "summary", ...selection.summary });
            this.report("offered");
        } catch (error) {
            this.stop(error.message);
        }
    }

    cleanup() {
        this.pending?.reject(new Error("Connection closed."));
    }

    exchange(message) {
        return new Promise((resolve, reject) => {
            const finish = (callback) => (value) => {
                clearTimeout(timer);
                this.pending = null;
                callback(value);
            };
            const timer = setTimeout(
                () => this.pending.reject(new Error("Recipient stopped responding.")),
                ACK_TIMEOUT,
            );
            this.pending = { resolve: finish(resolve), reject: finish(reject) };
            try {
                this.send(message);
            } catch (error) {
                this.pending.reject(error);
            }
        });
    }

    async run() {
        this.report("transferring");
        const maximum = this.connection.sctp?.maxMessageSize ?? 16 * 1024;
        const chunk = Math.min(CHUNK, maximum || CHUNK);
        for (const { path, file } of this.selection.entries) {
            if (!file) {
                await this.exchange({ type: "directory", path });
                continue;
            }
            await this.exchange({ type: "file", path, size: file.size });
            let offset = 0;
            while (offset < file.size) {
                const end = Math.min(file.size, offset + WINDOW);
                for (let start = offset; start < end; start += chunk) {
                    this.check();
                    const data = await file
                        .slice(start, Math.min(start + chunk, end))
                        .arrayBuffer();
                    this.check();
                    this.channel.send(data);
                }
                // The recipient acknowledges only after queued writes complete. This bounds both ends.
                await this.exchange({ type: "flush" });
                this.bytes += end - offset;
                offset = end;
                this.report("transferring");
            }
            await this.exchange({ type: "end-file" });
        }
        await this.exchange({ type: "complete" });
        if (!this.stopped) this.report("complete");
    }
}

export class Receiver extends Transfer {
    constructor(channel, update, onSummary) {
        super(channel, update);
        this.onSummary = onSummary;
        this.queue = Promise.resolve();
        this.queuedBytes = 0;
        this.queuedMessages = 0;
        this.entries = new Map();
        this.files = 0;
        this.windowBytes = 0;
        channel.onmessage = ({ data }) => this.enqueue(data);
    }

    enqueue(data) {
        if (this.stopped) return;
        const size = typeof data === "string" ? data.length * 2 : data.byteLength;
        this.queuedBytes += size;
        this.queuedMessages++;
        if (this.queuedBytes > WINDOW + 2 * CONTROL_LIMIT || this.queuedMessages > 4096) {
            this.stop("Sender exceeded the receive window.");
            return;
        }
        this.queue = this.queue.then(async () => {
            try {
                if (!this.stopped) await this.process(data);
            } catch (error) {
                this.stop(error.message);
            } finally {
                this.queuedBytes -= size;
                this.queuedMessages--;
            }
        });
    }

    async accept(parent) {
        this.check();
        if (!this.summary || this.root) throw new Error("No pending transfer.");
        const folder = `File share ${crypto.randomUUID()}`;
        const root = await parent.getDirectoryHandle(folder, { create: true });
        this.check();
        this.root = root;
        this.send({ type: "accept" });
        this.report("transferring");
        return folder;
    }

    cleanup() {
        if (this.file && !this.file.committing) void this.discard(this.file);
    }

    async discard({ writer, directory, name }) {
        await writer?.abort().catch(() => {});
        await directory.removeEntry(name).catch(() => {});
    }

    async requireNew(directory, name, kind) {
        try {
            if (kind === "directory") await directory.getDirectoryHandle(name);
            else await directory.getFileHandle(name);
        } catch (error) {
            if (error.name === "NotFoundError") {
                this.check();
                return;
            }
            throw error;
        }
        throw new Error(`A file or folder named ${name} already exists.`);
    }

    async directory(parts) {
        let directory = this.root;
        for (const part of parts) {
            this.check();
            directory = await directory.getDirectoryHandle(part);
        }
        this.check();
        return directory;
    }

    async process(data) {
        if (data instanceof ArrayBuffer) {
            if (
                !this.file ||
                !data.byteLength ||
                data.byteLength > CHUNK ||
                this.file.written + data.byteLength > this.file.size ||
                this.windowBytes + data.byteLength > WINDOW
            ) {
                throw new Error("Unexpected file data.");
            }
            await this.file.writer.write(data);
            this.file.written += data.byteLength;
            this.windowBytes += data.byteLength;
            this.bytes += data.byteLength;
            return;
        }

        const message = readControl(data);
        if (message.type === "summary" && !this.summary) {
            const { label, bytes, files, entries } = message;
            if (
                typeof label !== "string" ||
                !label.length ||
                label.length > 255 ||
                !integer(bytes) ||
                !integer(files) ||
                !integer(entries) ||
                !entries ||
                entries > MAX_ENTRIES ||
                files > entries
            ) {
                throw new Error("Invalid share details.");
            }
            this.summary = { label, bytes, files, entries };
            this.onSummary(this.summary);
            this.report("offered");
            return;
        }
        if (!this.root || this.phase === "complete")
            throw new Error("Unexpected transfer message.");

        switch (message.type) {
            case "directory":
            case "file": {
                const parts = pathParts(message.path);
                if (
                    this.file ||
                    this.entries.size >= this.summary.entries ||
                    this.entries.has(message.path)
                ) {
                    throw new Error("Unexpected file or folder.");
                }
                const name = parts.pop();
                if (parts.length && this.entries.get(parts.join("/")) !== "directory")
                    throw new Error("Missing parent folder.");
                this.entries.set(message.path, message.type);
                const directory = await this.directory(parts);
                await this.requireNew(directory, name, message.type);
                if (message.type === "directory") {
                    await directory.getDirectoryHandle(name, { create: true });
                } else {
                    if (
                        ++this.files > this.summary.files ||
                        !integer(message.size) ||
                        message.size > this.summary.bytes - this.bytes
                    ) {
                        throw new Error("Invalid file size or count.");
                    }
                    const handle = await directory.getFileHandle(name, { create: true });
                    const file = { directory, name, size: message.size, written: 0 };
                    try {
                        this.check();
                        file.writer = await handle.createWritable();
                        this.check();
                        this.file = file;
                    } catch (error) {
                        await this.discard(file);
                        throw error;
                    }
                }
                break;
            }
            case "flush":
                if (!this.file || !this.windowBytes) throw new Error("Unexpected file checkpoint.");
                this.windowBytes = 0;
                this.report("transferring");
                break;
            case "end-file":
                if (!this.file || this.file.written !== this.file.size || this.windowBytes)
                    throw new Error("Incomplete file.");
                this.file.committing = true;
                try {
                    await this.file.writer.close();
                } catch (error) {
                    await this.discard(this.file);
                    throw error;
                } finally {
                    this.file = null;
                }
                break;
            case "complete":
                if (
                    this.file ||
                    this.bytes !== this.summary.bytes ||
                    this.files !== this.summary.files ||
                    this.entries.size !== this.summary.entries
                ) {
                    throw new Error("Incomplete transfer.");
                }
                this.send({ type: "ok" });
                this.report("complete");
                return;
            default:
                throw new Error("Unknown transfer message.");
        }
        this.send({ type: "ok" });
    }
}
