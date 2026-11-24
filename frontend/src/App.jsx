import React, { useEffect, useRef, useState } from "react";
import { QRCodeSVG } from "qrcode.react";
import { Room, ROOM_CODE } from "./room.js";
import { fromDirectory, fromDrop, fromFiles } from "./selection.js";

function formatBytes(bytes) {
    const units = ["B", "KB", "MB", "GB", "TB", "PB"];
    let unit = 0;
    while (bytes >= 1000 && unit < units.length - 1) {
        bytes /= 1000;
        unit++;
    }
    return `${bytes.toFixed(unit && bytes < 10 ? 1 : 0)} ${units[unit]}`;
}

function summaryText({ bytes, files }) {
    return `${files.toLocaleString()} ${files === 1 ? "file" : "files"} · ${formatBytes(bytes)}`;
}

function peerState(peer) {
    if (!peer) return "";
    if (peer.phase === "complete") return "Done";
    if (peer.phase === "error") return "Failed";
    if (peer.state === "disconnected") return "Reconnecting";
    if (peer.phase === "transferring") return "Transferring";
    if (peer.phase === "offered") return "Waiting for destination";
    if (peer.state === "connected") return "Ready";
    return "Connecting";
}

function invite(code) {
    const url = new URL(window.location.pathname, window.location.origin);
    url.hash = `room=${code}`;
    return url.href;
}

function Peers({ view }) {
    if (!view.host) return null;
    const speeds = view.rates ?? {};
    const total = view.members.reduce((sum, id) => sum + (speeds[id] ?? 0), 0);
    return (
        <section className="peers" aria-label="Connected clients">
            <h2>Connected clients</h2>
            <ul>
                {[view.host, ...view.members].map((id) => {
                    const host = id === view.host;
                    const peer = view.peers?.[id];
                    return (
                        <li key={id}>
                            <div>
                                <span>
                                    {host ? "Sender" : `Peer ${id.slice(0, 6)}`}
                                    {id === view.me ? " (you)" : ""}
                                </span>
                                {peer && (
                                    <small>
                                        {peerState(peer)}
                                        {peer.route ? ` · ${peer.route}` : ""}
                                    </small>
                                )}
                            </div>
                            <span className="rate">
                                {formatBytes(host ? total : (speeds[id] ?? 0))}/s
                            </span>
                            {peer?.error && <small className="peer-error">{peer.error}</small>}
                        </li>
                    );
                })}
            </ul>
        </section>
    );
}

export function App() {
    const [joinCode, setJoinCode] = useState(
        () => new URLSearchParams(location.hash.slice(1)).get("room") ?? "",
    );
    const [view, setView] = useState({ status: "idle" });
    const [selection, setSelection] = useState(null);
    const [selecting, setSelecting] = useState(false);
    const [picking, setPicking] = useState(false);
    const [dragging, setDragging] = useState(false);
    const [problem, setProblem] = useState("");
    const [copied, setCopied] = useState("");
    const [savedFolder, setSavedFolder] = useState("");
    const room = useRef(null);
    const selectionTask = useRef(null);
    const files = useRef(null);
    const folders = useRef(null);

    function open(code) {
        if (room.current) return;
        if (code && !ROOM_CODE.test(code))
            return setProblem("Enter the room code from the sender.");
        if (!window.RTCPeerConnection) return setProblem("This browser does not support WebRTC.");
        setProblem("");
        setView({ status: "opening", role: code ? "guest" : "host" });
        const connection = new Room(code, selection, (value) => {
            if (room.current === connection)
                setView(value.role ? value : { ...value, role: code ? "guest" : "host" });
        });
        room.current = connection;
    }

    useEffect(() => {
        if (joinCode) open(joinCode);
        return () => {
            const connection = room.current;
            room.current = null;
            connection?.end();
        };
    }, []);

    function leave() {
        const connection = room.current;
        room.current = null;
        connection?.end();
        selectionTask.current = null;
        setView({ status: "idle" });
        setJoinCode("");
        setSelection(null);
        setSelecting(false);
        setPicking(false);
        setProblem("");
        setCopied("");
        setSavedFolder("");
        if (location.hash) history.replaceState(null, "", location.pathname + location.search);
    }

    async function select(read) {
        if (selection || selectionTask.current) return;
        const task = {};
        selectionTask.current = task;
        setProblem("");
        setSelecting(true);
        try {
            const result = await read();
            if (selectionTask.current !== task) return;
            setSelection(result);
            room.current?.share(result);
        } catch (error) {
            if (selectionTask.current === task && error.name !== "AbortError")
                setProblem(error.message);
        } finally {
            if (selectionTask.current === task) {
                selectionTask.current = null;
                setSelecting(false);
            }
        }
    }

    async function copy(text, label) {
        try {
            await navigator.clipboard.writeText(text);
            setCopied(`${label} copied.`);
        } catch {
            setCopied("Clipboard unavailable. You can select the link or code below.");
        }
    }

    async function receive() {
        const connection = room.current;
        setPicking(true);
        setProblem("");
        try {
            const directory = await window.showDirectoryPicker({ mode: "readwrite" });
            if (room.current !== connection) return;
            const folder = await connection.accept(directory);
            if (room.current === connection) setSavedFolder(folder);
        } catch (error) {
            if (room.current === connection && error.name !== "AbortError")
                setProblem(error.message);
        } finally {
            if (room.current === connection) setPicking(false);
        }
    }

    const guest = view.role === "guest";
    const active = view.status === "open";
    const idle = view.status === "idle";
    const source = view.peers?.[view.host];
    const canReceive = window.isSecureContext && typeof window.showDirectoryPicker === "function";
    const summary = guest ? view.summary : selection?.summary;
    const pending = active && source?.phase === "offered";

    return (
        <main
            onDragOver={(event) => event.preventDefault()}
            onDrop={(event) => event.preventDefault()}
        >
            <header>
                <h1>One-off File Share</h1>
                {!idle && (
                    <button className="plain" onClick={leave}>
                        {view.status === "closed" ? "Back" : "Leave"}
                    </button>
                )}
            </header>

            <section className="transfer" aria-label={guest ? "Receive files" : "Send files"}>
                <div
                    className={`circle${dragging ? " dragging" : ""}`}
                    onDragOver={(event) => {
                        if (!guest && !selection && view.status !== "closed") {
                            event.dataTransfer.dropEffect = "copy";
                            setDragging(true);
                        }
                    }}
                    onDragLeave={() => setDragging(false)}
                    onDrop={(event) => {
                        event.preventDefault();
                        setDragging(false);
                        if (!guest && !selection && view.status !== "closed")
                            void select(() => fromDrop(event.dataTransfer));
                    }}
                >
                    {summary ? (
                        <>
                            <strong className="filename">{summary.label}</strong>
                            <small>{summaryText(summary)}</small>
                            {guest && source?.phase === "transferring" && (
                                <>
                                    <progress
                                        aria-label="Transfer progress"
                                        value={source.bytes}
                                        max={summary.bytes || 1}
                                    />
                                    <small>{formatBytes(source.bytes)} received</small>
                                </>
                            )}
                            {guest && source?.phase === "complete" && (
                                <strong>Transfer complete</strong>
                            )}
                        </>
                    ) : guest ? (
                        <strong>
                            {view.status === "opening" ? "Joining room…" : "Waiting for the sender"}
                        </strong>
                    ) : (
                        <>
                            <strong>
                                {selecting ? "Reading selection…" : "Drag your files or folders in"}
                            </strong>
                            <div className="pickers">
                                <button
                                    className="plain"
                                    disabled={selecting || view.status === "closed"}
                                    onClick={() => files.current.click()}
                                >
                                    Files
                                </button>
                                <span> / </span>
                                <button
                                    className="plain"
                                    disabled={selecting || view.status === "closed"}
                                    onClick={() => {
                                        if (window.showDirectoryPicker)
                                            void select(async () =>
                                                fromDirectory(
                                                    await window.showDirectoryPicker({
                                                        mode: "read",
                                                    }),
                                                ),
                                            );
                                        else folders.current.click();
                                    }}
                                >
                                    Folder
                                </button>
                            </div>
                            <input
                                ref={files}
                                type="file"
                                multiple
                                hidden
                                onChange={(event) => {
                                    const picked = Array.from(event.target.files);
                                    event.target.value = "";
                                    if (picked.length) void select(() => fromFiles(picked));
                                }}
                            />
                            <input
                                ref={folders}
                                type="file"
                                webkitdirectory=""
                                hidden
                                onChange={(event) => {
                                    const picked = Array.from(event.target.files);
                                    event.target.value = "";
                                    if (picked.length) void select(() => fromFiles(picked));
                                }}
                            />
                        </>
                    )}
                </div>

                {idle && (
                    <>
                        <button className="primary" onClick={() => open()}>
                            Share
                        </button>
                        {selection && (
                            <button className="plain" onClick={() => setSelection(null)}>
                                Clear selection
                            </button>
                        )}
                        <form
                            className="join"
                            onSubmit={(event) => {
                                event.preventDefault();
                                open(joinCode.trim());
                            }}
                        >
                            <input
                                type="text"
                                aria-label="Room code"
                                placeholder="Room code"
                                value={joinCode}
                                required
                                maxLength={36}
                                autoCapitalize="none"
                                spellCheck={false}
                                onChange={(event) => setJoinCode(event.target.value)}
                            />
                            <button>Join</button>
                        </form>
                    </>
                )}
                {!guest && view.status === "opening" && <p>Opening room…</p>}
                {!guest && active && (
                    <>
                        <button className="primary" onClick={() => copy(invite(view.code), "Link")}>
                            Copy invite link
                        </button>
                        <details>
                            <summary>Link, code &amp; QR</summary>
                            <label>
                                Invite link{" "}
                                <input
                                    readOnly
                                    value={invite(view.code)}
                                    onFocus={(event) => event.target.select()}
                                />
                            </label>
                            <label>
                                Room code{" "}
                                <input
                                    readOnly
                                    value={view.code}
                                    onFocus={(event) => event.target.select()}
                                />
                            </label>
                            <button onClick={() => copy(view.code, "Code")}>Copy code</button>
                            <QRCodeSVG
                                className="qr"
                                value={invite(view.code)}
                                size={144}
                                marginSize={4}
                                title="Room invite QR code"
                            />
                        </details>
                        {copied && <small role="status">{copied}</small>}
                        <small>Anyone with the invite can join while this room is open.</small>
                    </>
                )}
                {guest && pending && canReceive && (
                    <button className="primary" disabled={picking} onClick={receive}>
                        Choose destination
                    </button>
                )}
                {guest && active && !canReceive && (
                    <p className="error">
                        Use Chrome or Edge on desktop over HTTPS (or localhost) to save files
                        directly to disk.
                    </p>
                )}
                {savedFolder && (
                    <small>
                        Destination subfolder: <span className="filename">{savedFolder}</span>
                    </small>
                )}
                {(problem || view.error) && (
                    <p className="error" role="alert">
                        {problem || view.error}
                    </p>
                )}
            </section>
            {active && <Peers view={view} />}
        </main>
    );
}
