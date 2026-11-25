import React, { useEffect, useRef, useState } from "react";
import { QRCodeSVG } from "qrcode.react";
import { Room, ROOM_CODE } from "./room.js";
import { openDirectoryDestination } from "./destination.js";
import { openStagedDestination, pruneStagedDownloads } from "./staging.js";
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
    const [needsPassword, setNeedsPassword] = useState(false);
    const [copied, setCopied] = useState("");
    const [savedFolder, setSavedFolder] = useState("");
    const [downloadRemoved, setDownloadRemoved] = useState(false);
    const stage = useRef(null);
    const room = useRef(null);
    const selectionTask = useRef(null);
    const files = useRef(null);
    const folders = useRef(null);
    const directSave = typeof window.showDirectoryPicker === "function";
    const canStage =
        typeof navigator.storage?.getDirectory === "function" &&
        typeof window.FileSystemFileHandle?.prototype.createWritable === "function";
    const canReceive = window.isSecureContext && (directSave || canStage);

    function releaseStage() {
        const previous = stage.current;
        stage.current = null;
        if (!previous) return;
        // Do not invalidate a download the browser may still be saving.
        if (previous.started) previous.destination.release();
        else void previous.destination.discard().catch(previous.destination.release);
    }

    function open(code, password) {
        if (room.current) return;
        if (code && !ROOM_CODE.test(code))
            return setProblem("Enter the room code from the sender.");
        setProblem("");
        setView({ status: "opening", role: code ? "guest" : "host" });
        const connection = new Room(
            code,
            selection,
            (value) => {
                if (room.current !== connection) return;
                if (value.passwordRequired) {
                    room.current = null;
                    setNeedsPassword(true);
                    setView({ status: "idle" });
                    setProblem(value.error ?? "");
                    return;
                }
                setView(value.role ? value : { ...value, role: code ? "guest" : "host" });
            },
            password,
        );
        room.current = connection;
    }

    useEffect(() => {
        if (joinCode) open(joinCode);
        if (canStage) void pruneStagedDownloads().catch(() => {});
        function onPageHide(event) {
            if (!event.persisted) releaseStage();
        }
        window.addEventListener("pagehide", onPageHide);
        return () => {
            window.removeEventListener("pagehide", onPageHide);
            const connection = room.current;
            room.current = null;
            connection?.end();
            releaseStage();
        };
    }, []);

    function leave() {
        const connection = room.current;
        room.current = null;
        connection?.end();
        releaseStage();
        selectionTask.current = null;
        setView({ status: "idle" });
        setJoinCode("");
        setSelection(null);
        setSelecting(false);
        setPicking(false);
        setProblem("");
        setCopied("");
        setSavedFolder("");
        setDownloadRemoved(false);
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
        let destination;
        try {
            if (directSave) {
                const parent = await window.showDirectoryPicker({ mode: "readwrite" });
                if (room.current !== connection) return;
                destination = await openDirectoryDestination(parent);
            } else {
                destination = await openStagedDestination(view.summary);
            }
            if (room.current !== connection) {
                await destination.discard?.();
                return;
            }
            connection.accept(destination);
            if (directSave) setSavedFolder(destination.folder);
            else stage.current = { destination, started: false };
        } catch (error) {
            await destination?.discard?.().catch(() => {});
            if (room.current === connection && error.name !== "AbortError") {
                const unavailable =
                    !directSave &&
                    ["UnknownError", "NotAllowedError", "SecurityError"].includes(error.name);
                setProblem(
                    unavailable
                        ? "Could not open browser storage. Try a regular window instead of private browsing."
                        : error.message,
                );
            }
        } finally {
            if (room.current === connection) setPicking(false);
        }
    }

    async function removeTemporaryCopy() {
        const current = stage.current;
        if (!current) return;
        try {
            await current.destination.discard();
            if (stage.current === current) {
                stage.current = null;
                setDownloadRemoved(true);
            }
        } catch (error) {
            if (stage.current === current) setProblem(error.message);
        }
    }

    const guest = view.role === "guest";
    const active = view.status === "open";
    const idle = view.status === "idle";
    const source = view.peers?.[view.host];
    const summary = guest ? view.summary : selection?.summary;
    const pending = active && source?.phase === "offered";
    const download = !downloadRemoved && source?.phase === "complete" && source.download;

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
                        {needsPassword ? (
                            <form
                                className="join"
                                onSubmit={(event) => {
                                    event.preventDefault();
                                    open(
                                        undefined,
                                        new FormData(event.currentTarget).get("password"),
                                    );
                                }}
                            >
                                <input
                                    type="password"
                                    name="password"
                                    aria-label="Room creation password"
                                    placeholder="Room creation password"
                                    autoComplete="current-password"
                                    required
                                />
                                <button className="primary">Create room</button>
                            </form>
                        ) : (
                            <button className="primary" onClick={() => open()}>
                                Share
                            </button>
                        )}
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
                    <>
                        <button className="primary" disabled={picking} onClick={receive}>
                            {directSave ? "Choose destination" : "Receive files"}
                        </button>
                        {!directSave && (
                            <small>
                                Kept in temporary browser storage until you can download it.
                            </small>
                        )}
                    </>
                )}
                {guest && active && !canReceive && (
                    <p className="error">
                        Receiving requires HTTPS or localhost and a browser that supports writing to
                        disk.
                    </p>
                )}
                {download && (
                    <>
                        <a
                            className="primary download"
                            href={download.url}
                            download={download.filename}
                            onClick={() => {
                                if (stage.current) stage.current.started = true;
                            }}
                        >
                            Download
                        </a>
                        <small>
                            Once your download finishes, you can remove the temporary copy.
                        </small>
                        <button className="plain" onClick={removeTemporaryCopy}>
                            Remove temporary copy
                        </button>
                    </>
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
