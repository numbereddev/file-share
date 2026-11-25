# One-off File Share

Select files or folders, click **Share**, and send the invite link, code, or QR. Recipients join and accept the offer. Everyone in the room sees the roster, transfer speeds, and other recipients' progress. Use **HTTPS or localhost** for receiving.

- **Chrome / Edge on desktop:** Choose a destination folder. Files stream directly there.
- **Firefox 111+ / Safari 26+:** Click **Receive files**, then **Download** once the transfer finishes. The browser stages the share in its private disk storage, not in RAM or on the server. A single file retains its name; folders and multiple files download as an uncompressed ZIP64 archive. Browser storage quotas apply, private browsing may block storage, and exporting needs space for both the staged and downloaded copies. For a 100 GB share, use the direct-to-folder option if possible. Keep the tab open until the download finishes, then use **Remove temporary copy** to reclaim browser storage. Inactive copies older than 24 hours are cleaned up on a later visit where browser locking is supported; clearing the site's data removes them immediately.

## Docker Compose

```sh
docker compose up -d --build
```

The container serves the built React/Vite UI and Rust backend on one port. Open `http://localhost:8080` for local use. By default, the port is published only on the host's loopback interface; set `APP_PORT` in `.env` to change it. No file volumes, database, or second web server are needed.

To restrict who can **create** a room, set a strong shared password in the Compose `.env`:

```dotenv
SHARE_PASSWORD='replace-with-a-long-random-password'
```

Apply an environment-only change with `docker compose up -d`. Clicking **Share** prompts for the password when it is configured. The Rust service enforces it; guests still join with just an invite. Unset or leave it empty for open room creation. Use HTTPS when exposing a password-protected site.

### Existing nginx on the host

Use your existing TLS certificate and vhost. Add these locations inside its `server` block, changing `8080` if you set a different `APP_PORT`:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
}

location = /api/signal {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 90s;
}
```

Open the site at its public HTTPS URL when creating an invite; that is the address encoded into the link and QR. The server relay uses the same `/api/signal` WebSocket location with `?relay=1`, so it needs no extra port or nginx rule. nginx's HTTP upload-size setting does not limit WebSocket transfers. The server sends heartbeat frames during idle connections.

## What happens to the file?

- **Direct when possible, server relay otherwise.** If a WebRTC data channel cannot open within about eight seconds, the app switches that recipient to an HTTPS WebSocket relay. Rust forwards blocks up to 64 KiB without saving files or buffering the whole transfer. Relayed bytes use server bandwidth and are visible to the server, unlike WebRTC file traffic. On both paths the receiver acknowledges batches of up to 4 MiB. Disk space, bandwidth, and both tabs staying open still matter; a 100 GB transfer was not benchmarked.
- **One stream per recipient.** Up to eight guests can join the room. A late joiner starts at byte zero without interrupting anyone else. The original sender supplies each stream, so its total upload scales with the number of active recipients. Recipients do not re-seed or swarm in this version.
- **Ephemeral.** A room has one file/folder selection. Leaving the room or disconnecting ends the session without resume. Direct-to-folder transfers use a new `File share …` subfolder and keep any completed files if a later file is interrupted. The browser-storage fallback discards interrupted shares. Anyone with the invite can join while the room is open, but transfers start only after each recipient accepts.

### Optional WebRTC/TURN configuration

The default STUN service is `stun:stun.l.google.com:19302`. Change the comma-separated `VITE_STUN_URLS` Compose build argument in `.env` and rebuild to use another service, or set it empty to disable STUN. The built-in WebSocket relay does not require TURN. To also allow WebRTC to use an existing coturn instance with `use-auth-secret` and `static-auth-secret`, set in `.env`:

```dotenv
TURN_URLS=turn:turn.example.com:3478?transport=udp,turns:turn.example.com:5349?transport=tcp
TURN_SECRET=replace-with-your-existing-coturn-shared-secret
TURN_TTL_SECONDS=86400
```

Rust issues time-limited credentials; do not put the shared secret into frontend build variables. TURN forwards encrypted packets separately for each recipient without storing files. Both types of relay can consume substantial bandwidth. Set `SHARE_PASSWORD` if you do not want the public creating rooms that use your server bandwidth.

## Local development

```sh
# Terminal 1
cd backend && cargo run

# Terminal 2
cd frontend && bun install --frozen-lockfile && bun --bun run dev
```

Open `http://localhost:5173`. Vite forwards signaling to Rust on port `3001`.

```sh
cd backend && cargo test --locked
cd ../frontend && bun --bun run test && bun --bun run build
```
