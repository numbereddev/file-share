# One-off File Share

Select files or folders, click **Share**, and send the invite link, code, or QR. Recipients join, see what is being offered, and choose a destination. Everyone in the room sees the roster and per-recipient transfer speeds. The recipient needs desktop Chromium (for example, Chrome or Edge) on **HTTPS or localhost** to stream to disk. The sender can use another browser with WebRTC; folder picker support varies.

## Docker Compose

```sh
docker compose up -d --build
```

The container serves the built React/Vite UI and Rust signaling on one port. Open `http://localhost:8080` for local use. By default, the port is published only on the host's loopback interface; set `APP_PORT` in `.env` to change it. No file volumes, database, or second web server are needed.

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

Open the site at its public HTTPS URL when creating an invite; that is the address encoded into the link and QR. File bytes never go through nginx, so nginx upload-size settings do not limit transfers. The signaling server sends heartbeat frames while idle.

## What happens to the file?

- **Direct when possible.** Rust exchanges WebRTC offers and ICE candidates and tracks the room and rates in memory. The sender reads slices from the original file and sends blocks up to 64 KiB over an encrypted, reliable data channel. The receiver writes them to disk and acknowledges batches of up to 4 MiB. Neither browser buffers the whole file, and the app has no fixed file-size cap below the browser or filesystem limits. Disk space, bandwidth, and both tabs staying open still matter; a 100 GB transfer was not benchmarked.
- **One stream per recipient.** Up to eight guests can join the room. A late joiner starts at byte zero without interrupting anyone else. The original sender supplies each stream, so its total upload scales with the number of active recipients. Recipients do not re-seed or swarm in this version.
- **Ephemeral.** A room has one file/folder selection. Leaving the room or disconnecting ends the session without resume. Received files are written into a new `File share …` subfolder. Completed files stay there if a later file is interrupted. Anyone with the invite can join while the room is open; recipients have to pick a destination before the sender starts writing to them.

### If direct connections fail

The default STUN service is `stun:stun.l.google.com:19302`. Change the comma-separated `VITE_STUN_URLS` Compose build argument in `.env` and rebuild to use another service, or set it empty to disable STUN. A restrictive NAT or firewall may still require a publicly reachable TURN server. To use an existing coturn instance with `use-auth-secret` and `static-auth-secret`, set in `.env`:

```dotenv
TURN_URLS=turn:turn.example.com:3478?transport=udp,turns:turn.example.com:5349?transport=tcp
TURN_SECRET=replace-with-your-existing-coturn-shared-secret
TURN_TTL_SECONDS=86400
```

Rust issues time-limited credentials; do not put the shared secret into frontend build variables. TURN forwards encrypted packets separately for each recipient without storing files. It is not an HTTP upload or server-side fan-out and can consume substantial bandwidth. Protect publicly exposed TURN with suitable access controls and quotas: this app does not authenticate room creators.

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
