FROM oven/bun:1.4.5-alpine AS frontend
WORKDIR /app
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend/ ./
ARG VITE_STUN_URLS=stun:stun.l.google.com:19302
RUN bun --bun run build

FROM rust:1-bookworm AS backend
WORKDIR /app
COPY backend/Cargo.toml backend/Cargo.lock ./
COPY backend/src/ ./src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=backend /app/target/release/file-share-backend /usr/local/bin/file-share-backend
COPY --from=frontend /app/dist /app/dist
ENV BIND_ADDR=0.0.0.0:3001 STATIC_DIR=/app/dist
USER 65534:65534
EXPOSE 3001
ENTRYPOINT ["file-share-backend"]
