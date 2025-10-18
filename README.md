# High-Frequency Crypto Prices Tool -

**Project:** Develop a website, bot, or API for tracking high-frequency crypto prices. https://cex.io/api/ticker/BTC/USD

a lightweight API client that connects to CEX.IO’s WebSocket endpoint and listens to high-frequency price (ticker) updates, which you can then use to build your own API, bot, or dashboard.

[ref](https://github.com/psyipm/cexio-websocket/tree/master)

---

A minimal, production-minded Rust service that:

- Connects to CEX.IO WebSocket to receive ticker updates.
- Maintains latest tick per pair and a short recent history in memory.
- Exposes REST endpoints to get latest tick and recent history.
- Exposes a WebSocket server endpoint for clients to subscribe to live ticks.

## Features

- Written with tokio + axum.
- In-memory time-series ring buffer per pair (configurable capacity).
- Simple JSON REST API.

## Run

1. `cargo build --release`
2. `RUST_LOG=info cargo run --release`

The service listens on `127.0.0.1:3000` by default.

## Endpoints

- `GET /api/v1/ticker?pair=BTC/USD` — latest tick (JSON)
- `GET /api/v1/history?pair=BTC/USD&limit=60` — last `limit` ticks (default 60)
- `GET /ws?pair=BTC/USD` — WebSocket subscription for live ticks

## Notes

- This service uses CEX.IO's public WebSocket `wss://ws.cex.io/ws/`.
- For "high-frequency" updates, WebSocket ingestion is used. Data retention is short to avoid unbounded memory growth.
- If you need persistent storage or very long histories, plug in a time-series DB (InfluxDB/TimescaleDB) instead of the in-memory buffers.
