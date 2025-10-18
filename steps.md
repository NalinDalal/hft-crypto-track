What the server does
- Attempts to connect to CEX.IO WebSocket endpoints (3 attempts, 10s timeout each).
- If all attempts fail the server falls back to a mock data generator that updates every 500ms.
- The mock generator produces realistic BTC/USD prices around $65,000 with small random-walk movements and realistic spreads.

API endpoints
- GET /api/v1/pairs
  - Returns the list of available pairs the server is tracking.
  - Example response:
    {
      "ok": true,
      "pairs": ["BTC/USD"]
    }

- GET /api/v1/ticker?pair=BTC/USD
  - Returns the latest tick for the requested pair. Pair strings are normalized, so `BTC/USD` and `BTCUSD` will match.
  - Example response (mock data):
    {
      "ok": true,
      "data": {
        "pair": "BTC/USD",
        "last": 64904.06,
        "bid": 64903.43,
        "ask": 64904.69,
        "ts": "2025-10-18T11:58:52.015694655Z",
        "volume": null
      }
    }

- GET /api/v1/history?pair=BTC/USD&limit=60
  - Returns recent history (up to `limit` entries) for the pair.
  - Example response:
    { "ok": true, "data": [ /* array of tick objects */ ] }

- WebSocket: /ws
  - Connect to this WebSocket to receive broadcast ticks as they are produced.

Troubleshooting
- If the server logs show "TLS support not compiled in" when trying to connect to wss:// endpoints, your environment lacks TLS support for the WebSocket client. The server will automatically switch to the mock data generator.
- To see logs when the server is running in background:
```bash
tail -f server.log
# or if nohup was used
tail -f nohup.out
```

Quick test sequence
1. Build & run server
2. Confirm pairs are available
```bash
curl 'http://127.0.0.1:3000/api/v1/pairs'
```
3. Request the ticker
```bash
curl 'http://127.0.0.1:3000/api/v1/ticker?pair=BTC/USD'
```

Notes
- The mock data generator is intentionally conservative (small per-step changes). You can tweak ranges in `src/ws_ingest.rs` to increase volatility or add more pairs.
- If you want me to update the code to silence `rand` deprecation warnings, or add more pairs/history seeding, tell me which option you prefer.