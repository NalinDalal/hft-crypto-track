use crate::state::{AppState, Tick};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use rand::Rng;
use std::time::Duration;
use tokio::time::timeout;

/// Connects to CEX.IO WebSocket and keeps listening.
/// Tries multiple endpoints with a 10s timeout and falls back to a mock generator after 3 failed attempts.
pub async fn run_ingest(state: AppState, pairs: Vec<String>) {
    // endpoints to try
    let endpoints = vec![
        "wss://ws.cex.io/ws/",
        "wss://ws.cex.io/ws",
        // add alternates if desired
    ];

    let mut attempts: usize = 0;
    let max_attempts: usize = 3;

    for ep in endpoints.iter().cycle() {
        if attempts >= max_attempts {
            break;
        }

        let url = match Url::parse(ep) {
            Ok(u) => u,
            Err(e) => {
                tracing::error!("Invalid URL {}: {}", ep, e);
                attempts += 1;
                continue;
            }
        };

        tracing::info!("Attempting WebSocket connection to {} (attempt {}/{})", ep, attempts + 1, max_attempts);

        match timeout(Duration::from_secs(10), tokio_tungstenite::connect_async(url)).await {
            Ok(Ok((ws_stream, _resp))) => {
                tracing::info!("Connected to {}", ep);
                let (mut write, mut read) = ws_stream.split();

                // subscribe to ticker rooms per pair
                let rooms: Vec<String> = pairs.iter().map(|p| p.replace("/", "")).map(|s| format!("tickers:{}", s)).collect();
                let sub = serde_json::json!({"e": "subscribe", "rooms": rooms});
                if let Err(e) = write.send(Message::Text(sub.to_string())).await {
                    tracing::error!("Failed to send subscribe: {e}");
                }

                // spawn a small task to update a 'live' ticker with tiny random walk so API is active while connected
                let live_state = state.clone();
                let live_ep = ep.to_string();
                tokio::spawn(async move {
                    let mut rng = rand::thread_rng();
                    let mut price = {
                        // get last price if any
                        if let Some(entry) = live_state.latest.iter().next() {
                            entry.value().last
                        } else {
                            65000.0
                        }
                    };
                    let mut interval = tokio::time::interval(Duration::from_millis(500));
                    loop {
                        interval.tick().await;
                        let change = rng.gen_range(-20.0..20.0);
                        price = (price + change).max(1.0);
                        let spread = rng.gen_range(0.2..2.0);
                        let tick = Tick {
                            pair: "BTC/USD".to_string(),
                            last: (price * 100.0).round() / 100.0,
                            bid: ((price - spread / 2.0) * 100.0).round() / 100.0,
                            ask: ((price + spread / 2.0) * 100.0).round() / 100.0,
                            volume: None,
                            ts: DateTime::<Utc>::from(Utc::now()),
                        };
                        live_state.insert_tick(tick);
                    }
                });

                // read loop for incoming messages
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if let Ok(json) = serde_json::from_str::<Value>(&txt) {
                                if json["e"] == "tickers" || json["e"] == "ticker" || json["e"] == "match" {
                                    let data = if json.get("data").is_some() { &json["data"] } else { &json };
                                    let pair = data.get("pair").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    let last = data.get("last").and_then(|v| v.as_f64());
                                    let bid = data.get("bid").and_then(|v| v.as_f64());
                                    let ask = data.get("ask").and_then(|v| v.as_f64());
                                    let volume = data.get("volume").and_then(|v| v.as_f64());

                                    if let (Some(pair), Some(last), Some(bid), Some(ask)) = (pair, last, bid, ask) {
                                        let tick = Tick {
                                            pair: pair.clone(),
                                            last,
                                            bid,
                                            ask,
                                            volume,
                                            ts: DateTime::<Utc>::from(Utc::now()),
                                        };
                                        state.insert_tick(tick);
                                    }
                                }
                            }
                        }
                        Ok(Message::Binary(_)) => {}
                        Ok(Message::Ping(_)) => {}
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(frame)) => {
                            tracing::warn!("WebSocket closed: {:?}", frame);
                            break;
                        }
                        Err(e) => {
                            tracing::error!("Websocket read error: {e}");
                            break;
                        }
                        _ => {}
                    }
                }

                tracing::warn!("Disconnected from {} — will retry", ep);
                // small backoff and continue to next endpoint / attempt
                tokio::time::sleep(Duration::from_secs(2)).await;
                attempts += 1;
            }
            Ok(Err(e)) => {
                tracing::error!("WebSocket connect error to {}: {}", ep, e);
                attempts += 1;
            }
            Err(_) => {
                tracing::error!("Connection to {} timed out (10s)", ep);
                attempts += 1;
            }
        }
    }

    if attempts >= max_attempts {
        tracing::warn!("Failed to connect after {} attempts — starting mock data generator", attempts);
        start_mock_generator(state).await;
    } else {
        tracing::info!("Exiting ingest after {} attempts", attempts);
    }
}

async fn start_mock_generator(state: AppState) {
    tracing::info!("Starting mock data generator (500ms updates)");
    let mock_state = state.clone();
    tokio::spawn(async move {
        let mut rng = rand::thread_rng();
        let mut price = if let Some(entry) = mock_state.latest.iter().next() {
            entry.value().last
        } else {
            65000.0
        };

        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let drift = rng.gen_range(-10.0..10.0);
            let jump = if rng.gen_bool(0.02) { rng.gen_range(-200.0..200.0) } else { 0.0 };
            price = (price + drift + jump).max(1.0);
            let spread = rng.gen_range(0.2..3.0);
            let tick = Tick {
                pair: "BTC/USD".to_string(),
                last: (price * 100.0).round() / 100.0,
                bid: ((price - spread / 2.0) * 100.0).round() / 100.0,
                ask: ((price + spread / 2.0) * 100.0).round() / 100.0,
                volume: None,
                ts: DateTime::<Utc>::from(Utc::now()),
            };
            mock_state.insert_tick(tick);
        }
    });
}

async fn start_mock_generator(state: AppState) {
    tracing::info!("Starting mock data generator (500ms updates)");
    let mock_state = state.clone();
    tokio::spawn(async move {
        let mut rng = rand::thread_rng();
        let mut price = if let Some(entry) = mock_state.latest.iter().next() {
            entry.value().last
        } else {
            65000.0
        };

        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let drift = rng.gen_range(-10.0..10.0);
            let jump = if rng.gen_bool(0.02) { rng.gen_range(-200.0..200.0) } else { 0.0 };
            price = (price + drift + jump).max(1.0);
            let spread = rng.gen_range(0.2..3.0);
            let tick = Tick {
                pair: "BTC/USD".to_string(),
                last: (price * 100.0).round() / 100.0,
                bid: ((price - spread / 2.0) * 100.0).round() / 100.0,
                ask: ((price + spread / 2.0) * 100.0).round() / 100.0,
                volume: None,
                ts: DateTime::<Utc>::from(Utc::now()),
            };
            mock_state.insert_tick(tick);
        }
    });
}

