use crate::state::{AppState, Tick};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

/// Connects to CEX.IO WebSocket and keeps listening.
/// Parses ticker messages and pushes to AppState.
pub async fn run_ingest(state: AppState, pairs: Vec<String>) {
    // CEX.IO websocket endpoint
    let url = Url::parse("wss://ws.cex.io/ws/").unwrap();

    loop {
        match tokio_tungstenite::connect_async(url.clone()).await {
            Ok((ws_stream, _)) => {
                tracing::info!("Connected to CEX.IO WebSocket");
                let (mut write, mut read) = ws_stream.split();

                // subscribe to ticker rooms per pair
                // CEX.io rooms: e.g. "tickers:BTCUSD"
                let rooms: Vec<String> = pairs.iter().map(|p| p.replace("/", "")).map(|s| format!("tickers:{}", s)).collect();
                let sub = serde_json::json!({"e": "subscribe", "rooms": rooms});
                if let Err(e) = write.send(Message::Text(sub.to_string())).await {
                    tracing::error!("Failed to send subscribe: {e}");
                }

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if let Ok(json) = serde_json::from_str::<Value>(&txt) {
                                // cex.io ticker messages structure varies; example:
                                // {"e":"tickers","ok":"ok","data":{"pair":"BTC/USD","last":54123.1,"bid":"54100","ask":"54150","volume":"10"}}
                                // or sometimes nested differently. We'll try to be robust.
                                if json["e"] == "tickers" || json["e"] == "ticker" || json["e"] == "match" {
                                    // try to extract data
                                    let data = if json.get("data").is_some() { &json["data"] } else { &json };
                                    // attempt multiple keys
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
                tracing::warn!("Disconnected from CEX.IO — will retry in 3s");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
            Err(e) => {
                tracing::error!("Failed to connect to CEX.IO: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}
