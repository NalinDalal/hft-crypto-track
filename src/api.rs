use crate::state::AppState;
use axum::{extract::Query, extract::ws::{Message, WebSocket, WebSocketUpgrade}, response::IntoResponse, routing::get, Json, Router};
use serde::Deserialize;
use std::net::SocketAddr;
use tokio_stream::wrappers::BroadcastStream;
use futures::{StreamExt, SinkExt};

#[derive(Deserialize)]
pub struct PairQuery {
    pub pair: Option<String>,
    pub limit: Option<usize>,
}

pub fn make_router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/ticker", get(get_ticker))
        .route("/api/v1/history", get(get_history))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

async fn get_ticker(Query(q): Query<PairQuery>, state: axum::extract::State<AppState>) -> impl IntoResponse {
    let pair = q.pair.unwrap_or_else(|| "BTC/USD".to_string());
    if let Some(entry) = state.latest.get(&pair) {
        Json(serde_json::json!({"ok": true, "data": entry.value().clone()}))
    } else {
        Json(serde_json::json!({"ok": false, "error": "pair not found"}))
    }
}

async fn get_history(Query(q): Query<PairQuery>, state: axum::extract::State<AppState>) -> impl IntoResponse {
    let pair = q.pair.unwrap_or_else(|| "BTC/USD".to_string());
    let limit = q.limit.unwrap_or(60usize);
    if let Some(entry) = state.history.get(&pair) {
        let vec = entry.last_n(limit);
        Json(serde_json::json!({"ok": true, "data": vec}))
    } else {
        Json(serde_json::json!({"ok": false, "error": "pair not found"}))
    }
}

async fn ws_handler(ws: WebSocketUpgrade, Query(q): Query<PairQuery>, state: axum::extract::State<AppState>) -> impl IntoResponse {
    // we ignore pair filter on server side for simplicity; clients may filter ticks they receive
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: axum::extract::State<AppState>) {
    // subscribe to broadcast channel
    let rx = state.broadcaster.subscribe();
    let mut stream = BroadcastStream::new(rx);

    // merge incoming ping messages from client with outgoing broadcast
    loop {
        tokio::select! {
            // outgoing to client
            Some(Ok(tick)) = stream.next() => {
                if let Ok(txt) = serde_json::to_string(&tick) {
                    if socket.send(Message::Text(txt)).await.is_err() {
                        break;
                    }
                }
            }
            // incoming from client
            Some(Ok(msg)) = socket.recv() => {
                match msg {
                    Message::Text(s) => {
                        // for now, ignore or could parse subscribe command
                        tracing::debug!("ws recv text: {}", s);
                    }
                    Message::Ping(_) => {}
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            else => break,
        }
    }
}

