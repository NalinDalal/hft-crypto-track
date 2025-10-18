mod ws_ingest;
mod state;
mod api;

use crate::state::AppState;
use hyper::server::Server;
use std::net::SocketAddr;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // configuration
    let listen_addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
    let history_capacity = 500usize; // keep last 500 ticks per pair
    let pairs = vec!["BTC/USD".to_string()];

    let state = AppState::new(history_capacity);

    // spawn ingest task
    let ingest_state = state.clone();
    tokio::spawn(async move {
        ws_ingest::run_ingest(ingest_state, pairs).await;
    });

    // build http server
    let app = api::make_router(state.clone());

    tracing::info!("Starting server on {}", listen_addr);

    // Create a make service that provides client's connect info (SocketAddr)
    let make_svc = app.into_make_service_with_connect_info::<SocketAddr>();

    // Run the server using hyper's Server
    Server::bind(&listen_addr)
        .serve(make_svc)
        .await
        .unwrap();
}

