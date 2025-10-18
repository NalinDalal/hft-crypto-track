
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::broadcast;

/// A single tick (normalized)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tick {
    pub pair: String,
    pub last: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: Option<f64>,
    pub ts: DateTime<Utc>,
}

/// Per-pair ring buffer for recent ticks
pub struct PairHistory {
    pub buf: VecDeque<Tick>,
    pub capacity: usize,
}

impl PairHistory {
    pub fn new(capacity: usize) -> Self {
        Self { buf: VecDeque::with_capacity(capacity), capacity }
    }

    pub fn push(&mut self, tick: Tick) {
        if self.buf.len() == self.capacity {
            self.buf.pop_front();
        }
        self.buf.push_back(tick);
    }

    pub fn last_n(&self, n: usize) -> Vec<Tick> {
        let n = n.min(self.buf.len());
        self.buf.iter().rev().take(n).cloned().collect::<Vec<_>>().into_iter().rev().collect()
    }
}

/// Global in-memory state
#[derive(Clone)]
pub struct AppState {
    /// latest tick per pair
    pub latest: Arc<DashMap<String, Tick>>,
    /// history per pair
    pub history: Arc<DashMap<String, PairHistory>>,
    /// broadcast channel to notify websocket subscribers of new tick
    pub broadcaster: broadcast::Sender<Tick>,
    /// history capacity
    pub history_capacity: usize,
}

impl AppState {
    pub fn new(history_capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            latest: Arc::new(DashMap::new()),
            history: Arc::new(DashMap::new()),
            broadcaster: tx,
            history_capacity,
        }
    }

    pub fn insert_tick(&self, tick: Tick) {
        let pair_key = tick.pair.clone();
        self.latest.insert(pair_key.clone(), tick.clone());
        let mut entry = self.history.entry(pair_key.clone()).or_insert_with(|| PairHistory::new(self.history_capacity));
        entry.push(tick.clone());
        // ignore errors if no receivers
        let _ = self.broadcaster.send(tick);
    }
}
