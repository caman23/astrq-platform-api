//! Per-symbol sliding-window flow metrics, updated online in O(1) amortized.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;

use crate::core::fmt::{py_round, py_round_int};
use crate::core::models::Side;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DayTotals {
    pub large_buy: f64,
    pub large_sell: f64,
    pub large_unknown: f64,
    pub putthrough_value: f64,
    pub putthrough_count: u64,
    pub groups_opened: u64,
    pub auction_blocks: u64,
    pub max_score: f64,
}

impl DayTotals {
    pub fn net_large(&self) -> f64 {
        self.large_buy - self.large_sell
    }
}

/// (ts_epoch_seconds, side, value, flagged, price)
pub type Entry = (f64, Side, f64, bool, f64);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolWindow {
    pub window_sec: f64,
    pub entries: VecDeque<Entry>,
    pub active_buy: f64,
    pub active_sell: f64,
    pub large_buy: f64,
    pub large_sell: f64,
}

impl SymbolWindow {
    pub fn new(window_sec: f64) -> Self {
        Self {
            window_sec,
            ..Default::default()
        }
    }

    fn apply(&mut self, e: &Entry, sign: f64) {
        let (_, side, value, flagged, _) = *e;
        match side {
            Side::Buy => {
                self.active_buy += sign * value;
                if flagged {
                    self.large_buy += sign * value;
                }
            }
            Side::Sell => {
                self.active_sell += sign * value;
                if flagged {
                    self.large_sell += sign * value;
                }
            }
            Side::Unknown => {}
        }
    }

    pub fn add(&mut self, ts: f64, side: Side, value: f64, flagged: bool, price: f64) {
        let e = (ts, side, value, flagged, price);
        self.apply(&e, 1.0);
        self.entries.push_back(e);
        self.evict(ts);
    }

    pub fn evict(&mut self, now: f64) {
        let cutoff = now - self.window_sec;
        while self.entries.front().is_some_and(|e| e.0 < cutoff) {
            let e = self.entries.pop_front().expect("front");
            self.apply(&e, -1.0);
        }
    }

    /// Share of aggressive value on `side` coming from large/child-sized trades.
    pub fn side_concentration(&self, side: Side) -> f64 {
        match side {
            Side::Buy if self.active_buy > 0.0 => self.large_buy / self.active_buy,
            Side::Sell if self.active_sell > 0.0 => self.large_sell / self.active_sell,
            _ => 0.0,
        }
    }

    pub fn snapshot(&self) -> Value {
        let total = self.active_buy + self.active_sell;
        let first_price = self.entries.front().map_or(0.0, |e| e.4);
        let last_price = self.entries.back().map_or(0.0, |e| e.4);
        let span = if self.entries.len() > 1 {
            self.entries.back().unwrap().0 - self.entries.front().unwrap().0
        } else {
            0.0
        };
        let large = self.large_buy + self.large_sell;
        json!({
            "window_sec": self.window_sec,
            "large_buy": py_round_int(self.large_buy),
            "large_sell": py_round_int(self.large_sell),
            "net_large": py_round_int(self.large_buy - self.large_sell),
            "active_buy": py_round_int(self.active_buy),
            "active_sell": py_round_int(self.active_sell),
            "large_ratio": if total > 0.0 { py_round(large / total, 4) } else { 0.0 },
            "large_value_per_min": py_round_int(large / (span / 60.0).max(1.0)),
            "price_change_pct": if first_price != 0.0 { py_round((last_price - first_price) / first_price * 100.0, 3) } else { 0.0 },
        })
    }
}
