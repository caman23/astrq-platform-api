//! Follow-through: what did the price do after a signal?
//!
//! Every order (correlation id) is registered at its first signal with the reference
//! price at that moment. The price prevailing 5/15/30 minutes later is recorded exactly:
//! a checkpoint resolves with the last trade price *before* the first trade past its due
//! time (or on the timer when no trade comes). Per symbol and horizon the checkpoints sit
//! in a FIFO ordered by due time, so a tick costs O(1) amortized.

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap as HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};

use crate::core::fmt::py_round;
use crate::core::models::{Severity, Side};

/// (seconds, label)
pub const HORIZONS: [(f64, &str); 3] = [(300.0, "5m"), (900.0, "15m"), (1800.0, "30m")];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub symbol: String,
    pub family: String,
    pub side: Side,
    /// Severity at first detection (what a reader could act on).
    pub severity: Severity,
    pub ts: DateTime<Utc>,
    pub ref_price: f64,
    /// Raw price change (%) at each horizon; None until resolved.
    pub moves: [Option<f64>; 3],
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SymbolTrack {
    last_price: f64,
    /// per-horizon FIFO of (due epoch seconds, correlation id)
    pending: [VecDeque<(f64, String)>; 3],
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutcomeBook {
    pub orders: HashMap<String, Outcome>,
    /// One entry per symbol, so a trade costs a single map lookup.
    symbols: HashMap<String, SymbolTrack>,
}

fn epoch(ts: DateTime<Utc>) -> f64 {
    ts.timestamp_micros() as f64 / 1e6
}

fn pct(from: f64, to: f64) -> f64 {
    if from > 0.0 {
        (to - from) / from * 100.0
    } else {
        0.0
    }
}

/// Signal kind -> family used in the accuracy table.
pub fn family(kind: &str) -> &'static str {
    match kind {
        k if k.starts_with("block_order") => "block_order",
        "absorption" => "absorption",
        "accumulation" => "accumulation",
        "block_auction" => "block_auction",
        _ => "block_putthrough",
    }
}

impl OutcomeBook {
    pub fn clear(&mut self) {
        self.orders.clear();
        self.symbols.clear();
    }

    /// Register an order at its first signal. Later signals of the same order are ignored.
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        cid: &str,
        symbol: &str,
        kind: &str,
        side: Side,
        severity: Severity,
        ts: DateTime<Utc>,
        ref_price: f64,
    ) {
        if ref_price <= 0.0 || self.orders.contains_key(cid) {
            return;
        }
        self.orders.insert(
            cid.to_string(),
            Outcome {
                symbol: symbol.to_string(),
                family: family(kind).to_string(),
                side,
                severity,
                ts,
                ref_price,
                moves: [None; 3],
            },
        );
        let q = crate::tools::large_order_radar::flow::slot(&mut self.symbols, symbol);
        if q.last_price <= 0.0 {
            q.last_price = ref_price;
        }
        for (i, (h, _)) in HORIZONS.iter().enumerate() {
            q.pending[i].push_back((epoch(ts) + h, cid.to_string()));
        }
    }

    fn resolve_due(
        orders: &mut HashMap<String, Outcome>,
        q: &mut SymbolTrack,
        now: f64,
        price: f64,
    ) {
        for (i, fifo) in q.pending.iter_mut().enumerate() {
            while fifo.front().is_some_and(|(due, _)| *due <= now) {
                let (_, cid) = fifo.pop_front().expect("front");
                if let Some(o) = orders.get_mut(&cid) {
                    o.moves[i] = Some(pct(o.ref_price, price));
                }
            }
        }
    }

    /// A trade: checkpoints due before it see the price that prevailed until now.
    pub fn on_trade(&mut self, symbol: &str, ts: DateTime<Utc>, price: f64) {
        let q = crate::tools::large_order_radar::flow::slot(&mut self.symbols, symbol);
        let prev = q.last_price;
        if prev > 0.0 {
            Self::resolve_due(&mut self.orders, q, epoch(ts), prev);
        }
        q.last_price = price;
    }

    /// Timer: resolve checkpoints whose due time passed without a newer trade.
    pub fn on_timer(&mut self, now: DateTime<Utc>) {
        let t = epoch(now);
        for q in self.symbols.values_mut() {
            if q.last_price > 0.0 {
                let p = q.last_price;
                Self::resolve_due(&mut self.orders, q, t, p);
            }
        }
    }

    pub fn last_price(&self, symbol: &str) -> Option<f64> {
        self.symbols
            .get(symbol)
            .map(|q| q.last_price)
            .filter(|p| *p > 0.0)
    }

    /// Per-order JSON for the dashboard: horizon moves + move until now (or the close).
    /// With `at` (a past moment), only horizons elapsed by then, and no "now".
    pub fn order_json(&self, cid: &str, at: Option<DateTime<Utc>>) -> Value {
        let Some(o) = self.orders.get(cid) else {
            return Value::Null;
        };
        let now = self
            .last_price(&o.symbol)
            .filter(|_| at.is_none())
            .map(|p| py_round(pct(o.ref_price, p), 3));
        let mut m = serde_json::Map::new();
        m.insert("ref_price".into(), json!(o.ref_price));
        for (i, (h, label)) in HORIZONS.iter().enumerate() {
            let elapsed = at.is_none_or(|a| epoch(o.ts) + h <= epoch(a));
            let v = o.moves[i].filter(|_| elapsed).map(|x| py_round(x, 3));
            m.insert(label.to_string(), json!(v));
        }
        m.insert("now".into(), json!(now));
        Value::Object(m)
    }

    /// Accuracy table: for directional orders (buy/sell), the share whose price moved in
    /// the order's direction at each horizon, and the average directional move.
    pub fn stats(&self) -> Value {
        #[derive(Default)]
        struct Acc {
            n: [u32; 3],
            hits: [u32; 3],
            sum: [f64; 3],
            orders: u32,
        }
        let mut rows: BTreeMap<(u8, String), Acc> = BTreeMap::new();
        for o in self.orders.values() {
            let dir = match o.side {
                Side::Buy => 1.0,
                Side::Sell => -1.0,
                Side::Unknown => continue,
            };
            let keys = [
                (0u8, o.severity.as_str().to_string()),
                (1u8, o.family.clone()),
                (2u8, "all".to_string()),
            ];
            for k in keys {
                let a = rows.entry(k).or_default();
                a.orders += 1;
                for (i, m) in o.moves.iter().enumerate() {
                    if let Some(m) = m {
                        let d = dir * m;
                        a.n[i] += 1;
                        a.sum[i] += d;
                        if d > 0.0 {
                            a.hits[i] += 1;
                        }
                    }
                }
            }
        }
        let rows: Vec<Value> = rows
            .into_iter()
            .map(|((g, key), a)| {
                let per = |f: &dyn Fn(usize) -> Value| (0..3).map(f).collect::<Vec<_>>();
                let group = ["severity", "family", "all"][g as usize];
                json!({
                    "group": group,
                    "key": key,
                    "orders": a.orders,
                    "n": a.n,
                    "hit_rate": per(&|i| if a.n[i] > 0 { json!(py_round(a.hits[i] as f64 / a.n[i] as f64, 3)) } else { Value::Null }),
                    "avg_move": per(&|i| if a.n[i] > 0 { json!(py_round(a.sum[i] / a.n[i] as f64, 3)) } else { Value::Null }),
                })
            })
            .collect();
        json!({
            "horizons": HORIZONS.iter().map(|(_, l)| *l).collect::<Vec<_>>(),
            "rows": rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + s, 0).unwrap()
    }

    #[test]
    fn resolves_with_price_prevailing_at_due_time() {
        let mut b = OutcomeBook::default();
        b.on_trade("AAA", t(0), 100.0);
        b.register(
            "g1",
            "AAA",
            "block_order_open",
            Side::Buy,
            Severity::Alert,
            t(0),
            100.0,
        );
        b.on_trade("AAA", t(200), 101.0); // before 5m
        b.on_trade("AAA", t(299), 102.0); // last price before the 5m mark
        b.on_trade("AAA", t(310), 99.0); // first trade past 5m: resolves at 102
        assert_eq!(b.orders["g1"].moves[0], Some(2.0));
        assert_eq!(b.orders["g1"].moves[1], None);
        // no trade around 15m: the timer resolves with the last price (99)
        b.on_timer(t(905));
        assert_eq!(b.orders["g1"].moves[1], Some(-1.0));
        assert_eq!(b.order_json("g1", None)["now"], json!(-1.0));
        let past = b.order_json("g1", Some(t(400)));
        assert_eq!(
            (past["5m"].clone(), past["15m"].clone(), past["now"].clone()),
            (json!(2.0), Value::Null, Value::Null)
        );
    }

    #[test]
    fn stats_are_directional_and_skip_unknown_side() {
        let mut b = OutcomeBook::default();
        b.on_trade("AAA", t(0), 100.0);
        b.register(
            "buy",
            "AAA",
            "block_order_open",
            Side::Buy,
            Severity::Alert,
            t(0),
            100.0,
        );
        b.register(
            "sell",
            "AAA",
            "absorption",
            Side::Sell,
            Severity::Notice,
            t(0),
            100.0,
        );
        b.register(
            "pt",
            "AAA",
            "block_putthrough",
            Side::Unknown,
            Severity::Notice,
            t(0),
            100.0,
        );
        b.on_trade("AAA", t(250), 101.0);
        b.on_trade("AAA", t(301), 101.0); // resolves 5m at 101 (+1%)
        let s = b.stats();
        let row = |k: &str| {
            s["rows"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["key"] == k)
                .cloned()
                .unwrap()
        };
        assert_eq!(row("all")["orders"], 2, "put-through has no direction");
        assert_eq!(row("all")["hit_rate"][0], 0.5);
        assert_eq!(row("alert")["avg_move"][0], 1.0);
        assert_eq!(row("absorption")["avg_move"][0], -1.0);
        assert_eq!(row("all")["hit_rate"][1], Value::Null);
    }
}
