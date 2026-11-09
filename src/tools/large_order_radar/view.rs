//! Aggregated radar view for the dashboard: one entry per symbol that had at least one
//! large-order signal today, with its orders (latest state of each correlation id) and
//! child trades for hover details.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::core::fmt::py_round_int;
use crate::core::models::{Severity, Signal};
use crate::core::tool::ToolContext;
use crate::tools::large_order_radar::window::DayTotals;

/// Per-order aggregate over all of its signals; only the latest signal's payload is
/// materialized, and only for orders that are actually returned.
struct OrderAgg<'a> {
    latest: &'a Signal,
    peak_score: f64,
    peak_severity: Severity,
    updates: u64,
}

impl OrderAgg<'_> {
    fn last_at(&self) -> &str {
        self.latest
            .features
            .get("group")
            .and_then(|g| g.get("last_at"))
            .and_then(Value::as_str)
            .unwrap_or("")
    }

    /// `trades`: None = no child trades, Some(n) = the last n (usize::MAX = all).
    /// `detail = false` leaves the drawer-only fields empty (keys kept for clients).
    fn to_json(&self, trades: Option<usize>, detail: bool) -> Map<String, Value> {
        let s = self.latest;
        let group = s.features.get("group").and_then(Value::as_object);
        let mut o = Map::new();
        o.insert("id".into(), json!(s.correlation_id));
        o.insert("kind".into(), json!(s.kind));
        o.insert("state".into(), json!(s.state));
        o.insert("side".into(), json!(s.side.as_str()));
        o.insert("score".into(), json!(s.score));
        o.insert("severity".into(), json!(s.severity.as_str()));
        o.insert("peak_score".into(), json!(self.peak_score));
        o.insert("peak_severity".into(), json!(self.peak_severity.as_str()));
        o.insert("quality_flags".into(), json!(s.quality_flags));
        o.insert("updated_at".into(), json!(s.ts.to_rfc3339()));
        o.insert("updates".into(), json!(self.updates));
        let summary = s.features.get("summary_vi").filter(|_| detail);
        o.insert(
            "summary_vi".into(),
            summary.cloned().unwrap_or_else(|| json!([])),
        );
        o.insert(
            "explanation".into(),
            if detail {
                serde_json::to_value(&s.explanation).unwrap_or_default()
            } else {
                json!([])
            },
        );
        if let Some(g) = group {
            for (k, v) in g {
                if k != "trades" {
                    o.insert(k.clone(), v.clone());
                }
            }
        }
        let all = group
            .and_then(|g| g.get("trades"))
            .and_then(Value::as_array);
        let trades = match (trades, all) {
            (Some(n), Some(all)) => Value::Array(all[all.len().saturating_sub(n)..].to_vec()),
            _ => json!([]),
        };
        o.insert("trades".into(), trades);
        o
    }
}

fn dominant(buy: f64, sell: f64) -> &'static str {
    let total = buy + sell;
    if total <= 0.0 {
        "unknown"
    } else if buy / total >= 0.6 {
        "buy"
    } else if sell / total >= 0.6 {
        "sell"
    } else {
        "mixed"
    }
}

struct Entry<'a> {
    order_ids: Vec<&'a str>,
    by_id: HashMap<&'a str, OrderAgg<'a>>,
    sector: Option<&'a Value>,
    baseline: Option<&'a Value>,
    metrics: Option<&'a Value>,
    last_signal: &'a Signal,
}

/// View modes:
/// - `?symbol=X`: every order of X with all child trades and score breakdowns (drawer);
/// - default overview: per symbol the newest `orders` (5) orders, the last `trades` (8)
///   child trades of the newest `orders_with_trades` (1) orders, and empty drawer-only
///   fields (metrics, baseline, summary, score breakdown) — what the radar hover shows;
/// - `?full=1`: the v1 overview (30 orders, trades for 3, with score breakdowns).
pub fn build_view(
    signals: &[Arc<Signal>],
    ctx: &ToolContext,
    params: &HashMap<String, String>,
    days: &BTreeMap<String, DayTotals>,
) -> Value {
    let only = params.get("symbol").map(|s| s.to_ascii_uppercase());
    let full = params.get("full").is_some_and(|v| v == "1" || v == "true");
    let int = |k: &str, d: usize| params.get(k).and_then(|v| v.parse().ok()).unwrap_or(d);
    let with_trades = int("orders_with_trades", if full { 3 } else { 1 });
    let max_orders = int("orders", if full { 30 } else { 5 }).min(30);
    let detail = full || only.is_some();
    let keep_trades = if detail { usize::MAX } else { int("trades", 8) };

    // Pass 1: aggregate without copying any payload.
    let mut order_of_symbols: Vec<&str> = Vec::new();
    let mut by_sym: HashMap<&str, Entry> = HashMap::new();
    for s in signals.iter().map(|s| &**s) {
        if only.as_ref().is_some_and(|o| &s.symbol != o) {
            continue;
        }
        let e = by_sym.entry(&s.symbol).or_insert_with(|| {
            order_of_symbols.push(&s.symbol);
            Entry {
                order_ids: vec![],
                by_id: HashMap::new(),
                sector: None,
                baseline: None,
                metrics: None,
                last_signal: s,
            }
        });
        match e.by_id.get_mut(s.correlation_id.as_str()) {
            Some(a) => {
                a.peak_score = a.peak_score.max(s.score);
                if s.severity.rank() >= a.peak_severity.rank() {
                    a.peak_severity = s.severity;
                }
                a.updates += 1;
                a.latest = s;
            }
            None => {
                e.order_ids.push(&s.correlation_id);
                e.by_id.insert(
                    &s.correlation_id,
                    OrderAgg {
                        latest: s,
                        peak_score: s.score,
                        peak_severity: s.severity,
                        updates: 1,
                    },
                );
            }
        }
        if let Some(m) = s.features.get("symbol_metrics") {
            e.metrics = Some(m);
        }
        if let Some(sec) = s.features.get("sector").filter(|v| !v.is_null()) {
            e.sector = Some(sec);
        }
        if let Some(b) = s.features.get("baseline").filter(|v| !v.is_null()) {
            e.baseline = Some(b);
        }
        e.last_signal = s;
    }

    // Pass 2: rank, then materialize only the orders that are returned.
    let universe = ctx.universe.read();
    let empty = json!({});
    let mut out: Vec<(f64, &str, Value)> = Vec::new();
    for sym in order_of_symbols {
        let e = by_sym.remove(sym).expect("entry");
        let n_orders = e.order_ids.len();
        let mut aggs: Vec<&OrderAgg> = e.order_ids.iter().map(|id| &e.by_id[id]).collect();
        // newest first by last trade time (stable for equal keys, like Python's sort)
        aggs.sort_by(|a, b| b.last_at().cmp(a.last_at()));
        if only.is_none() {
            aggs.truncate(30);
        }
        // Put-through deals are listed but do not position the symbol on the radar.
        let ranked: Vec<&&OrderAgg> = {
            let r: Vec<_> = aggs
                .iter()
                .filter(|a| a.latest.kind != "block_putthrough")
                .collect();
            if r.is_empty() {
                aggs.iter().collect()
            } else {
                r
            }
        };
        let peak = ranked.iter().map(|a| a.peak_score).fold(f64::MIN, f64::max);
        let sev = ranked
            .iter()
            .map(|a| a.peak_severity)
            .max_by_key(|s| s.rank())
            .unwrap_or_default();
        if only.is_none() {
            aggs.truncate(max_orders);
        }
        let orders: Vec<Map<String, Value>> = aggs
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let t = (only.is_some() || i < with_trades).then_some(keep_trades);
                a.to_json(t, detail)
            })
            .collect();

        let m = e.metrics.unwrap_or(&empty);
        let day = days.get(sym);
        let num = |k: &str| m.get(k).cloned().unwrap_or(json!(0));
        let (buy, sell) = match day {
            Some(d) => (d.large_buy, d.large_sell),
            None => (
                m["day_large_buy"].as_f64().unwrap_or(0.0),
                m["day_large_sell"].as_f64().unwrap_or(0.0),
            ),
        };
        let sector = e
            .sector
            .cloned()
            .unwrap_or_else(|| json!(universe.get(sym).and_then(|i| i.sector.clone())));
        out.push((
            peak,
            sym,
            json!({
                "symbol": sym,
                "sector": sector,
                "max_score": peak,
                "max_severity": sev.as_str(),
                "dominant_side": dominant(buy, sell),
                "last_signal_at": e.last_signal.ts.to_rfc3339(),
                "n_orders": n_orders,
                "day_large_buy": day.map_or_else(|| num("day_large_buy"), |d| json!(py_round_int(d.large_buy))),
                "day_large_sell": day.map_or_else(|| num("day_large_sell"), |d| json!(py_round_int(d.large_sell))),
                "day_net_large": day.map_or_else(|| num("day_net_large"), |d| json!(py_round_int(d.net_large()))),
                "day_putthrough_value": day.map_or_else(|| num("day_putthrough_value"), |d| json!(py_round_int(d.putthrough_value))),
                "metrics": if detail { m } else { &empty },
                "baseline": e.baseline.filter(|_| detail),
                "orders": orders,
            }),
        ));
    }
    out.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    let trading_date = ctx.calendar.trading_date(ctx.clock.now()).to_string();
    let symbols: Vec<Value> = out.into_iter().map(|(_, _, v)| v).collect();
    json!({"trading_date": trading_date, "symbols": symbols})
}
