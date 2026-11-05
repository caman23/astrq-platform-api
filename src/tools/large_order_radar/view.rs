//! Aggregated radar view for the dashboard: one entry per symbol that had at least one
//! large-order signal today, with its orders (latest state of each correlation id) and
//! child trades for hover details.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

use crate::core::fmt::py_round_int;
use crate::core::models::{Severity, Signal};
use crate::core::tool::ToolContext;
use crate::tools::large_order_radar::window::DayTotals;

fn order_from(s: &Signal, prev: Option<&Map<String, Value>>) -> Map<String, Value> {
    let mut g = s.features.get("group").and_then(Value::as_object).cloned().unwrap_or_default();
    let trades = g.remove("trades").unwrap_or_else(|| json!([]));
    let prev_peak = prev.and_then(|p| p["peak_score"].as_f64());
    let peak = prev_peak.map_or(s.score, |p| p.max(s.score));
    let mut peak_sev = s.severity;
    if let Some(ps) = prev.and_then(|p| p["peak_severity"].as_str()).and_then(Severity::parse) {
        if ps.rank() > peak_sev.rank() {
            peak_sev = ps;
        }
    }
    let mut o = Map::new();
    o.insert("id".into(), json!(s.correlation_id));
    o.insert("kind".into(), json!(s.kind));
    o.insert("state".into(), json!(s.state));
    o.insert("side".into(), json!(s.side.as_str()));
    o.insert("score".into(), json!(s.score));
    o.insert("severity".into(), json!(s.severity.as_str()));
    o.insert("peak_score".into(), json!(peak));
    o.insert("peak_severity".into(), json!(peak_sev.as_str()));
    o.insert("quality_flags".into(), json!(s.quality_flags));
    o.insert("updated_at".into(), json!(s.ts.to_rfc3339()));
    o.insert("updates".into(), json!(prev.and_then(|p| p["updates"].as_u64()).map_or(1, |u| u + 1)));
    o.insert("summary_vi".into(), s.features.get("summary_vi").cloned().unwrap_or_else(|| json!([])));
    o.insert("explanation".into(), serde_json::to_value(&s.explanation).unwrap_or_default());
    for (k, v) in g {
        o.insert(k, v);
    }
    o.insert("trades".into(), trades);
    o
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

struct Entry {
    orders: Vec<String>,
    by_id: HashMap<String, Map<String, Value>>,
    sector: Option<Value>,
    baseline: Option<Value>,
    metrics: Value,
    last_signal_at: String,
}

pub fn build_view(
    signals: &[Signal],
    ctx: &ToolContext,
    params: &HashMap<String, String>,
    days: &BTreeMap<String, DayTotals>,
) -> Value {
    let only = params.get("symbol").map(|s| s.to_ascii_uppercase());
    let with_trades: usize = params.get("orders_with_trades").and_then(|v| v.parse().ok()).unwrap_or(3);
    let mut order_of_symbols: Vec<String> = Vec::new();
    let mut by_sym: HashMap<String, Entry> = HashMap::new();
    for s in signals {
        if only.as_ref().is_some_and(|o| &s.symbol != o) {
            continue;
        }
        let e = by_sym.entry(s.symbol.clone()).or_insert_with(|| {
            order_of_symbols.push(s.symbol.clone());
            Entry { orders: vec![], by_id: HashMap::new(), sector: None, baseline: None, metrics: json!({}), last_signal_at: String::new() }
        });
        let prev = e.by_id.get(&s.correlation_id);
        let o = order_from(s, prev);
        if prev.is_none() {
            e.orders.push(s.correlation_id.clone());
        }
        e.by_id.insert(s.correlation_id.clone(), o);
        if let Some(m) = s.features.get("symbol_metrics") {
            e.metrics = m.clone();
        }
        if let Some(sec) = s.features.get("sector").filter(|v| !v.is_null()) {
            e.sector = Some(sec.clone());
        }
        if let Some(b) = s.features.get("baseline").filter(|v| !v.is_null()) {
            e.baseline = Some(b.clone());
        }
        e.last_signal_at = s.ts.to_rfc3339();
    }

    let universe = ctx.universe.read();
    let mut out: Vec<Value> = Vec::new();
    for sym in order_of_symbols {
        let e = by_sym.remove(&sym).expect("entry");
        let n_orders = e.orders.len();
        let mut orders: Vec<Map<String, Value>> = e.orders.iter().filter_map(|id| e.by_id.get(id).cloned()).collect();
        // newest first by last trade time (stable for equal keys, like Python's sort)
        orders.sort_by(|a, b| {
            let ka = a.get("last_at").and_then(Value::as_str).unwrap_or("");
            let kb = b.get("last_at").and_then(Value::as_str).unwrap_or("");
            kb.cmp(ka)
        });
        if only.is_none() {
            for o in orders.iter_mut().skip(with_trades) {
                o.insert("trades".into(), json!([]));
            }
            orders.truncate(30);
        }
        // Put-through deals are listed but do not position the symbol on the radar.
        let ranked: Vec<&Map<String, Value>> = {
            let r: Vec<&Map<String, Value>> = orders.iter().filter(|o| o["kind"] != "block_putthrough").collect();
            if r.is_empty() { orders.iter().collect() } else { r }
        };
        let peak = ranked.iter().map(|o| o["peak_score"].as_f64().unwrap_or(0.0)).fold(f64::MIN, f64::max);
        let sev = ranked
            .iter()
            .filter_map(|o| o["peak_severity"].as_str().and_then(Severity::parse))
            .max_by_key(|s| s.rank())
            .unwrap_or_default();
        let m = &e.metrics;
        let day = days.get(&sym);
        let num = |k: &str| m.get(k).cloned().unwrap_or(json!(0));
        let (buy, sell) = match day {
            Some(d) => (d.large_buy, d.large_sell),
            None => (m["day_large_buy"].as_f64().unwrap_or(0.0), m["day_large_sell"].as_f64().unwrap_or(0.0)),
        };
        let sector = e.sector.clone().unwrap_or_else(|| json!(universe.get(&sym).and_then(|i| i.sector.clone())));
        out.push(json!({
            "symbol": sym,
            "sector": sector,
            "max_score": peak,
            "max_severity": sev.as_str(),
            "dominant_side": dominant(buy, sell),
            "last_signal_at": e.last_signal_at,
            "n_orders": n_orders,
            "day_large_buy": day.map_or_else(|| num("day_large_buy"), |d| json!(py_round_int(d.large_buy))),
            "day_large_sell": day.map_or_else(|| num("day_large_sell"), |d| json!(py_round_int(d.large_sell))),
            "day_net_large": day.map_or_else(|| num("day_net_large"), |d| json!(py_round_int(d.net_large()))),
            "day_putthrough_value": day.map_or_else(|| num("day_putthrough_value"), |d| json!(py_round_int(d.putthrough_value))),
            "metrics": m,
            "baseline": e.baseline,
            "orders": orders,
        }));
    }
    out.sort_by(|a, b| {
        let sa = a["max_score"].as_f64().unwrap_or(0.0);
        let sb = b["max_score"].as_f64().unwrap_or(0.0);
        sb.total_cmp(&sa).then_with(|| a["symbol"].as_str().cmp(&b["symbol"].as_str()))
    });
    let trading_date = ctx.calendar.trading_date(ctx.clock.now()).to_string();
    json!({"trading_date": trading_date, "symbols": out})
}
