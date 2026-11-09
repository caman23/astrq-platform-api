//! Shared test helpers: tiny universe, fixed baselines, tick builders, offline runner.
//! Mirrors the original Python test helpers so golden files are directly comparable.
#![allow(dead_code)]

use anyhow::Result;
use astrq::core::config::{Settings, ToolEntry};
use astrq::core::context::{Baseline, ContextProvider, ContextService};
use astrq::core::models::{Instrument, MarketEvent, Quote, Session, Side, SideSource, Tick};
use astrq::core::offline::{run_offline, OfflineResult};
use astrq::core::universe::Universe;
use astrq::tools::large_order_radar::config::RadarConfig;
use astrq::tools::large_order_radar::LargeOrderRadar;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::Asia::Ho_Chi_Minh;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

pub const DAY: (i32, u32, u32) = (2026, 10, 21);
pub const SYMBOLS: [&str; 2] = ["AAA", "BBB"];

pub fn ts(hms: &str, ms: u32) -> DateTime<Utc> {
    let p: Vec<u32> = hms.split(':').map(|x| x.parse().unwrap()).collect();
    Ho_Chi_Minh
        .with_ymd_and_hms(DAY.0, DAY.1, DAY.2, p[0], p[1], p[2])
        .unwrap()
        .with_timezone(&Utc)
        + chrono::Duration::milliseconds(ms as i64)
}

pub fn hms_of(t: DateTime<Utc>) -> String {
    t.with_timezone(&Ho_Chi_Minh).format("%H:%M:%S").to_string()
}

/// median trade value 20M VND, mid tier => group open threshold 500M.
pub struct FixedContext {
    pub median: f64,
}

#[async_trait]
impl ContextProvider for FixedContext {
    fn name(&self) -> &str {
        "fixed"
    }
    async fn baseline(&self, symbol: &str, d: NaiveDate) -> Result<Option<Baseline>> {
        Ok(Some(Baseline {
            symbol: symbol.into(),
            trading_date: d,
            median_trade_value: self.median,
            p95_trade_value: self.median * 7.0,
            avg_daily_value: 50e9,
            liquidity_tier: "mid".into(),
            source: "test".into(),
        }))
    }
}

pub fn fixed_context() -> Arc<ContextService> {
    Arc::new(ContextService::new(Arc::new(FixedContext { median: 20e6 })))
}

thread_local! {
    static SEQ: RefCell<HashMap<String, i64>> = RefCell::new(HashMap::new());
}

pub fn reset_seq() {
    SEQ.with(|s| s.borrow_mut().clear());
}

pub struct T {
    pub ms: u32,
    pub session: Option<Session>,
    pub negotiated: bool,
    pub provider_side: bool,
}

impl Default for T {
    fn default() -> Self {
        Self {
            ms: 0,
            session: None,
            negotiated: false,
            provider_side: true,
        }
    }
}

pub fn tick_opt(sym: &str, hms: &str, price: f64, volume: i64, side: Side, o: T) -> MarketEvent {
    let seq = SEQ.with(|s| {
        let mut m = s.borrow_mut();
        let n = m.entry(sym.to_string()).or_insert(0);
        *n += 1;
        *n
    });
    let t = ts(hms, o.ms);
    MarketEvent::Tick(Tick {
        provider: "test".into(),
        symbol: sym.into(),
        ts_exchange: t,
        ts_received: t,
        price,
        volume,
        value: price * volume as f64,
        side: if o.provider_side { side } else { Side::Unknown },
        side_source: if o.provider_side {
            SideSource::Provider
        } else {
            SideSource::Unknown
        },
        session: o.session.unwrap_or(Session::Closed),
        is_negotiated: o.negotiated,
        seq: Some(seq),
        raw_ref: None,
        ingest_mono: None,
    })
}

pub fn tick(sym: &str, hms: &str, price: f64, volume: i64, side: Side) -> MarketEvent {
    tick_opt(sym, hms, price, volume, side, T::default())
}

pub fn quote(sym: &str, hms: &str, bid: f64, ask: f64) -> MarketEvent {
    let t = ts(hms, 0);
    MarketEvent::Quote(Quote {
        provider: "test".into(),
        symbol: sym.into(),
        ts_exchange: t,
        ts_received: t,
        best_bid: Some(bid),
        best_ask: Some(ask),
        bid_size: None,
        ask_size: None,
        ingest_mono: None,
    })
}

pub fn universe() -> Universe {
    Universe::from_instruments(
        "test",
        SYMBOLS
            .iter()
            .map(|s| Instrument::new(s, Some("Test")))
            .collect(),
    )
}

pub fn radar_entry() -> ToolEntry {
    ToolEntry::default()
}

pub async fn run_with(events: Vec<MarketEvent>, cfg: RadarConfig, flush: bool) -> OfflineResult {
    run_offline(
        events,
        &Settings::default(),
        universe(),
        fixed_context(),
        vec![(Box::new(LargeOrderRadar::new(cfg)), radar_entry())],
        true,
        flush,
    )
    .await
    .unwrap()
}

pub async fn run(events: Vec<MarketEvent>) -> OfflineResult {
    run_with(events, RadarConfig::default(), true).await
}

/// Compact, stable projection of signals for golden comparison (same as Python).
pub fn project(r: &OfflineResult) -> Vec<Value> {
    r.signals
        .iter()
        .map(|s| {
            let g = s.features.get("group").cloned().unwrap_or(json!({}));
            json!({
                "t": hms_of(s.ts),
                "symbol": s.symbol,
                "kind": s.kind,
                "side": s.side.as_str(),
                "n": g.get("n_trades").cloned().unwrap_or(Value::Null),
                "value": g.get("total_value").cloned().unwrap_or(Value::Null),
                "score": s.score,
                "severity": s.severity.as_str(),
                "flags": s.quality_flags,
            })
        })
        .collect()
}

// ------------------------------------------------------------------ scenarios

fn noise(sym: &str, start: &str, n: usize, every: i64) -> Vec<MarketEvent> {
    let t0 = ts(start, 0);
    (0..n)
        .map(|i| {
            let t = t0 + chrono::Duration::seconds(i as i64 * every);
            let side = if i % 2 == 1 { Side::Buy } else { Side::Sell };
            let price = 20_000.0 + if side == Side::Buy { 50.0 } else { 0.0 };
            tick(sym, &hms_of(t), price, 1000, side)
        })
        .collect()
}

fn children(
    sym: &str,
    start: &str,
    n: usize,
    every: i64,
    value: f64,
    side: Side,
) -> Vec<MarketEvent> {
    let t0 = ts(start, 0);
    let mut price = 20_000.0;
    (0..n)
        .map(|i| {
            if i > 0 && i % 3 == 0 {
                price += if side == Side::Buy { 50.0 } else { -50.0 };
            }
            let t = t0 + chrono::Duration::seconds(i as i64 * every);
            let vol = ((value / price) as i64) / 100 * 100;
            tick(sym, &hms_of(t), price, vol, side)
        })
        .collect()
}

fn sorted(mut v: Vec<MarketEvent>) -> Vec<MarketEvent> {
    v.sort_by_key(|e| e.ts_exchange()); // stable, like Python's sorted
    v
}

pub fn single_block() -> Vec<MarketEvent> {
    reset_seq();
    let block = tick("AAA", "10:01:00", 20_050.0, 100_000, Side::Buy); // 2.005B single print
    let mut v = noise("AAA", "10:00:00", 20, 7);
    v.push(block);
    sorted(v)
}

pub fn split_parent() -> Vec<MarketEvent> {
    reset_seq();
    let mut v = noise("AAA", "10:00:00", 30, 7);
    v.extend(children("AAA", "10:00:10", 12, 5, 150e6, Side::Buy));
    sorted(v)
}

pub fn reversal() -> Vec<MarketEvent> {
    reset_seq();
    let mut v = children("AAA", "10:00:00", 6, 5, 150e6, Side::Buy);
    v.extend(children("AAA", "10:00:32", 6, 5, 150e6, Side::Sell));
    sorted(v)
}

pub fn lunch_break() -> Vec<MarketEvent> {
    reset_seq();
    let mut v = children("AAA", "11:29:20", 6, 5, 150e6, Side::Buy);
    v.extend(children("AAA", "13:00:05", 6, 5, 150e6, Side::Buy));
    sorted(v)
}

pub fn auctions() -> Vec<MarketEvent> {
    reset_seq();
    let o = |session| T {
        ms: 999,
        session: Some(session),
        negotiated: false,
        provider_side: false,
    };
    vec![
        tick_opt(
            "AAA",
            "09:14:59",
            20_000.0,
            150_000,
            Side::Unknown,
            o(Session::Ato),
        ), // 3B ATO
        tick_opt(
            "BBB",
            "09:14:59",
            20_000.0,
            5_000,
            Side::Unknown,
            o(Session::Ato),
        ), // 100M
        tick_opt(
            "AAA",
            "14:44:59",
            20_100.0,
            200_000,
            Side::Unknown,
            o(Session::Atc),
        ), // 4.02B ATC
    ]
}

pub fn putthrough_interleaved() -> Vec<MarketEvent> {
    reset_seq();
    let mut v = children("AAA", "10:00:00", 8, 5, 150e6, Side::Buy);
    v.push(tick_opt(
        "AAA",
        "10:00:12",
        19_500.0,
        1_000_000,
        Side::Unknown,
        T {
            negotiated: true,
            provider_side: false,
            ..T::default()
        },
    ));
    sorted(v)
}

pub fn duplicates_and_late() -> Vec<MarketEvent> {
    reset_seq();
    let kids = children("AAA", "10:00:00", 8, 5, 150e6, Side::Buy);
    let dup = kids[3].clone(); // exact duplicate (same seq) -> dropped by ingest
    let late = tick("AAA", "10:00:21", 20_050.0, 7_400, Side::Buy); // arrives after 10:00:25
    let mut v: Vec<MarketEvent> = kids[..6].to_vec();
    v.push(dup);
    v.push(late);
    v.extend_from_slice(&kids[6..]);
    v
}

pub fn inferred_side() -> Vec<MarketEvent> {
    reset_seq();
    let mut price = 20_000.0;
    (0..8)
        .map(|i| {
            price += 50.0;
            let o = T {
                provider_side: false,
                ..T::default()
            };
            tick_opt(
                "BBB",
                &format!("10:05:{:02}", i * 5),
                price,
                7_500,
                Side::Unknown,
                o,
            )
        })
        .collect()
}

pub type Scenario = (&'static str, fn() -> Vec<MarketEvent>);

/// Large sells (600M) once a minute for 10 minutes while the price holds: absorbed.
/// With `falling`, each pair of sells pushes the price down a tick: not absorbed.
pub fn absorption_flow(falling: bool) -> Vec<MarketEvent> {
    reset_seq();
    let t0 = ts("10:00:00", 0);
    // the whole market (noise included) steps down a tick every 2 minutes when falling
    let level = |sec: i64| {
        if falling {
            20_000.0 - 50.0 * ((sec - 60).max(0) / 120) as f64
        } else {
            20_000.0
        }
    };
    let mut v: Vec<MarketEvent> = (0..150)
        .map(|i| {
            let sec = i * 7;
            let side = if i % 2 == 1 { Side::Buy } else { Side::Sell };
            let px = level(sec) + if side == Side::Buy { 50.0 } else { 0.0 };
            let t = t0 + chrono::Duration::seconds(sec);
            tick("AAA", &hms_of(t), px, 1000, side)
        })
        .collect();
    for i in 0..10 {
        let sec = 60 + i * 60;
        let t = t0 + chrono::Duration::seconds(sec);
        v.push(tick("AAA", &hms_of(t), level(sec), 30_000, Side::Sell));
    }
    sorted(v)
}

/// Medium buys (~100M, below the 200M large bar) every 90 s for 24 minutes — with
/// identical sizes (TWAP-like) or with irregular sizes (ordinary flow).
pub fn stealth_buys(regular: bool) -> Vec<MarketEvent> {
    reset_seq();
    let mut v = noise("AAA", "10:00:00", 220, 7);
    let t0 = ts("10:00:30", 0);
    let sizes = [2_500, 9_000, 4_100, 15_000, 3_000, 7_700, 12_500, 5_300];
    for i in 0..16 {
        let vol = if regular {
            5_000
        } else {
            sizes[i % sizes.len()]
        };
        let t = t0 + chrono::Duration::seconds(i as i64 * 90);
        v.push(tick("AAA", &hms_of(t), 20_050.0, vol, Side::Buy));
    }
    sorted(v)
}

pub fn scenarios() -> Vec<Scenario> {
    vec![
        ("single_block", single_block),
        ("split_parent", split_parent),
        ("reversal", reversal),
        ("lunch_break", lunch_break),
        ("auctions", auctions),
        ("putthrough_interleaved", putthrough_interleaved),
        ("duplicates_and_late", duplicates_and_late),
        ("inferred_side", inferred_side),
    ]
}
