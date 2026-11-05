//! Deterministic offline pipeline (tests, golden files, backtests).
//!
//! Runs events through the real `Ingest` and `ToolRunner` code paths, driving tool
//! timers from event time on absolute interval boundaries, so identical input always
//! yields identical signals (and a run split by snapshot/restore matches one run).

use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::core::bus::Bus;
use crate::core::clock::{Clock, EventClock};
use crate::core::config::{Settings, ToolEntry};
use crate::core::context::ContextService;
use crate::core::ingest::{Ingest, IngestStats};
use crate::core::market_rules::MarketRules;
use crate::core::models::{MarketEvent, Signal};
use crate::core::runner::ToolRunner;
use crate::core::sessions::SessionCalendar;
use crate::core::tool::{Tool, ToolContext};
use crate::core::universe::Universe;

pub struct OfflineResult {
    pub signals: Vec<Signal>,
    pub ingest_stats: IngestStats,
    pub runners: BTreeMap<String, Arc<ToolRunner>>,
}

pub async fn run_offline(
    events: Vec<MarketEvent>,
    settings: &Settings,
    universe: Universe,
    context: Arc<ContextService>,
    tools: Vec<(Box<dyn Tool>, ToolEntry)>,
    warm: bool,
    flush: bool,
) -> Result<OfflineResult> {
    let calendar = Arc::new(SessionCalendar::new(&settings.sessions));
    let rules = Arc::new(MarketRules::new(&settings.market_rules));
    let clock: Arc<dyn Clock> = Arc::new(EventClock::new(None));
    let universe = Arc::new(RwLock::new(universe));
    let market_bus = Arc::new(Bus::<MarketEvent>::new("market", 1));
    let signal_bus = Arc::new(Bus::<Signal>::new("signals", 1));
    let mut collector = signal_bus.subscribe("collector", |_| true, Some(10_000_000));
    let mut ingest = Ingest::new(market_bus, calendar.clone(), universe.clone(), Some(clock.clone()));

    let mut runners: BTreeMap<String, Arc<ToolRunner>> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    for (tool, entry) in tools {
        let id = tool.id().to_string();
        let ctx = ToolContext {
            tool_id: id.clone(),
            universe: universe.clone(),
            context: context.clone(),
            calendar: calendar.clone(),
            rules: rules.clone(),
            clock: clock.clone(),
        };
        let runner = Arc::new(ToolRunner::new(tool, ctx.clone(), entry, signal_bus.clone(), None));
        runner.tool.lock().on_start(&ctx);
        order.push(id.clone());
        runners.insert(id, runner);
    }

    let symbols = universe.read().symbols();
    let mut next_timer: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
    let mut warmed: HashSet<NaiveDate> = HashSet::new();
    let mut last_ts: Option<DateTime<Utc>> = None;

    for raw in events {
        let Some(ev) = ingest.process(raw) else { continue };
        let ts = ev.ts_exchange();
        let d = calendar.trading_date(ts);
        if warm && warmed.insert(d) {
            context.warm(&symbols, d).await;
        }
        last_ts = Some(last_ts.map_or(ts, |l| l.max(ts)));
        for id in &order {
            let r = &runners[id];
            let interval = r.entry.timer_interval_sec;
            let step = Duration::microseconds((interval * 1e6) as i64);
            let nt = next_timer.entry(id.clone()).or_insert_with(|| {
                let first = ((ts.timestamp_micros() as f64 / 1e6) / interval).floor() * interval + interval;
                Utc.timestamp_micros((first * 1e6) as i64).single().expect("valid ts")
            });
            while ts >= *nt {
                r.process_timer(*nt);
                *nt += step;
            }
            if r.wants(&ev) {
                r.process_event(&ev);
            }
        }
    }

    if flush {
        if let Some(last) = last_ts {
            // Keep ticking timers until the session ends (as the live clock would), so
            // idle groups close at their real idle time; then a final tick at close.
            let d = calendar.trading_date(last);
            let close_at = calendar.at_local(d, calendar.close_time());
            let end = close_at.max(last + Duration::seconds(1));
            for id in &order {
                let r = &runners[id];
                let step = Duration::microseconds((r.entry.timer_interval_sec * 1e6) as i64);
                let mut nt = next_timer.get(id).copied().unwrap_or(end);
                while nt < end {
                    r.process_timer(nt);
                    nt += step;
                }
                r.process_timer(end);
            }
        }
    }

    let mut signals = Vec::new();
    while let Ok(s) = collector.rx.try_recv() {
        signals.push(s);
    }
    Ok(OfflineResult { signals, ingest_stats: ingest.stats.clone(), runners })
}
