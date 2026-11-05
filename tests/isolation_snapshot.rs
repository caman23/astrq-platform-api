//! Fault isolation between tools, snapshot/restore mid-session, synthetic determinism.

mod common;

use anyhow::{bail, Result};
use astrq::core::clock::{Clock, SimClock};
use astrq::core::config::{Settings, SyntheticConfig, ToolEntry};
use astrq::core::context::ContextService;
use astrq::core::market_rules::MarketRules;
use astrq::core::models::{Instrument, MarketEvent, Signal};
use astrq::core::offline::run_offline;
use astrq::core::sessions::SessionCalendar;
use astrq::core::tool::{Subscription, Tool, ToolContext, ToolManifest};
use astrq::core::universe::Universe;
use astrq::providers::synthetic::{SyntheticContextProvider, SyntheticProvider};
use astrq::providers::MarketDataProvider;
use astrq::tools::large_order_radar::config::RadarConfig;
use astrq::tools::large_order_radar::LargeOrderRadar;
use astrq::tools::tick_counter::{TickCounter, TickCounterConfig};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use common::*;
use serde_json::{json, Value};
use std::sync::Arc;

struct Faulty {
    id: &'static str,
    panic: bool,
}

impl Tool for Faulty {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> &'static str {
        "0"
    }
    fn manifest(&self) -> ToolManifest {
        ToolManifest {
            id: self.id.into(),
            version: "0".into(),
            name: self.id.into(),
            description: String::new(),
            subscriptions: vec![],
            config_schema: json!({}),
            signal_kinds: vec![],
            has_view: false,
        }
    }
    fn config_json(&self) -> Value {
        json!({})
    }
    fn subscriptions(&self) -> Vec<Subscription> {
        vec![Subscription::new(&["tick"])]
    }
    fn on_event(&mut self, _ev: &MarketEvent, _ctx: &ToolContext) -> Result<Vec<Signal>> {
        if self.panic {
            panic!("boom");
        }
        bail!("boom")
    }
    fn on_timer(&mut self, _now: DateTime<Utc>, _ctx: &ToolContext) -> Result<Vec<Signal>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn faulty_and_panicking_tools_do_not_affect_others() {
    let events = split_parent();
    let n_ticks = events.iter().filter(|e| e.event_type() == "tick").count();
    let strict = ToolEntry {
        max_consecutive_failures: 3,
        breaker_cooldown_sec: 60.0,
        ..ToolEntry::default()
    };
    let tools: Vec<(Box<dyn Tool>, ToolEntry)> = vec![
        (
            Box::new(TickCounter::new(TickCounterConfig { milestone: 1 })),
            ToolEntry::default(),
        ),
        (
            Box::new(Faulty {
                id: "erroring",
                panic: false,
            }),
            strict.clone(),
        ),
        (
            Box::new(Faulty {
                id: "panicking",
                panic: true,
            }),
            strict,
        ),
    ];
    let res = run_offline(
        events,
        &Settings::default(),
        universe(),
        fixed_context(),
        tools,
        true,
        true,
    )
    .await
    .unwrap();
    let counted = res
        .signals
        .iter()
        .filter(|s| s.tool_id == "tick_counter")
        .count();
    assert_eq!(counted, n_ticks, "healthy tool saw every tick");
    for id in ["erroring", "panicking"] {
        let st = res.runners[id].status_snapshot();
        assert_eq!(st.state, "breaker_open", "{id}");
        assert_eq!(st.errors, 3, "{id}: breaker stops calls after 3 in a row");
        assert!(st.skipped_breaker > 0, "{id}");
    }
    assert!(res.runners["panicking"]
        .status_snapshot()
        .last_error
        .unwrap()
        .contains("panic: boom"));
}

#[tokio::test]
async fn restart_mid_session_keeps_open_groups() {
    let events = split_parent();
    let full = project(&run(events.clone()).await);
    let cut = events.len() / 2;

    let first = run_with(events[..cut].to_vec(), RadarConfig::default(), false).await;
    let blob = first.runners["large_order_radar"]
        .tool
        .lock()
        .snapshot()
        .unwrap();
    let mut restored = LargeOrderRadar::new(RadarConfig::default());
    restored.restore(&blob).unwrap();
    assert!(
        !restored.book.active.is_empty(),
        "a group must be open at the cut point"
    );
    assert_eq!(restored.snapshot().unwrap(), blob);

    let rest = run_offline(
        events[cut..].to_vec(),
        &Settings::default(),
        universe(),
        fixed_context(),
        vec![(Box::new(restored), radar_entry())],
        true,
        true,
    )
    .await
    .unwrap();
    let mut combined = project(&first);
    combined.extend(project(&rest));
    assert_eq!(combined, full);
}

async fn synthetic_events(seed: u64, minutes: i64) -> Vec<MarketEvent> {
    let s = Settings::default();
    let cfg = SyntheticConfig {
        seed,
        speed: 0.0,
        trading_date: NaiveDate::from_ymd_opt(2026, 10, 21),
        start_time: astrq::core::config::HmTime(NaiveTime::from_hms_opt(9, 14, 0).unwrap()),
        parent_orders_per_hour: 400.0,
        block_trades_per_hour: 100.0,
        ..SyntheticConfig::default()
    };
    let cal = Arc::new(SessionCalendar::new(&s.sessions));
    let rules = Arc::new(MarketRules::new(&s.market_rules));
    let start = SyntheticProvider::start_time(&cfg, &cal);
    let clock: Arc<dyn Clock> = Arc::new(SimClock::new(start, 0.0));
    let syms: Vec<String> = ["VCB", "FPT", "HPG", "SSI", "MWG", "VHM", "GAS", "TCB"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut p = SyntheticProvider::new(cfg, &syms, cal, rules, clock, false);
    p.connect().await.unwrap();
    let end = start + chrono::Duration::minutes(minutes);
    let mut out = vec![];
    while let Some(ev) = p.next_event().await.unwrap() {
        if ev.ts_exchange() > end {
            break;
        }
        out.push(ev);
    }
    out
}

#[tokio::test]
async fn synthetic_market_and_pipeline_are_deterministic() {
    let a = synthetic_events(7, 40).await;
    let b = synthetic_events(7, 40).await;
    let ser = |v: &[MarketEvent]| {
        v.iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(ser(&a), ser(&b));
    assert_ne!(ser(&a), ser(&synthetic_events(8, 40).await));

    let syms = ["VCB", "FPT", "HPG", "SSI", "MWG", "VHM", "GAS", "TCB"];
    let go = || async {
        let u = Universe::from_instruments(
            "t",
            syms.iter().map(|s| Instrument::new(s, None)).collect(),
        );
        let ctx = Arc::new(ContextService::new(Arc::new(SyntheticContextProvider {
            seed: 7,
            rules: MarketRules::new(&Settings::default().market_rules),
        })));
        let tools: Vec<(Box<dyn Tool>, ToolEntry)> = vec![(
            Box::new(LargeOrderRadar::new(RadarConfig::default())),
            ToolEntry::default(),
        )];
        run_offline(a.clone(), &Settings::default(), u, ctx, tools, true, true)
            .await
            .unwrap()
            .signals
            .iter()
            .map(|s| s.signal_id.clone())
            .collect::<Vec<_>>()
    };
    let (x, y) = (go().await, go().await);
    assert_eq!(x, y);
    assert!(
        x.len() > 5,
        "the synthetic market must produce radar signals ({})",
        x.len()
    );
}
