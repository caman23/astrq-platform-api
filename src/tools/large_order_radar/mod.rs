//! Tool #1 — Large Order Radar (Radar Lệnh Lớn).

pub mod config;
pub mod flow;
pub mod grouping;
pub mod outcome;
pub mod scoring;
pub mod side;
pub mod thresholds;
pub mod view;
pub mod window;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

use crate::core::context::Baseline;
use crate::core::fmt::{self, py_round, py_round_int};
use crate::core::models::{Contribution, MarketEvent, Severity, Side, Signal, Tick};
use crate::core::tool::{Subscription, Tool, ToolContext, ToolManifest};
use config::RadarConfig;
use flow::{AbsorptionDetector, AccumulationDetector, FlowEvent};
use grouping::{GroupBook, ParentGroup, TradeRec};
use outcome::OutcomeBook;
use scoring::{score, ScoreInput};
use side::SideClassifier;
use thresholds::Thresholds;
use window::{DayTotals, SymbolWindow};

const NS: Uuid = Uuid::from_u128(0x6d1f3c4e_9a51_4f1e_8d7e_1c2b3a4d5e6f);

fn state_vi(action: &str) -> &'static str {
    match action {
        "open" => "MỞ",
        "update" => "CẬP NHẬT",
        "close" => "ĐÓNG",
        _ => "ĐƠN",
    }
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    trading_date: Option<NaiveDate>,
    book: Option<GroupBook>,
    windows: BTreeMap<String, SymbolWindow>,
    days: BTreeMap<String, DayTotals>,
    classifier: Option<SideClassifier>,
    expected: BTreeMap<String, Value>,
    #[serde(default)]
    outcomes: OutcomeBook,
    #[serde(default)]
    absorption: AbsorptionDetector,
    #[serde(default)]
    accumulation: AccumulationDetector,
}

/// End-of-day archive: what a past session's view needs beyond its signals.
#[derive(Serialize, Deserialize)]
struct Archive {
    trading_date: NaiveDate,
    days: BTreeMap<String, DayTotals>,
    outcomes: OutcomeBook,
}

pub struct LargeOrderRadar {
    pub cfg: RadarConfig,
    classifier: SideClassifier,
    pub book: GroupBook,
    windows: BTreeMap<String, SymbolWindow>,
    pub days: BTreeMap<String, DayTotals>,
    expected: BTreeMap<String, Value>,
    trading_date: Option<NaiveDate>,
    /// Follow-through of every order (price 5/15/30 minutes after its first signal).
    pub outcomes: OutcomeBook,
    absorption: AbsorptionDetector,
    accumulation: AccumulationDetector,
}

struct SignalSpec<'a> {
    symbol: &'a str,
    ts: DateTime<Utc>,
    kind: String,
    state: &'a str,
    side: Side,
    correlation_id: String,
    dedupe_suffix: String,
    score: f64,
    severity: Severity,
    explanation: Vec<Contribution>,
    flags: Vec<String>,
    features: Map<String, Value>,
}

impl LargeOrderRadar {
    pub const ID: &'static str = "large_order_radar";

    pub fn new(cfg: RadarConfig) -> Self {
        Self {
            classifier: SideClassifier::new(cfg.quote_stale_sec),
            book: GroupBook::new(
                cfg.group_gap_sec,
                cfg.price_band_ticks,
                cfg.update_step_ratio,
                cfg.max_trades_in_features,
            ),
            windows: BTreeMap::new(),
            days: BTreeMap::new(),
            expected: BTreeMap::new(),
            trading_date: None,
            outcomes: OutcomeBook::default(),
            absorption: AbsorptionDetector::default(),
            accumulation: AccumulationDetector::default(),
            cfg,
        }
    }

    pub fn from_config(v: &Value) -> Result<Self> {
        let cfg: RadarConfig = serde_json::from_value(v.clone())?;
        Ok(Self::new(cfg))
    }

    // get_mut first: `entry(symbol.to_string())` would allocate a key on every tick
    fn window(&mut self, symbol: &str) -> &mut SymbolWindow {
        if !self.windows.contains_key(symbol) {
            let w = SymbolWindow::new(self.cfg.metrics_window_sec);
            self.windows.insert(symbol.to_string(), w);
        }
        self.windows.get_mut(symbol).expect("just inserted")
    }

    fn day(&mut self, symbol: &str) -> &mut DayTotals {
        if !self.days.contains_key(symbol) {
            self.days.insert(symbol.to_string(), DayTotals::default());
        }
        self.days.get_mut(symbol).expect("just inserted")
    }

    fn roll_day(&mut self, ts: DateTime<Utc>, ctx: &ToolContext) {
        let d = ctx.calendar.trading_date(ts);
        if self.trading_date != Some(d) {
            if self.trading_date.is_some() {
                self.book.active.clear();
                self.windows.clear();
                self.days.clear();
                self.expected.clear();
                self.outcomes.clear();
                self.absorption.clear();
                self.accumulation.clear();
            }
            self.trading_date = Some(d);
        }
    }

    fn tick_size(&self, symbol: &str, price: f64, ctx: &ToolContext) -> f64 {
        let u = ctx.universe.read();
        let exchange = u.get(symbol).map_or("HOSE", |i| i.exchange.as_str());
        ctx.rules.tick_size(price, exchange)
    }

    fn baseline(&self, symbol: &str, ctx: &ToolContext) -> Option<Baseline> {
        self.trading_date
            .and_then(|d| ctx.context.baseline(symbol, d))
    }

    fn symbol_features(&mut self, symbol: &str) -> Value {
        let mut m = self.window(symbol).snapshot();
        let day = self.day(symbol).clone();
        let o = m.as_object_mut().expect("object");
        o.insert("day_large_buy".into(), json!(py_round_int(day.large_buy)));
        o.insert("day_large_sell".into(), json!(py_round_int(day.large_sell)));
        o.insert("day_net_large".into(), json!(py_round_int(day.net_large())));
        o.insert(
            "day_putthrough_value".into(),
            json!(py_round_int(day.putthrough_value)),
        );
        o.insert("day_putthrough_count".into(), json!(day.putthrough_count));
        o.insert("day_groups_opened".into(), json!(day.groups_opened));
        m
    }

    fn baseline_features(b: Option<&Baseline>) -> Value {
        match b {
            None => Value::Null,
            Some(b) => json!({
                "median_trade_value": py_round_int(b.median_trade_value),
                "p95_trade_value": py_round_int(b.p95_trade_value),
                "avg_daily_value": py_round_int(b.avg_daily_value),
                "liquidity_tier": b.liquidity_tier,
                "source": b.source,
            }),
        }
    }

    fn make_signal(&mut self, ctx: &ToolContext, spec: SignalSpec) -> Signal {
        let dedupe_key = format!(
            "{}:{}:{}:{}:{}",
            Self::ID,
            spec.symbol,
            spec.correlation_id,
            spec.state,
            spec.dedupe_suffix
        );
        let sector = ctx
            .universe
            .read()
            .get(spec.symbol)
            .and_then(|i| i.sector.clone());
        let mut features = spec.features;
        features.insert("sector".into(), json!(sector));
        features.insert("symbol_metrics".into(), self.symbol_features(spec.symbol));
        let day = self.day(spec.symbol);
        day.max_score = day.max_score.max(spec.score);
        Signal {
            signal_id: Uuid::new_v5(&NS, dedupe_key.as_bytes()).to_string(),
            tool_id: Self::ID.into(),
            tool_version: self.version().into(),
            symbol: spec.symbol.into(),
            ts: spec.ts,
            kind: spec.kind,
            side: spec.side,
            score: spec.score,
            severity: spec.severity,
            state: spec.state.into(),
            features,
            explanation: spec.explanation,
            quality_flags: spec.flags,
            dedupe_key,
            correlation_id: spec.correlation_id,
        }
    }

    fn group_signal(
        &mut self,
        action: &str,
        g: &ParentGroup,
        ctx: &ToolContext,
        ts: DateTime<Utc>,
    ) -> Signal {
        let baseline = self.baseline(&g.symbol, ctx);
        let concentration = self.window(&g.symbol).side_concentration(g.side);
        let res = score(
            &ScoreInput {
                total_value: g.total_value,
                duration_sec: g.duration_sec(),
                side: g.side,
                first_price: g.first_price,
                last_price: g.last_price,
                side_confidence: g.side_confidence(),
                concentration,
                baseline: baseline.as_ref(),
            },
            &self.cfg,
            None,
        );
        let mut flags = Vec::new();
        if g.baseline_cold || baseline.is_none() {
            flags.push("baseline_cold".to_string());
        }
        if g.side == Side::Unknown {
            flags.push("side_unknown".into());
        } else if g.n_inferred > 0 {
            flags.push("side_inferred".into());
        }
        if g.n_inside_spread > 0 {
            flags.push("inside_spread".into());
        }
        if g.n_late > 0 {
            flags.push("late_ticks".into());
        }
        if action == "open" {
            self.day(&g.symbol).groups_opened += 1;
        }
        let ratio_note = match baseline.as_ref().filter(|b| b.median_trade_value > 0.0) {
            Some(b) => format!(
                " · ×{:.0} trung vị lệnh",
                g.total_value / b.median_trade_value
            ),
            None => String::new(),
        };
        let mv = if g.first_price != 0.0 {
            (g.last_price - g.first_price) / g.first_price * 100.0
        } else {
            0.0
        };
        let summary = vec![
            format!(
                "Nhóm lệnh {} · {} lệnh · {}{ratio_note}",
                state_vi(action),
                g.n_trades,
                fmt::vnd(g.total_value)
            ),
            format!(
                "Giá {} → {} ({mv:+.2}%) · {}–{}",
                fmt::price(g.first_price),
                fmt::price(g.last_price),
                fmt::hms(g.first_ts),
                fmt::hms(g.last_ts)
            ),
        ];
        let mut features = Map::new();
        features.insert(
            "group".into(),
            json!({
                "group_id": g.group_id,
                "action": action,
                "side": g.side.as_str(),
                "session": g.session.as_str(),
                "total_value": py_round_int(g.total_value),
                "total_volume": g.total_volume,
                "n_trades": g.n_trades,
                "vwap": py_round(g.vwap(), 2),
                "first_price": g.first_price,
                "last_price": g.last_price,
                "min_price": g.min_price,
                "max_price": g.max_price,
                "price_change_pct": py_round(mv, 3),
                "started_at": g.first_ts.to_rfc3339(),
                "last_at": g.last_ts.to_rfc3339(),
                "duration_sec": py_round(g.duration_sec(), 1),
                "max_single_value": py_round_int(g.max_single_value),
                "open_threshold": py_round_int(g.open_threshold),
                "triggers": g.triggers,
                "side_confidence": py_round(g.side_confidence(), 3),
                "trades": g.trades.iter().map(TradeRec::to_json).collect::<Vec<_>>(),
            }),
        );
        features.insert(
            "baseline".into(),
            Self::baseline_features(baseline.as_ref()),
        );
        features.insert("summary_vi".into(), json!(summary));
        self.make_signal(
            ctx,
            SignalSpec {
                symbol: &g.symbol,
                ts,
                kind: format!("block_order_{action}"),
                state: action,
                side: g.side,
                correlation_id: g.group_id.clone(),
                dedupe_suffix: g.update_count.to_string(),
                score: res.score,
                severity: res.severity,
                explanation: res.explanation,
                flags,
                features,
            },
        )
    }

    fn single_signal(
        &mut self,
        t: &Tick,
        ctx: &ToolContext,
        kind: &str,
        mut flags: Vec<String>,
    ) -> Signal {
        let baseline = self.baseline(&t.symbol, ctx);
        let res = score(
            &ScoreInput {
                total_value: t.value,
                duration_sec: 0.0,
                side: Side::Unknown,
                first_price: t.price,
                last_price: t.price,
                side_confidence: 0.0,
                concentration: 0.0,
                baseline: baseline.as_ref(),
            },
            &self.cfg,
            Some(&["value", "relative"]),
        );
        if baseline.is_none() {
            flags.push("baseline_cold".into());
        }
        let mut severity = res.severity;
        let cap = self.cfg.putthrough_max_severity;
        if kind == "block_putthrough" && severity.rank() > cap.rank() {
            severity = cap;
        }
        let cid = format!(
            "{}-{kind}-{}-{}",
            t.symbol,
            t.ts_exchange.timestamp_millis(),
            t.seq.unwrap_or(0)
        );
        let label = if kind == "block_putthrough" {
            "Thỏa thuận".to_string()
        } else {
            format!("Khớp {}", t.session.as_str().to_uppercase())
        };
        let triggers = Thresholds { cfg: &self.cfg }.large_triggers(t.value, baseline.as_ref());
        let trade = json!({
            "ts": t.ts_exchange.to_rfc3339(), "price": t.price, "volume": t.volume, "value": t.value,
            "side": "unknown", "side_source": "unknown", "confidence": 0.0, "inside_spread": false,
            "triggers": triggers, "seq": t.seq,
        });
        let mut features = Map::new();
        features.insert(
            "group".into(),
            json!({
                "group_id": cid, "action": "single", "side": "unknown", "session": t.session.as_str(),
                "total_value": py_round_int(t.value), "total_volume": t.volume, "n_trades": 1,
                "vwap": t.price, "first_price": t.price, "last_price": t.price,
                "min_price": t.price, "max_price": t.price, "price_change_pct": 0.0,
                "started_at": t.ts_exchange.to_rfc3339(), "last_at": t.ts_exchange.to_rfc3339(),
                "duration_sec": 0.0, "max_single_value": py_round_int(t.value),
                "triggers": triggers, "side_confidence": 0.0, "trades": [trade],
            }),
        );
        features.insert(
            "expected_price".into(),
            self.expected.get(&t.symbol).cloned().unwrap_or(Value::Null),
        );
        features.insert(
            "baseline".into(),
            Self::baseline_features(baseline.as_ref()),
        );
        features.insert(
            "summary_vi".into(),
            json!([format!(
                "{label} · {} · giá {} · {}",
                fmt::vnd(t.value),
                fmt::price(t.price),
                fmt::hms(t.ts_exchange)
            )]),
        );
        self.make_signal(
            ctx,
            SignalSpec {
                symbol: &t.symbol,
                ts: t.ts_exchange,
                kind: kind.into(),
                state: "single",
                side: Side::Unknown,
                correlation_id: cid,
                dedupe_suffix: "0".into(),
                score: res.score,
                severity,
                explanation: res.explanation,
                flags,
                features,
            },
        )
    }

    /// Signal for an absorption / accumulation episode (same `group` shape as block
    /// orders, so the dashboard renders it with the same components).
    fn flow_signal(&mut self, ev: FlowEvent, ctx: &ToolContext, t: &Tick) -> Signal {
        let baseline = self.baseline(&t.symbol, ctx);
        let mv = if ev.first_price > 0.0 {
            (ev.last_price - ev.first_price) / ev.first_price * 100.0
        } else {
            0.0
        };
        let duration = (ev.last_ts - ev.first_ts).num_milliseconds() as f64 / 1000.0;
        let mut flags = vec![];
        if baseline.is_none() {
            flags.push("baseline_cold".to_string());
        }
        let mut features = Map::new();
        features.insert(
            "group".into(),
            json!({
                "group_id": ev.correlation_id,
                "action": ev.action,
                "side": ev.side.as_str(),
                "session": t.session.as_str(),
                "total_value": py_round_int(ev.value),
                "total_volume": ev.volume,
                "n_trades": ev.n_trades,
                "vwap": ev.last_price,
                "first_price": ev.first_price,
                "last_price": ev.last_price,
                "min_price": ev.first_price.min(ev.last_price),
                "max_price": ev.first_price.max(ev.last_price),
                "price_change_pct": py_round(mv, 3),
                "started_at": ev.first_ts.to_rfc3339(),
                "last_at": ev.last_ts.to_rfc3339(),
                "duration_sec": py_round(duration, 1),
                "max_single_value": 0,
                "open_threshold": py_round_int(ev.threshold),
                "triggers": [ev.kind],
                "side_confidence": 1.0,
                "flow_share": py_round(ev.share, 3),
                "trades": [],
            }),
        );
        features.insert(
            "baseline".into(),
            Self::baseline_features(baseline.as_ref()),
        );
        features.insert("summary_vi".into(), json!(ev.summary));
        self.make_signal(
            ctx,
            SignalSpec {
                symbol: &t.symbol,
                ts: t.ts_exchange,
                kind: ev.kind.into(),
                state: ev.action,
                side: ev.side,
                correlation_id: ev.correlation_id,
                dedupe_suffix: ev.update.to_string(),
                score: ev.score,
                severity: ev.severity,
                explanation: ev.explanation,
                flags,
                features,
            },
        )
    }

    /// Absorption + accumulation on a continuous-session trade (O(1) amortized).
    #[allow(clippy::too_many_arguments)]
    fn flow_signals(
        &mut self,
        t: &Tick,
        side: Side,
        is_large: bool,
        in_block: bool,
        open_threshold: f64,
        baseline: Option<&Baseline>,
        ctx: &ToolContext,
    ) -> Vec<Signal> {
        let mut out = Vec::new();
        if self.cfg.absorption.enabled && is_large {
            self.absorption
                .on_large(&t.symbol, t.ts_exchange, side, t.value, t.price);
        }
        if self.cfg.absorption.enabled && !self.absorption.idle(&t.symbol) {
            let tick = self.tick_size(&t.symbol, t.price, ctx);
            let w = self
                .windows
                .get(&t.symbol)
                .expect("window exists after add");
            if let Some(ev) = self.absorption.check(
                &t.symbol,
                w,
                open_threshold,
                tick,
                t.ts_exchange,
                &self.cfg.absorption,
                &self.cfg,
            ) {
                out.push(self.flow_signal(ev, ctx, t));
            }
        }
        if self.cfg.accumulation.enabled {
            let acfg = &self.cfg.accumulation;
            // trades inside an open block group are already on the radar
            let medium = !in_block
                && AccumulationDetector::is_medium(
                    t.value,
                    is_large,
                    baseline.map(|b| b.median_trade_value),
                    acfg,
                );
            if let Some(ev) = self.accumulation.on_trade(
                &t.symbol,
                t.ts_exchange,
                side,
                t.volume,
                t.value,
                t.price,
                medium,
                open_threshold,
                acfg,
                &self.cfg,
            ) {
                out.push(self.flow_signal(ev, ctx, t));
            }
        }
        out
    }

    /// Start follow-through tracking for orders seen for the first time.
    fn track_outcomes(&mut self, signals: &[Signal]) {
        for s in signals {
            let price = s
                .features
                .get("group")
                .and_then(|g| g.get("last_price"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            self.outcomes.register(
                &s.correlation_id,
                &s.symbol,
                &s.kind,
                s.side,
                s.severity,
                s.ts,
                price,
            );
        }
    }

    fn on_tick(&mut self, t: &Tick, ctx: &ToolContext) -> Vec<Signal> {
        let baseline = self.baseline(&t.symbol, ctx);
        if t.is_negotiated {
            // Put-through is shown separately and never mixed into matched-order metrics.
            let day = self.day(&t.symbol);
            day.putthrough_value += t.value;
            day.putthrough_count += 1;
            if t.value >= self.cfg.putthrough_min_value {
                return vec![self.single_signal(
                    t,
                    ctx,
                    "block_putthrough",
                    vec!["negotiated".into(), "side_unknown".into()],
                )];
            }
            return vec![];
        }
        self.outcomes.on_trade(&t.symbol, t.ts_exchange, t.price);
        let c = self.classifier.classify(t);
        let th = Thresholds { cfg: &self.cfg };
        let triggers = th.large_triggers(t.value, baseline.as_ref());
        let flagged = !triggers.is_empty() || th.is_child_candidate(t.value, baseline.as_ref());
        let open_threshold = th.group_open_value(baseline.as_ref());
        let auction_value = th.auction_value(baseline.as_ref());
        let ts_epoch = t.ts_exchange.timestamp_micros() as f64 / 1e6;
        self.window(&t.symbol)
            .add(ts_epoch, c.side, t.value, flagged, t.price);
        if flagged {
            let day = self.day(&t.symbol);
            match c.side {
                Side::Buy => day.large_buy += t.value,
                Side::Sell => day.large_sell += t.value,
                Side::Unknown => day.large_unknown += t.value,
            }
        }
        if t.session.is_auction() {
            if t.value >= auction_value {
                self.day(&t.symbol).auction_blocks += 1;
                return vec![self.single_signal(
                    t,
                    ctx,
                    "block_auction",
                    vec!["auction".into(), "side_unknown".into()],
                )];
            }
            return vec![];
        }
        if !t.session.is_continuous() {
            return vec![];
        }
        let is_large = !triggers.is_empty();
        if !flagged {
            return self.flow_signals(
                t,
                c.side,
                is_large,
                false,
                open_threshold,
                baseline.as_ref(),
                ctx,
            );
        }
        let rec = TradeRec {
            ts: t.ts_exchange,
            price: t.price,
            volume: t.volume,
            value: t.value,
            side: c.side,
            side_source: c.source,
            confidence: c.confidence(),
            inside_spread: c.inside_spread,
            triggers,
            seq: t.seq,
        };
        let tick_size = self.tick_size(&t.symbol, t.price, ctx);
        let events = self.book.add(
            &t.symbol,
            rec,
            t.session,
            tick_size,
            open_threshold,
            baseline.is_none(),
        );
        for (a, g) in &events {
            if *a == "open" {
                self.accumulation
                    .forget_since(&g.symbol, g.side, g.first_ts);
            }
        }
        let in_block = self.book.is_open(&t.symbol, c.side);
        let emit_close = self.cfg.emit_close;
        let mut out: Vec<Signal> = events
            .into_iter()
            .filter(|(a, _)| *a != "close" || emit_close)
            .map(|(a, g)| self.group_signal(a, &g, ctx, t.ts_exchange))
            .collect();
        out.extend(self.flow_signals(
            t,
            c.side,
            is_large,
            in_block,
            open_threshold,
            baseline.as_ref(),
            ctx,
        ));
        out
    }
}

impl Tool for LargeOrderRadar {
    fn id(&self) -> &'static str {
        Self::ID
    }
    fn version(&self) -> &'static str {
        "1.1.0"
    }

    fn manifest(&self) -> ToolManifest {
        ToolManifest {
            id: Self::ID.into(),
            version: self.version().into(),
            name: "Radar Lệnh Lớn".into(),
            description:
                "Phát hiện giao dịch lớn và chuỗi lệnh cùng chiều bị chia nhỏ trên rổ VN100.".into(),
            subscriptions: self
                .subscriptions()
                .iter()
                .map(Subscription::describe)
                .collect(),
            config_schema: serde_json::to_value(schemars::schema_for!(RadarConfig))
                .unwrap_or_default(),
            signal_kinds: [
                "block_order_open",
                "block_order_update",
                "block_order_close",
                "block_auction",
                "block_putthrough",
                "absorption",
                "accumulation",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            has_view: true,
        }
    }

    fn config_json(&self) -> Value {
        serde_json::to_value(&self.cfg).unwrap_or_default()
    }

    fn subscriptions(&self) -> Vec<Subscription> {
        vec![Subscription::new(&["tick", "quote", "expected_price"])]
    }

    fn on_event(&mut self, ev: &MarketEvent, ctx: &ToolContext) -> Result<Vec<Signal>> {
        self.roll_day(ev.ts_exchange(), ctx);
        Ok(match ev {
            MarketEvent::Quote(q) => {
                self.classifier.on_quote(q);
                vec![]
            }
            MarketEvent::ExpectedPrice(e) => {
                self.expected.insert(
                    e.symbol.clone(),
                    json!({"session": e.session.as_str(), "price": e.price, "volume": e.volume, "ts": e.ts_exchange.to_rfc3339()}),
                );
                vec![]
            }
            MarketEvent::Tick(t) => {
                let out = self.on_tick(t, ctx);
                self.track_outcomes(&out);
                out
            }
            MarketEvent::ForeignFlow(_) => vec![],
        })
    }

    fn on_timer(&mut self, now: DateTime<Utc>, ctx: &ToolContext) -> Result<Vec<Signal>> {
        let session = ctx.calendar.session_at(now);
        let events = if session.is_continuous() {
            self.book.close_idle(now, self.cfg.idle_close_sec)
        } else {
            self.book.close_all() // lunch break / auction / close ends every group
        };
        // build CLOSE signals before evicting, so their metrics describe the group's period
        let out = if self.cfg.emit_close {
            events
                .into_iter()
                .map(|(a, g)| self.group_signal(a, &g, ctx, now))
                .collect()
        } else {
            vec![]
        };
        let t = now.timestamp_micros() as f64 / 1e6;
        for w in self.windows.values_mut() {
            w.evict(t);
        }
        self.outcomes.on_timer(now);
        if self.cfg.accumulation.enabled {
            self.accumulation.on_timer(now, &self.cfg.accumulation);
        }
        if self.cfg.absorption.enabled {
            // end absorption episodes whose window no longer qualifies
            for sym in self.absorption.active_symbols() {
                if let Some(w) = self.windows.get(&sym) {
                    let open = Thresholds { cfg: &self.cfg }
                        .group_open_value(self.baseline(&sym, ctx).as_ref());
                    let tick = w
                        .entries
                        .back()
                        .map_or(0.0, |e| self.tick_size(&sym, e.4, ctx));
                    let _ = self.absorption.check(
                        &sym,
                        w,
                        open,
                        tick,
                        now,
                        &self.cfg.absorption,
                        &self.cfg,
                    );
                }
            }
        }
        Ok(out)
    }

    fn snapshot(&self) -> Option<Vec<u8>> {
        let st = State {
            trading_date: self.trading_date,
            book: Some(self.book.clone()),
            windows: self.windows.clone(),
            days: self.days.clone(),
            classifier: Some(self.classifier.clone()),
            expected: self.expected.clone(),
            outcomes: self.outcomes.clone(),
            absorption: self.absorption.clone(),
            accumulation: self.accumulation.clone(),
        };
        serde_json::to_vec(&st).ok()
    }

    fn restore(&mut self, data: &[u8]) -> Result<()> {
        let st: State = serde_json::from_slice(data)?;
        self.trading_date = st.trading_date;
        if let Some(b) = st.book {
            self.book = b;
        }
        self.windows = st.windows;
        self.days = st.days;
        if let Some(c) = st.classifier {
            self.classifier = c;
        }
        self.expected = st.expected;
        self.outcomes = st.outcomes;
        self.absorption = st.absorption;
        self.accumulation = st.accumulation;
        Ok(())
    }

    fn view(
        &self,
        signals: &[Arc<Signal>],
        ctx: &ToolContext,
        params: &HashMap<String, String>,
    ) -> Option<Value> {
        // the date the data belongs to: after midnight / on a weekend the radar still
        // holds the last session until the next one starts
        let date = self
            .trading_date
            .unwrap_or_else(|| ctx.calendar.trading_date(ctx.clock.now()));
        Some(view::build_view(
            signals,
            ctx,
            params,
            date,
            &self.days,
            &self.outcomes,
        ))
    }

    fn state_date(&self) -> Option<NaiveDate> {
        self.trading_date
    }

    fn archive(&self) -> Option<(NaiveDate, Vec<u8>)> {
        let d = self.trading_date?;
        let a = Archive {
            trading_date: d,
            days: self.days.clone(),
            outcomes: self.outcomes.clone(),
        };
        Some((d, serde_json::to_vec(&a).ok()?))
    }

    fn archive_view(
        &self,
        archive: Option<&[u8]>,
        signals: &[Arc<Signal>],
        date: NaiveDate,
        ctx: &ToolContext,
        params: &HashMap<String, String>,
    ) -> Option<Value> {
        // Without an archive (days recorded before archives existed) the view still works
        // from the signals' own snapshots; only follow-through and accuracy are missing.
        let a: Archive = archive
            .and_then(|b| serde_json::from_slice(b).ok())
            .filter(|a: &Archive| a.trading_date == date)
            .unwrap_or_else(|| Archive {
                trading_date: date,
                days: BTreeMap::new(),
                outcomes: OutcomeBook::default(),
            });
        let mut v = view::build_view(signals, ctx, params, date, &a.days, &a.outcomes);
        if let Some(o) = v.as_object_mut() {
            o.insert("archived".into(), json!(true));
            o.insert("has_outcomes".into(), json!(archive.is_some()));
        }
        Some(v)
    }

    fn daily_summary(&self, _ctx: &ToolContext) -> Option<String> {
        let mut rows: Vec<(&String, &DayTotals)> = self.days.iter().collect();
        rows.sort_by(|a, b| b.1.net_large().total_cmp(&a.1.net_large()));
        let buys: Vec<String> = rows
            .iter()
            .filter(|(_, d)| d.net_large() > 0.0)
            .take(5)
            .map(|(s, d)| format!("{s} {}", fmt::vnd(d.net_large())))
            .collect();
        let sells: Vec<String> = rows
            .iter()
            .rev()
            .filter(|(_, d)| d.net_large() < 0.0)
            .take(5)
            .map(|(s, d)| format!("{s} {}", fmt::vnd(d.net_large())))
            .collect();
        let groups: u64 = self.days.values().map(|d| d.groups_opened).sum();
        let pt: f64 = self.days.values().map(|d| d.putthrough_value).sum();
        let or_dash = |v: Vec<String>| {
            if v.is_empty() {
                "—".to_string()
            } else {
                v.join(", ")
            }
        };
        Some(
            [
                format!(
                    "📊 Tổng kết Radar Lệnh Lớn · {}",
                    self.trading_date.map(|d| d.to_string()).unwrap_or_default()
                ),
                format!("Số nhóm lệnh lớn: {groups} · Thỏa thuận: {}", fmt::vnd(pt)),
                format!("Ròng lớn MUA nhiều nhất: {}", or_dash(buys)),
                format!("Ròng lớn BÁN nhiều nhất: {}", or_dash(sells)),
                fmt::DISCLAIMER_VI.to_string(),
            ]
            .join("\n"),
        )
    }
}
