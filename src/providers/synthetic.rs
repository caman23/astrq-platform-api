//! Deterministic synthetic market (seeded) for development and demos.
//!
//! Produces a full trading day: background trades with a one-tick spread, split parent
//! orders (runs of same-side child trades), single block trades, put-through deals and
//! single-price ATO/ATC matches. Same seed + same universe => identical event stream.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Timelike, Utc};
use rand::distributions::{Distribution, WeightedIndex};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::LogNormal;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::core::clock::Clock;
use crate::core::config::SyntheticConfig;
use crate::core::context::{Baseline, ContextProvider};
use crate::core::market_rules::MarketRules;
use crate::core::models::{ExpectedPrice, MarketEvent, Quote, Session, Side, SideSource, Tick};
use crate::core::sessions::SessionCalendar;
use crate::providers::{new_status, MarketDataProvider, StatusHandle};

/// tier -> (weight, trades per minute, median VND value per trade)
const TIERS: [(&str, f64, f64, f64); 3] = [
    ("high", 30.0, 36.0, 45e6),
    ("mid", 45.0, 14.0, 22e6),
    ("low", 25.0, 5.0, 10e6),
];

fn seeded(label: &str) -> ChaCha8Rng {
    let h = Sha256::digest(label.as_bytes());
    ChaCha8Rng::seed_from_u64(u64::from_le_bytes(h[..8].try_into().expect("8 bytes")))
}

fn tier_weight(tier: &str) -> f64 {
    TIERS.iter().find(|t| t.0 == tier).map_or(1.0, |t| t.1)
}

#[derive(Debug, Clone)]
pub struct SymbolProfile {
    pub symbol: String,
    pub ref_price: f64,
    pub tier: &'static str,
    pub trades_per_min: f64,
    pub median_shares: i64,
}

impl SymbolProfile {
    pub fn median_trade_value(&self) -> f64 {
        self.ref_price * self.median_shares as f64
    }
}

pub fn synthetic_profile(symbol: &str, seed: u64, rules: &MarketRules) -> SymbolProfile {
    let mut rng = seeded(&format!("profile:{seed}:{symbol}"));
    let idx = WeightedIndex::new(TIERS.iter().map(|t| t.1))
        .expect("weights")
        .sample(&mut rng);
    let (tier, _, tpm, median_value) = TIERS[idx];
    let price = rules.round_to_tick(rng.gen_range(9_000f64.ln()..140_000f64.ln()).exp(), "HOSE");
    let shares = median_value * rng.gen_range(0.7..1.4) / price;
    SymbolProfile {
        symbol: symbol.into(),
        ref_price: price,
        tier,
        trades_per_min: tpm * rng.gen_range(0.7..1.3),
        median_shares: ((shares as i64) / 100 * 100).max(100),
    }
}

/// Baselines consistent with the synthetic market (same seed => same numbers).
pub struct SyntheticContextProvider {
    pub seed: u64,
    pub rules: MarketRules,
}

#[async_trait]
impl ContextProvider for SyntheticContextProvider {
    fn name(&self) -> &str {
        "synthetic"
    }
    async fn baseline(&self, symbol: &str, d: NaiveDate) -> Result<Option<Baseline>> {
        let p = synthetic_profile(symbol, self.seed, &self.rules);
        Ok(Some(Baseline {
            symbol: symbol.into(),
            trading_date: d,
            median_trade_value: p.median_trade_value(),
            p95_trade_value: p.median_trade_value() * 7.0,
            avg_daily_value: p.trades_per_min * 270.0 * p.median_trade_value() * 1.6,
            liquidity_tier: p.tier.into(),
            source: "synthetic".into(),
        }))
    }
}

struct SymState {
    profile: SymbolProfile,
    tick: f64,
    bid: f64,
    seq: i64,
    auction_volume: i64,
}

struct Parent {
    symbol: String,
    side: Side,
    remaining_value: f64,
    child_value: f64,
    interval_s: i64,
    next_at: DateTime<Utc>,
    children_done: i64,
    impact_every: i64,
}

pub struct SyntheticProvider {
    cfg: SyntheticConfig,
    calendar: Arc<SessionCalendar>,
    rules: Arc<MarketRules>,
    clock: Arc<dyn Clock>,
    stop_at_close: bool,
    status: StatusHandle,
    rng: ChaCha8Rng,
    symbols: Vec<String>,
    state: BTreeMap<String, SymState>,
    parents: Vec<Parent>,
    ms: i64,
    t: Option<DateTime<Utc>>,
    last_session: Session,
    buffer: VecDeque<MarketEvent>,
    closed: bool,
}

impl SyntheticProvider {
    pub fn new(
        cfg: SyntheticConfig,
        symbols: &[String],
        calendar: Arc<SessionCalendar>,
        rules: Arc<MarketRules>,
        clock: Arc<dyn Clock>,
        stop_at_close: bool,
    ) -> Self {
        let rng = seeded(&format!("market:{}", cfg.seed));
        let mut me = Self {
            status: new_status(&format!("synthetic seed={} speed={}x", cfg.seed, cfg.speed)),
            cfg,
            calendar,
            rules,
            clock,
            stop_at_close,
            rng,
            symbols: vec![],
            state: BTreeMap::new(),
            parents: vec![],
            ms: 0,
            t: None,
            last_session: Session::Closed,
            buffer: VecDeque::new(),
            closed: false,
        };
        me.set_symbols(symbols);
        me
    }

    pub fn start_time(cfg: &SyntheticConfig, calendar: &SessionCalendar) -> DateTime<Utc> {
        let d = cfg
            .trading_date
            .unwrap_or_else(|| Utc::now().with_timezone(&calendar.tz).date_naive());
        calendar.at_local(d, cfg.start_time.0)
    }

    fn set_symbols(&mut self, symbols: &[String]) {
        let mut syms: Vec<String> = symbols.to_vec();
        syms.sort();
        syms.dedup();
        for s in &syms {
            if !self.state.contains_key(s) {
                let p = synthetic_profile(s, self.cfg.seed, &self.rules);
                let tick = self.rules.tick_size(p.ref_price, "HOSE");
                self.state.insert(
                    s.clone(),
                    SymState {
                        tick,
                        bid: p.ref_price,
                        seq: 0,
                        auction_volume: 0,
                        profile: p,
                    },
                );
            }
        }
        self.symbols = syms;
    }

    fn past_close(&self, t: DateTime<Utc>) -> bool {
        self.calendar.local(t).time() >= self.calendar.close_time()
    }

    // ------------------------------------------------------------ generation

    fn ts(&mut self, t: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        // monotonic within the second so per-symbol seq order matches time order
        self.ms = (self.ms + self.rng.gen_range(1..=6)).min(999);
        let ex = t + Duration::milliseconds(self.ms);
        let rx = ex + Duration::milliseconds(self.rng.gen_range(40..=600));
        (ex, rx)
    }

    fn clamp(&self, sym: &str, price: f64) -> f64 {
        let r = self.state[sym].profile.ref_price;
        self.rules
            .round_to_tick(price.clamp(r * 0.931, r * 1.069), "HOSE")
    }

    fn quote(&mut self, sym: &str, t: DateTime<Utc>) -> MarketEvent {
        let (ex, rx) = self.ts(t);
        let (bid, tick) = (self.state[sym].bid, self.state[sym].tick);
        let bid_size = self.rng.gen_range(1..=50) * 100;
        let ask_size = self.rng.gen_range(1..=50) * 100;
        MarketEvent::Quote(Quote {
            provider: "synthetic".into(),
            symbol: sym.into(),
            ts_exchange: ex,
            ts_received: rx,
            best_bid: Some(bid),
            best_ask: Some(bid + tick),
            bid_size: Some(bid_size),
            ask_size: Some(ask_size),
            ingest_mono: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn trade(
        &mut self,
        sym: &str,
        t: DateTime<Utc>,
        session: Session,
        side: Side,
        volume: i64,
        price: Option<f64>,
        negotiated: bool,
    ) -> MarketEvent {
        let (ex, rx) = self.ts(t);
        let st = self.state.get_mut(sym).expect("symbol");
        st.seq += 1;
        let price = price.unwrap_or(if side == Side::Buy {
            st.bid + st.tick
        } else {
            st.bid
        });
        let emit_side = self.cfg.emit_side_flag && !negotiated && !session.is_auction();
        MarketEvent::Tick(Tick {
            provider: "synthetic".into(),
            symbol: sym.into(),
            ts_exchange: ex,
            ts_received: rx,
            price,
            volume,
            value: price * volume as f64,
            side: if emit_side { side } else { Side::Unknown },
            side_source: if emit_side {
                SideSource::Provider
            } else {
                SideSource::Unknown
            },
            session,
            is_negotiated: negotiated,
            seq: Some(st.seq),
            raw_ref: Some(format!("syn:{sym}:{}", st.seq)),
            ingest_mono: None,
        })
    }

    fn small_volume(&mut self, sym: &str) -> i64 {
        let m = self.state[sym].profile.median_shares as f64;
        let shares = m * LogNormal::new(0.0, 0.8)
            .expect("lognormal")
            .sample(&mut self.rng);
        ((shares as i64) / 100 * 100).max(100)
    }

    fn pick_symbol(&mut self) -> String {
        let weights: Vec<f64> = self
            .symbols
            .iter()
            .map(|s| tier_weight(self.state[s].profile.tier))
            .collect();
        let idx = WeightedIndex::new(&weights)
            .expect("weights")
            .sample(&mut self.rng);
        self.symbols[idx].clone()
    }

    fn step(&mut self, t: DateTime<Utc>, session: Session, prev: Session) -> Vec<MarketEvent> {
        self.ms = 0;
        let mut out = Vec::new();
        if prev.is_auction() && session != prev {
            out.extend(self.auction_match(t - Duration::milliseconds(1), prev));
        }
        if session.is_auction() {
            out.extend(self.auction_build(t, session));
            return out;
        }
        if session.is_continuous() {
            out.extend(self.background(t, session));
            out.extend(self.parents_step(t, session));
            if self.rng.gen::<f64>() < self.cfg.block_trades_per_hour / 3600.0 {
                out.push(self.block_trade(t, session));
            }
        }
        if session.is_continuous() || session == Session::PutThrough {
            let mut rate = self.cfg.put_through_per_hour / 3600.0;
            if session == Session::PutThrough {
                rate *= 6.0;
            }
            if self.rng.gen::<f64>() < rate {
                out.push(self.put_through(t, session));
            }
        }
        out
    }

    fn background(&mut self, t: DateTime<Utc>, session: Session) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        for sym in self.symbols.clone() {
            let lam = self.state[&sym].profile.trades_per_min / 60.0;
            if self.rng.gen::<f64>() >= lam {
                continue;
            }
            if self.rng.gen::<f64>() < 0.12 {
                let dir = if self.rng.gen_bool(0.5) { -1.0 } else { 1.0 };
                let st = &self.state[&sym];
                let nb = self.clamp(&sym, st.bid + st.tick * dir);
                self.state.get_mut(&sym).expect("sym").bid = nb;
                if self.cfg.emit_quotes {
                    out.push(self.quote(&sym, t));
                }
            }
            let side = if self.rng.gen::<f64>() < 0.5 {
                Side::Buy
            } else {
                Side::Sell
            };
            let vol = self.small_volume(&sym);
            out.push(self.trade(&sym, t, session, side, vol, None, false));
        }
        out
    }

    fn parents_step(&mut self, t: DateTime<Utc>, session: Session) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        if self.rng.gen::<f64>() < self.cfg.parent_orders_per_hour / 3600.0 {
            let sym = self.pick_symbol();
            let child = (self.state[&sym].profile.median_trade_value()
                * self.rng.gen_range(4.5..12.0))
            .max(100e6);
            let side = if self.rng.gen::<f64>() < 0.55 {
                Side::Buy
            } else {
                Side::Sell
            };
            let n = self.rng.gen_range(6..=30) as f64;
            let interval = self.rng.gen_range(2..=12);
            let impact = self.rng.gen_range(2..=5);
            self.parents.push(Parent {
                symbol: sym,
                side,
                remaining_value: child * n,
                child_value: child,
                interval_s: interval,
                next_at: t,
                children_done: 0,
                impact_every: impact,
            });
        }
        let mut parents = std::mem::take(&mut self.parents);
        for p in parents.iter_mut() {
            if p.next_at > t {
                continue;
            }
            let value = (p.child_value * self.rng.gen_range(0.7..1.3)).min(p.remaining_value);
            let st = &self.state[&p.symbol];
            let price = if p.side == Side::Buy {
                st.bid + st.tick
            } else {
                st.bid
            };
            let vol = (((value / price) as i64) / 100 * 100).max(100);
            out.push(self.trade(&p.symbol.clone(), t, session, p.side, vol, None, false));
            p.remaining_value -= price * vol as f64;
            p.children_done += 1;
            if p.children_done % p.impact_every == 0 {
                let st = &self.state[&p.symbol];
                let step = if p.side == Side::Buy {
                    st.tick
                } else {
                    -st.tick
                };
                let nb = self.clamp(&p.symbol, st.bid + step);
                self.state.get_mut(&p.symbol).expect("sym").bid = nb;
                if self.cfg.emit_quotes {
                    out.push(self.quote(&p.symbol.clone(), t));
                }
            }
            let jitter = self.rng.gen_range(-1..=2);
            p.next_at = t + Duration::seconds((p.interval_s + jitter).max(1));
        }
        parents.retain(|p| p.remaining_value > 0.0);
        self.parents = parents;
        out
    }

    fn block_trade(&mut self, t: DateTime<Utc>, session: Session) -> MarketEvent {
        let sym = self.pick_symbol();
        let side = if self.rng.gen::<f64>() < 0.5 {
            Side::Buy
        } else {
            Side::Sell
        };
        let value = (self.state[&sym].profile.median_trade_value()
            * self.rng.gen_range(12.0..50.0))
        .max(0.6e9);
        let st = &self.state[&sym];
        let price = if side == Side::Buy {
            st.bid + st.tick
        } else {
            st.bid
        };
        let vol = (((value / price) as i64) / 100 * 100).max(100);
        self.trade(&sym, t, session, side, vol, None, false)
    }

    fn put_through(&mut self, t: DateTime<Utc>, session: Session) -> MarketEvent {
        let sym = self.pick_symbol();
        let value = self.rng.gen_range(5e9..60e9);
        let raw = self.state[&sym].bid * self.rng.gen_range(0.97..1.03);
        let price = self.clamp(&sym, raw);
        let vol = (((value / price) as i64) / 100 * 100).max(100);
        let s = if session.is_continuous() {
            session
        } else {
            Session::PutThrough
        };
        self.trade(&sym, t, s, Side::Unknown, vol, Some(price), true)
    }

    fn auction_build(&mut self, t: DateTime<Utc>, session: Session) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        if !self.calendar.local(t).second().is_multiple_of(30) {
            return out;
        }
        for sym in self.symbols.clone() {
            if self.rng.gen::<f64>() < 0.6 {
                let v = self.small_volume(&sym);
                self.state.get_mut(&sym).expect("sym").auction_volume += v;
            }
            if self.rng.gen::<f64>() < 0.5 {
                let (ex, rx) = self.ts(t);
                let k = self.rng.gen_range(-2..=2) as f64;
                let st = &self.state[&sym];
                let price = self.clamp(&sym, st.bid + st.tick * k);
                out.push(MarketEvent::ExpectedPrice(ExpectedPrice {
                    provider: "synthetic".into(),
                    symbol: sym.clone(),
                    ts_exchange: ex,
                    ts_received: rx,
                    session,
                    price,
                    volume: Some(self.state[&sym].auction_volume),
                    ingest_mono: None,
                }));
            }
        }
        out
    }

    fn auction_match(&mut self, t: DateTime<Utc>, session: Session) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        for sym in self.symbols.clone() {
            let mut vol =
                std::mem::take(&mut self.state.get_mut(&sym).expect("sym").auction_volume);
            if self.rng.gen::<f64>() < 0.06 {
                // occasional big auction interest
                vol += ((self.rng.gen_range(1e9..8e9) / self.state[&sym].bid) as i64) / 100 * 100;
            }
            if vol <= 0 {
                continue;
            }
            let k = self.rng.gen_range(-2..=2) as f64;
            let st = &self.state[&sym];
            let price = self.clamp(&sym, st.bid + st.tick * k);
            let rx = t + Duration::milliseconds(self.rng.gen_range(40..=600));
            let st = self.state.get_mut(&sym).expect("sym");
            st.bid = price;
            st.seq += 1;
            out.push(MarketEvent::Tick(Tick {
                provider: "synthetic".into(),
                symbol: sym.clone(),
                ts_exchange: t,
                ts_received: rx,
                price,
                volume: vol,
                value: price * vol as f64,
                side: Side::Unknown,
                side_source: SideSource::Unknown,
                session,
                is_negotiated: false,
                seq: Some(st.seq),
                raw_ref: Some(format!("syn:{sym}:{}", st.seq)),
                ingest_mono: None,
            }));
        }
        out
    }
}

#[async_trait]
impl MarketDataProvider for SyntheticProvider {
    fn name(&self) -> &'static str {
        "synthetic"
    }
    fn status(&self) -> StatusHandle {
        self.status.clone()
    }
    async fn connect(&mut self) -> Result<()> {
        self.status.lock().connected = true;
        Ok(())
    }
    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.set_symbols(symbols);
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        loop {
            if self.closed {
                return Ok(None);
            }
            if let Some(ev) = self.buffer.pop_front() {
                self.status.lock().last_event_at = Some(ev.ts_received());
                return Ok(Some(ev));
            }
            let t = match self.t {
                Some(t) => t,
                None => {
                    let s = Self::start_time(&self.cfg, &self.calendar);
                    self.last_session = self.calendar.session_at(s);
                    s
                }
            };
            self.t = Some(t);
            let speed = self.clock.speed();
            if speed > 0.0 {
                let lag = (t - self.clock.now()).num_microseconds().unwrap_or(0) as f64 / 1e6;
                if lag > 0.0 {
                    tokio::time::sleep(std::time::Duration::from_secs_f64((lag / speed).min(0.25)))
                        .await;
                    continue;
                }
            } else {
                self.clock.advance_to(t);
            }
            let session = self.calendar.session_at(t);
            let prev = self.last_session;
            let mut batch = self.step(t, session, prev);
            self.last_session = session;
            batch.sort_by_key(|e| e.ts_exchange());
            self.buffer.extend(batch);
            self.t = Some(t + Duration::seconds(1));
            if session == Session::Closed && self.past_close(t) {
                self.status.lock().finished = true;
                if self.stop_at_close {
                    self.closed = self.buffer.is_empty();
                    if self.closed {
                        return Ok(None);
                    }
                } else if self.buffer.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            } else if speed <= 0.0 && !self.buffer.is_empty() {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn close(&mut self) {
        self.closed = true;
        self.status.lock().connected = false;
    }
}
