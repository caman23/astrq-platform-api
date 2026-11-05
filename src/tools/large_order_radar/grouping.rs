//! Parent-order grouping: same symbol + same side + price band + max gap.
//!
//! Lifecycle: pending -> OPEN (total >= open threshold) -> UPDATE (+step) -> CLOSE (idle
//! timeout, session change, or a trade that cannot join).
//! Invariant: `group.total_value == sum(member values)`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

use crate::core::models::{Session, Side, SideSource};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRec {
    pub ts: DateTime<Utc>,
    pub price: f64,
    pub volume: i64,
    pub value: f64,
    pub side: Side,
    pub side_source: SideSource,
    pub confidence: f64,
    #[serde(default)]
    pub inside_spread: bool,
    #[serde(default)]
    pub triggers: Vec<String>,
    #[serde(default)]
    pub seq: Option<i64>,
}

impl TradeRec {
    /// Shape used in signal features / the dashboard (ISO timestamps with offset).
    pub fn to_json(&self) -> Value {
        json!({
            "ts": self.ts.to_rfc3339(),
            "price": self.price,
            "volume": self.volume,
            "value": self.value,
            "side": self.side.as_str(),
            "side_source": self.side_source.as_str(),
            "confidence": self.confidence,
            "inside_spread": self.inside_spread,
            "triggers": self.triggers,
            "seq": self.seq,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupState {
    Pending,
    Open,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentGroup {
    pub group_id: String,
    pub symbol: String,
    pub side: Side,
    pub session: Session,
    pub open_threshold: f64,
    pub baseline_cold: bool,
    pub first_ts: DateTime<Utc>,
    pub last_ts: DateTime<Utc>,
    pub first_price: f64,
    pub last_price: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub total_value: f64,
    pub total_volume: i64,
    pub n_trades: u64,
    pub conf_value_sum: f64,
    pub max_single_value: f64,
    pub n_inferred: u64,
    pub n_inside_spread: u64,
    pub n_late: u64,
    pub triggers: Vec<String>,
    pub trades: Vec<TradeRec>,
    pub state: GroupState,
    pub last_emitted_value: f64,
    pub update_count: u64,
}

impl ParentGroup {
    pub fn start(
        symbol: &str,
        tr: &TradeRec,
        session: Session,
        open_threshold: f64,
        cold: bool,
    ) -> Self {
        let gid = format!(
            "{symbol}-{}-{}",
            &tr.side.as_str()[..1],
            tr.ts.timestamp_millis()
        );
        Self {
            group_id: gid,
            symbol: symbol.into(),
            side: tr.side,
            session,
            open_threshold,
            baseline_cold: cold,
            first_ts: tr.ts,
            last_ts: tr.ts,
            first_price: tr.price,
            last_price: tr.price,
            min_price: tr.price,
            max_price: tr.price,
            total_value: 0.0,
            total_volume: 0,
            n_trades: 0,
            conf_value_sum: 0.0,
            max_single_value: 0.0,
            n_inferred: 0,
            n_inside_spread: 0,
            n_late: 0,
            triggers: vec![],
            trades: vec![],
            state: GroupState::Pending,
            last_emitted_value: 0.0,
            update_count: 0,
        }
    }

    pub fn vwap(&self) -> f64 {
        if self.total_volume > 0 {
            self.total_value / self.total_volume as f64
        } else {
            0.0
        }
    }
    pub fn duration_sec(&self) -> f64 {
        ((self.last_ts - self.first_ts)
            .num_microseconds()
            .unwrap_or(0) as f64
            / 1e6)
            .max(0.0)
    }
    pub fn side_confidence(&self) -> f64 {
        if self.total_value > 0.0 {
            self.conf_value_sum / self.total_value
        } else {
            0.0
        }
    }

    pub fn accepts(&self, tr: &TradeRec, session: Session, gap_sec: f64, band: f64) -> bool {
        if session != self.session || tr.side != self.side {
            return false;
        }
        let gap = (tr.ts - self.last_ts)
            .num_microseconds()
            .unwrap_or(i64::MAX)
            .unsigned_abs() as f64
            / 1e6;
        gap <= gap_sec && (tr.price - self.last_price).abs() <= band + 1e-9
    }

    pub fn add(&mut self, tr: TradeRec, keep_trades: usize) {
        self.total_value += tr.value;
        self.total_volume += tr.volume;
        self.n_trades += 1;
        self.conf_value_sum += tr.confidence * tr.value;
        self.max_single_value = self.max_single_value.max(tr.value);
        self.min_price = self.min_price.min(tr.price);
        self.max_price = self.max_price.max(tr.price);
        if tr.side_source != SideSource::Provider {
            self.n_inferred += 1;
        }
        if tr.inside_spread {
            self.n_inside_spread += 1;
        }
        for t in &tr.triggers {
            if !self.triggers.contains(t) {
                self.triggers.push(t.clone());
            }
        }
        if tr.ts >= self.last_ts {
            self.last_ts = tr.ts;
            self.last_price = tr.price;
        } else {
            self.n_late += 1;
        }
        if tr.ts < self.first_ts {
            self.first_ts = tr.ts;
            self.first_price = tr.price;
        }
        self.trades.push(tr);
        if self.trades.len() > keep_trades {
            // keep the first few and the most recent ones
            let head = (keep_trades / 4).max(1);
            self.trades.remove(head);
        }
    }
}

pub type GroupEvent = (&'static str, ParentGroup); // ("open"|"update"|"close", snapshot)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupBook {
    pub gap_sec: f64,
    pub band_ticks: u32,
    pub update_step: f64,
    pub keep_trades: usize,
    /// Key: "SYMBOL|side" (BTreeMap keeps snapshot and close order deterministic).
    pub active: BTreeMap<String, ParentGroup>,
}

fn key(symbol: &str, side: Side) -> String {
    format!("{symbol}|{}", side.as_str())
}

impl GroupBook {
    pub fn new(gap_sec: f64, band_ticks: u32, update_step: f64, keep_trades: usize) -> Self {
        Self {
            gap_sec,
            band_ticks,
            update_step,
            keep_trades,
            active: BTreeMap::new(),
        }
    }

    pub fn add(
        &mut self,
        symbol: &str,
        tr: TradeRec,
        session: Session,
        tick_size: f64,
        open_threshold: f64,
        baseline_cold: bool,
    ) -> Vec<GroupEvent> {
        let mut out = Vec::new();
        let k = key(symbol, tr.side);
        let band = self.band_ticks as f64 * tick_size;
        if let Some(g) = self.active.get(&k) {
            if !g.accepts(&tr, session, self.gap_sec, band) {
                out.extend(self.close_key(&k));
            }
        }
        let keep = self.keep_trades;
        let step = self.update_step;
        let g = self.active.entry(k).or_insert_with(|| {
            ParentGroup::start(symbol, &tr, session, open_threshold, baseline_cold)
        });
        g.add(tr, keep);
        if g.state == GroupState::Pending && g.total_value >= g.open_threshold {
            g.state = GroupState::Open;
            g.last_emitted_value = g.total_value;
            out.push(("open", g.clone()));
        } else if g.state == GroupState::Open
            && g.total_value >= g.last_emitted_value * (1.0 + step)
        {
            g.update_count += 1;
            g.last_emitted_value = g.total_value;
            out.push(("update", g.clone()));
        }
        out
    }

    fn close_key(&mut self, k: &str) -> Vec<GroupEvent> {
        match self.active.remove(k) {
            Some(mut g) if g.state == GroupState::Open => {
                g.state = GroupState::Closed;
                vec![("close", g)]
            }
            _ => vec![],
        }
    }

    pub fn close_idle(&mut self, now: DateTime<Utc>, idle_sec: f64) -> Vec<GroupEvent> {
        let keys: Vec<String> = self
            .active
            .iter()
            .filter(|(_, g)| {
                (now - g.last_ts).num_microseconds().unwrap_or(0) as f64 / 1e6 > idle_sec
            })
            .map(|(k, _)| k.clone())
            .collect();
        keys.iter().flat_map(|k| self.close_key(k)).collect()
    }

    pub fn close_all(&mut self) -> Vec<GroupEvent> {
        let keys: Vec<String> = self.active.keys().cloned().collect();
        keys.iter().flat_map(|k| self.close_key(k)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use rand::{Rng, SeedableRng};

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 21, 3, 0, 0).unwrap()
    }

    fn rec(sec: f64, value: f64, side: Side, price: f64) -> TradeRec {
        TradeRec {
            ts: t0() + Duration::milliseconds((sec * 1000.0) as i64),
            price,
            volume: (value / price) as i64,
            value,
            side,
            side_source: SideSource::Provider,
            confidence: 1.0,
            inside_spread: false,
            triggers: vec![],
            seq: None,
        }
    }

    fn book() -> GroupBook {
        GroupBook::new(30.0, 3, 0.5, 40)
    }

    #[test]
    fn lifecycle_open_update_close() {
        let mut b = book();
        let mut actions = vec![];
        for i in 0..10 {
            for (a, _) in b.add(
                "AAA",
                rec(i as f64 * 5.0, 150e6, Side::Buy, 20_000.0),
                Session::ContinuousAm,
                50.0,
                500e6,
                false,
            ) {
                actions.push(a);
            }
        }
        assert_eq!(actions, vec!["open", "update", "update"]);
        let closed = b.close_idle(t0() + Duration::seconds(45 + 91), 90.0);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.total_value, 1.5e9);
        assert_eq!(closed[0].1.n_trades, 10);
    }

    #[test]
    fn gap_band_session_and_sides() {
        let mut b = book();
        b.add(
            "AAA",
            rec(0.0, 600e6, Side::Buy, 20_000.0),
            Session::ContinuousAm,
            50.0,
            500e6,
            false,
        );
        let ev = b.add(
            "AAA",
            rec(31.0, 600e6, Side::Buy, 20_000.0),
            Session::ContinuousAm,
            50.0,
            500e6,
            false,
        );
        assert_eq!(
            ev.iter().map(|e| e.0).collect::<Vec<_>>(),
            vec!["close", "open"]
        );
        let ev = b.add(
            "AAA",
            rec(35.0, 600e6, Side::Buy, 20_200.0),
            Session::ContinuousAm,
            50.0,
            500e6,
            false,
        );
        assert_eq!(
            ev.iter().map(|e| e.0).collect::<Vec<_>>(),
            vec!["close", "open"]
        );
        let ev = b.add(
            "AAA",
            rec(36.0, 600e6, Side::Buy, 20_200.0),
            Session::ContinuousPm,
            50.0,
            500e6,
            false,
        );
        assert_eq!(
            ev.iter().map(|e| e.0).collect::<Vec<_>>(),
            vec!["close", "open"]
        );
        let ev = b.add(
            "AAA",
            rec(37.0, 600e6, Side::Sell, 20_200.0),
            Session::ContinuousPm,
            50.0,
            500e6,
            false,
        );
        assert_eq!(ev.iter().map(|e| e.0).collect::<Vec<_>>(), vec!["open"]);
        assert_eq!(b.active.len(), 2);
    }

    /// Every trade lands in exactly one group, whatever the arrival order.
    #[test]
    fn value_is_conserved_under_reordering() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(7);
        for _ in 0..200 {
            let n = rng.gen_range(1..60);
            let mut t = 0.0;
            let mut trades: Vec<TradeRec> = (0..n)
                .map(|_| {
                    t += rng.gen_range(0.0..40.0);
                    let side = if rng.gen_bool(0.5) {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    rec(t, rng.gen_range(50e6..2e9), side, 20_000.0)
                })
                .collect();
            for i in 0..trades.len().saturating_sub(1) {
                if rng.gen_bool(0.3) {
                    trades.swap(i, i + 1);
                }
            }
            let expected: f64 = trades.iter().map(|t| t.value).sum();
            let mut b = GroupBook::new(30.0, 3, 0.5, 10_000);
            let mut finished: Vec<ParentGroup> = vec![];
            for tr in trades {
                let k = key("AAA", tr.side);
                let before = b.active.get(&k).cloned();
                b.add("AAA", tr, Session::ContinuousAm, 50.0, 500e6, false);
                if let Some(g) = before {
                    // replaced by a new group => the old one finished (closed or discarded)
                    if b.active.get(&k).map(|x| x.group_id.as_str()) != Some(g.group_id.as_str()) {
                        finished.push(g);
                    }
                }
            }
            let groups: Vec<&ParentGroup> = finished.iter().chain(b.active.values()).collect();
            let total: f64 = groups.iter().map(|g| g.total_value).sum();
            assert!(
                (total - expected).abs() < 1e-2 * groups.len().max(1) as f64 + 1e-3,
                "{total} vs {expected}"
            );
            for g in groups {
                let s: f64 = g.trades.iter().map(|t| t.value).sum();
                assert!((g.total_value - s).abs() < 1e-3);
                assert_eq!(g.state != GroupState::Pending, g.total_value >= 500e6);
            }
        }
    }
}
