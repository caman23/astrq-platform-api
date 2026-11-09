//! Flow detectors that look at a symbol's recent trades as a whole, not at one order:
//!
//! - **Absorption** (hấp thụ): large aggressive flow is one-sided, yet the price does not
//!   follow — someone is absorbing it with passive orders. Large sells absorbed → the
//!   absorber is a buyer (signal side = buy), and vice versa.
//! - **Accumulation / distribution** (gom/xả âm thầm): many *medium* trades (below the
//!   large-trade bar, so invisible to block detection), regular in size, on one side,
//!   adding up to a block-sized amount over tens of minutes — typical of TWAP/iceberg
//!   execution.
//!
//! Both are episode-based (OPEN, then UPDATE when the amount grows by a step, then a
//! cooldown) and cost O(1) amortized per trade.

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap as HashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use crate::core::fmt::py_round;
use crate::core::models::{Contribution, Severity, Side};
use crate::tools::large_order_radar::config::{AbsorptionConfig, AccumulationConfig, RadarConfig};
use crate::tools::large_order_radar::window::SymbolWindow;

/// One detection, turned into a signal by the radar.
#[derive(Debug, Clone)]
pub struct FlowEvent {
    pub kind: &'static str,
    pub action: &'static str,
    pub side: Side,
    pub correlation_id: String,
    pub update: u32,
    pub value: f64,
    pub volume: i64,
    pub n_trades: u32,
    pub first_ts: DateTime<Utc>,
    pub last_ts: DateTime<Utc>,
    pub first_price: f64,
    pub last_price: f64,
    pub share: f64,
    pub threshold: f64,
    pub score: f64,
    pub severity: Severity,
    pub explanation: Vec<Contribution>,
    pub summary: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Episode {
    id: String,
    emitted_value: f64,
    updates: u32,
}

/// Open/update/cooldown bookkeeping for one (symbol, side).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EpisodeSlot {
    active: Option<Episode>,
    ended_at: Option<f64>,
}

impl EpisodeSlot {
    /// Returns ("open"|"update", id, update#) when this observation should be signalled.
    fn observe(
        &mut self,
        hit: bool,
        value: f64,
        now: f64,
        step: f64,
        cooldown: f64,
        make_id: impl FnOnce() -> String,
    ) -> Option<(&'static str, String, u32)> {
        if !hit {
            if self.active.take().is_some() {
                self.ended_at = Some(now);
            }
            return None;
        }
        match &mut self.active {
            Some(e) if value >= e.emitted_value * (1.0 + step) => {
                e.emitted_value = value;
                e.updates += 1;
                Some(("update", e.id.clone(), e.updates))
            }
            Some(_) => None,
            None if self.ended_at.is_some_and(|t| now - t < cooldown) => None,
            None => {
                let id = make_id();
                self.active = Some(Episode {
                    id: id.clone(),
                    emitted_value: value,
                    updates: 0,
                });
                Some(("open", id, 0))
            }
        }
    }
}

/// `map.entry(key.to_string())` allocates on every call; this only allocates on insert.
pub(crate) fn slot<'a, V: Default>(map: &'a mut HashMap<String, V>, key: &str) -> &'a mut V {
    if !map.contains_key(key) {
        map.insert(key.to_string(), V::default());
    }
    map.get_mut(key).expect("just inserted")
}

fn clamp01(x: f64) -> f64 {
    x.clamp(0.0, 1.0)
}

/// 0 at x == base, 1 at x == base * span (log scale).
fn log_ratio(x: f64, base: f64, span: f64) -> f64 {
    if x <= 0.0 || base <= 0.0 {
        0.0
    } else {
        clamp01((x / base).ln() / span.ln())
    }
}

fn scored(
    factors: &[(&str, &str, f64, f64, f64, String)],
    cfg: &RadarConfig,
) -> (f64, Severity, Vec<Contribution>) {
    let mut acc = 0.0;
    let explanation = factors
        .iter()
        .map(|(f, label, raw, n, w, note)| {
            let c = 100.0 * w * n;
            acc += c;
            Contribution {
                factor: f.to_string(),
                label: label.to_string(),
                raw: py_round(*raw, 4),
                normalized: py_round(*n, 4),
                weight: *w,
                contribution: py_round(c, 2),
                note: note.clone(),
            }
        })
        .collect();
    let s = py_round(acc, 1);
    let sev = if s >= cfg.alert_score {
        Severity::Alert
    } else if s >= cfg.notice_score {
        Severity::Notice
    } else {
        Severity::Info
    };
    (s, sev, explanation)
}

fn pct(a: f64, b: f64) -> f64 {
    if a > 0.0 {
        (b - a) / a * 100.0
    } else {
        0.0
    }
}

// ------------------------------------------------------------------ absorption

/// Large (block-sized) trades of one symbol in the absorption window.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LargeRing {
    /// (ts epoch s, side, value, price)
    entries: VecDeque<(f64, Side, f64, f64)>,
    value: [f64; 2], // buy, sell
    n: [u32; 2],
}

impl LargeRing {
    fn idx(side: Side) -> Option<usize> {
        match side {
            Side::Buy => Some(0),
            Side::Sell => Some(1),
            Side::Unknown => None,
        }
    }

    fn evict(&mut self, cutoff: f64) {
        while self.entries.front().is_some_and(|e| e.0 < cutoff) {
            let (_, side, v, _) = self.entries.pop_front().expect("front");
            if let Some(i) = Self::idx(side) {
                self.value[i] -= v;
                self.n[i] -= 1;
            }
        }
        if self.entries.is_empty() {
            self.value = [0.0; 2];
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AbsorptionState {
    ring: LargeRing,
    /// [absorber = buy, absorber = sell]
    slots: [EpisodeSlot; 2],
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AbsorptionDetector {
    /// One entry per symbol, so the per-trade checks cost a single map lookup.
    symbols: HashMap<String, AbsorptionState>,
}

impl AbsorptionDetector {
    pub fn clear(&mut self) {
        self.symbols.clear();
    }

    /// Record a large trade (one that met a large-trade threshold) with a known side.
    pub fn on_large(
        &mut self,
        symbol: &str,
        ts: DateTime<Utc>,
        side: Side,
        value: f64,
        price: f64,
    ) {
        let Some(i) = LargeRing::idx(side) else {
            return;
        };
        let r = &mut slot(&mut self.symbols, symbol).ring;
        r.entries
            .push_back((ts.timestamp_micros() as f64 / 1e6, side, value, price));
        r.value[i] += value;
        r.n[i] += 1;
    }

    /// Nothing to evaluate: no large trade in the window and no open episode. Lets the
    /// radar skip the check (and the tick-size lookup) for most trades.
    pub fn idle(&self, symbol: &str) -> bool {
        self.symbols
            .get(symbol)
            .is_none_or(|s| s.ring.entries.is_empty() && s.slots.iter().all(|x| x.active.is_none()))
    }

    /// Symbols with an open episode (re-checked on the timer so episodes can end).
    pub fn active_symbols(&self) -> Vec<String> {
        self.symbols
            .iter()
            .filter(|(_, s)| s.slots.iter().any(|x| x.active.is_some()))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Evaluate `symbol`'s window after a trade (or on the timer, to end episodes).
    #[allow(clippy::too_many_arguments)]
    pub fn check(
        &mut self,
        symbol: &str,
        w: &SymbolWindow,
        open_threshold: f64,
        tick_size: f64,
        now: DateTime<Utc>,
        acfg: &AbsorptionConfig,
        cfg: &RadarConfig,
    ) -> Option<FlowEvent> {
        let min_value = acfg.min_value_k * open_threshold;
        let t = now.timestamp_micros() as f64 / 1e6;
        let st = slot(&mut self.symbols, symbol);
        st.ring.evict(t - w.window_sec);
        let ring = &st.ring;
        let large = ring.value[0] + ring.value[1];
        let slots = &mut st.slots;
        let mut out = None;
        // i = 0: sells absorbed by a buyer; i = 1: buys absorbed by a seller
        for (i, slot) in slots.iter_mut().enumerate() {
            let (absorbed, n, absorber, pressured, dir) = if i == 0 {
                (ring.value[1], ring.n[1], Side::Buy, Side::Sell, -1.0)
            } else {
                (ring.value[0], ring.n[0], Side::Sell, Side::Buy, 1.0)
            };
            let share = if large > 0.0 { absorbed / large } else { 0.0 };
            let mut hit = absorbed >= min_value && share >= acfg.min_share && n >= acfg.min_trades;
            let (mut since, mut first_px, mut last_px, mut follow) = (t, 0.0, 0.0, 0.0);
            if hit {
                // Only once the cheap conditions hold (the ring holds large trades only):
                // the pressure must be sustained, not one parent order filled in a burst,
                // and the pressured trades' own prices — same side of the book, so no
                // bid/ask bounce — must not follow by more than `max_move_ticks`.
                let mut pressure = ring.entries.iter().filter(|e| e.1 == pressured);
                let first = pressure.next().expect("n >= min_trades");
                let last = pressure.next_back().unwrap_or(first);
                (since, first_px, last_px) = (first.0, first.3, last.3);
                follow = dir * (last_px - first_px) / tick_size.max(1e-9);
                hit =
                    last.0 - since >= acfg.min_duration_sec && follow <= acfg.max_move_ticks + 1e-9;
            }
            let mv = pct(first_px, last_px);
            let ts_ms = now.timestamp_millis();
            let Some((action, id, update)) = slot.observe(
                hit,
                absorbed,
                t,
                acfg.update_step_ratio,
                acfg.cooldown_sec,
                || format!("{symbol}-absorption-{}-{ts_ms}", absorber.as_str()),
            ) else {
                continue;
            };
            let factors = [
                (
                    "value",
                    "Giá trị bị hấp thụ",
                    absorbed,
                    log_ratio(absorbed, min_value, 10.0),
                    0.35,
                    "0 tại ngưỡng, 1 tại 10× ngưỡng".to_string(),
                ),
                (
                    "share",
                    "Lệnh lớn dồn một chiều",
                    share,
                    clamp01((share - acfg.min_share) / (1.0 - acfg.min_share).max(1e-9)),
                    0.25,
                    format!("{:.0}% giá trị lệnh lớn", share * 100.0),
                ),
                (
                    "hold",
                    "Giá giữ vững",
                    -follow,
                    clamp01(0.5 - follow * 0.25),
                    0.25,
                    "0,5 khi giá đứng yên, 1 khi đi ngược 2 bước giá".to_string(),
                ),
                (
                    "trades",
                    "Số lệnh lớn",
                    n as f64,
                    log_ratio(n as f64, acfg.min_trades as f64, 5.0),
                    0.15,
                    String::new(),
                ),
            ];
            let (score, severity, explanation) = scored(&factors, cfg);
            let what = if i == 0 { "bán" } else { "mua" };
            out = Some(FlowEvent {
                kind: "absorption",
                action,
                side: absorber,
                correlation_id: id,
                update,
                value: absorbed,
                volume: 0,
                n_trades: n,
                first_ts: chrono::DateTime::from_timestamp_micros((since * 1e6) as i64)
                    .unwrap_or(now),
                last_ts: now,
                first_price: first_px,
                last_price: last_px,
                share,
                threshold: min_value,
                score,
                severity,
                explanation,
                summary: vec![
                    format!(
                        "Hấp thụ lực {what} · {} lệnh lớn {what} chủ động · {:.0}% lệnh lớn",
                        n,
                        share * 100.0
                    ),
                    format!(
                        "Giá khớp {mv:+.2}% sau {:.0} phút dù lệnh lớn dồn một chiều",
                        (t - since) / 60.0
                    ),
                ],
            });
        }
        out
    }
}

// ------------------------------------------------------------------ accumulation

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Ring {
    /// (ts epoch s, volume, value)
    entries: VecDeque<(f64, i64, f64)>,
    value: f64,
    vol: f64,
    vol2: f64,
}

impl Ring {
    fn push(&mut self, ts: f64, volume: i64, value: f64) {
        self.entries.push_back((ts, volume, value));
        self.value += value;
        self.vol += volume as f64;
        self.vol2 += (volume as f64).powi(2);
    }

    fn evict(&mut self, cutoff: f64) {
        while self.entries.front().is_some_and(|e| e.0 < cutoff) {
            let (_, v, x) = self.entries.pop_front().expect("front");
            self.value -= x;
            self.vol -= v as f64;
            self.vol2 -= (v as f64).powi(2);
        }
        if self.entries.is_empty() {
            (self.value, self.vol, self.vol2) = (0.0, 0.0, 0.0);
        }
    }

    fn n(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Coefficient of variation of trade volumes.
    fn volume_cv(&self) -> f64 {
        let n = self.entries.len() as f64;
        if n < 2.0 || self.vol <= 0.0 {
            return f64::INFINITY;
        }
        let mean = self.vol / n;
        let var = (self.vol2 / n - mean * mean).max(0.0);
        var.sqrt() / mean
    }

    /// Share of trades carrying the most frequent volume (O(n), only evaluated when the
    /// cheaper conditions already hold).
    fn repeat_share(&self) -> f64 {
        let mut counts: HashMap<i64, u32> = HashMap::default();
        let mut best = 0;
        for (_, v, _) in &self.entries {
            let c = counts.entry(*v).or_default();
            *c += 1;
            best = best.max(*c);
        }
        if self.entries.is_empty() {
            0.0
        } else {
            best as f64 / self.entries.len() as f64
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AccState {
    rings: [Ring; 2], // buy, sell
    slots: [EpisodeSlot; 2],
    last_price: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccumulationDetector {
    symbols: HashMap<String, AccState>,
}

impl AccumulationDetector {
    pub fn clear(&mut self) {
        self.symbols.clear();
    }

    /// A block group just opened on `side`: its trades are visible to the block detector,
    /// so they no longer count as hidden flow.
    pub fn forget_since(&mut self, symbol: &str, side: Side, since: DateTime<Utc>) {
        let idx = match side {
            Side::Buy => 0,
            Side::Sell => 1,
            Side::Unknown => return,
        };
        let Some(st) = self.symbols.get_mut(symbol) else {
            return;
        };
        let t = since.timestamp_micros() as f64 / 1e6;
        let r = &mut st.rings[idx];
        while r.entries.back().is_some_and(|e| e.0 >= t) {
            let (_, v, x) = r.entries.pop_back().expect("back");
            r.value -= x;
            r.vol -= v as f64;
            r.vol2 -= (v as f64).powi(2);
        }
    }

    /// Is this trade "medium": block-relevant in aggregate but below the large bar?
    pub fn is_medium(
        value: f64,
        is_large: bool,
        median: Option<f64>,
        acfg: &AccumulationConfig,
    ) -> bool {
        let bar = median
            .filter(|m| *m > 0.0)
            .map_or(acfg.medium_min_value, |m| {
                (acfg.medium_k * m).max(acfg.medium_min_value)
            });
        !is_large && value >= bar
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_trade(
        &mut self,
        symbol: &str,
        ts: DateTime<Utc>,
        side: Side,
        volume: i64,
        value: f64,
        price: f64,
        medium: bool,
        open_threshold: f64,
        acfg: &AccumulationConfig,
        cfg: &RadarConfig,
    ) -> Option<FlowEvent> {
        // most trades are not medium: return before touching the map
        let idx = match side {
            Side::Buy if medium => 0,
            Side::Sell if medium => 1,
            _ => return None,
        };
        let st = slot(&mut self.symbols, symbol);
        let t = ts.timestamp_micros() as f64 / 1e6;
        st.last_price = price;
        st.rings[idx].push(t, volume, value);
        let cutoff = t - acfg.window_sec;
        st.rings[0].evict(cutoff);
        st.rings[1].evict(cutoff);
        Self::evaluate(st, idx, symbol, ts, open_threshold, acfg, cfg)
    }

    /// Timer: expire old trades so finished episodes end and the cooldown starts.
    pub fn on_timer(&mut self, now: DateTime<Utc>, acfg: &AccumulationConfig) {
        let t = now.timestamp_micros() as f64 / 1e6;
        for st in self.symbols.values_mut() {
            for i in 0..2 {
                st.rings[i].evict(t - acfg.window_sec);
                let r = &st.rings[i];
                if st.slots[i].active.is_some() && r.n() < acfg.min_trades {
                    st.slots[i].observe(false, 0.0, t, 0.0, 0.0, String::new);
                }
            }
        }
    }

    fn evaluate(
        st: &mut AccState,
        idx: usize,
        symbol: &str,
        ts: DateTime<Utc>,
        open_threshold: f64,
        acfg: &AccumulationConfig,
        cfg: &RadarConfig,
    ) -> Option<FlowEvent> {
        let r = &st.rings[idx];
        let total = st.rings[0].value + st.rings[1].value;
        let share = if total > 0.0 { r.value / total } else { 0.0 };
        let min_value = acfg.min_value_k * open_threshold;
        let cv = r.volume_cv();
        let mut hit = r.n() >= acfg.min_trades && r.value >= min_value && share >= acfg.min_share;
        let mut repeat = 0.0;
        if hit && cv > acfg.max_volume_cv {
            repeat = r.repeat_share();
            hit = repeat >= acfg.min_repeat_share;
        }
        let t = ts.timestamp_micros() as f64 / 1e6;
        let side = if idx == 0 { Side::Buy } else { Side::Sell };
        let ts_ms = ts.timestamp_millis();
        let (action, id, update) = st.slots[idx].observe(
            hit,
            r.value,
            t,
            acfg.update_step_ratio,
            acfg.cooldown_sec,
            || format!("{symbol}-accumulation-{}-{ts_ms}", side.as_str()),
        )?;
        let r = &st.rings[idx];
        let (first, last) = (r.entries.front()?, r.entries.back()?);
        let regular = if cv <= acfg.max_volume_cv {
            clamp01(1.0 - cv / acfg.max_volume_cv.max(1e-9) * 0.5)
        } else {
            clamp01(repeat)
        };
        let n = r.n();
        let span_min = (last.0 - first.0) / 60.0;
        let factors = [
            (
                "value",
                "Tổng giá trị",
                r.value,
                log_ratio(r.value, min_value, 10.0),
                0.35,
                "0 tại ngưỡng, 1 tại 10× ngưỡng".to_string(),
            ),
            (
                "share",
                "Một chiều",
                share,
                clamp01((share - acfg.min_share) / (1.0 - acfg.min_share).max(1e-9)),
                0.2,
                format!("{:.0}% giá trị lệnh vừa", share * 100.0),
            ),
            (
                "regularity",
                "Đều đặn về khối lượng",
                if cv.is_finite() { cv } else { 0.0 },
                regular,
                0.25,
                if cv <= acfg.max_volume_cv {
                    format!("hệ số biến thiên KL {cv:.2}")
                } else {
                    format!("{:.0}% lệnh cùng một mức KL", repeat * 100.0)
                },
            ),
            (
                "trades",
                "Số lệnh",
                n as f64,
                log_ratio(n as f64, acfg.min_trades as f64, 5.0),
                0.2,
                String::new(),
            ),
        ];
        let (score, severity, explanation) = scored(&factors, cfg);
        let what = if idx == 0 { "Gom" } else { "Xả" };
        let first_price = first.2 / first.1.max(1) as f64;
        let mv = pct(first_price, st.last_price);
        Some(FlowEvent {
            kind: "accumulation",
            action,
            side,
            correlation_id: id,
            update,
            value: r.value,
            volume: r.vol as i64,
            n_trades: n,
            first_ts: chrono::DateTime::from_timestamp_micros((first.0 * 1e6) as i64).unwrap_or(ts),
            last_ts: ts,
            first_price,
            last_price: st.last_price,
            share,
            threshold: min_value,
            score,
            severity,
            explanation,
            summary: vec![
                format!(
                    "{what} âm thầm · {n} lệnh vừa trong {span_min:.0} phút · {:.0}% cùng chiều",
                    share * 100.0
                ),
                format!("Giá {mv:+.2}% từ lệnh đầu tiên của chuỗi"),
            ],
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

    fn window(trades: &[(i64, Side, f64, f64)]) -> SymbolWindow {
        let mut w = SymbolWindow::new(900.0);
        for (s, side, value, price) in trades {
            w.add(1_800_000_000.0 + *s as f64, *side, *value, true, *price);
        }
        w
    }

    /// Feed trades (all large) to the detector, then evaluate at `now`.
    fn absorb(
        d: &mut AbsorptionDetector,
        trades: &[(i64, Side, f64, f64)],
        fed: &mut usize,
        now: i64,
        cfg: &RadarConfig,
    ) -> Option<FlowEvent> {
        for (s, side, value, price) in &trades[*fed..] {
            d.on_large("AAA", t(*s), *side, *value, *price);
        }
        *fed = trades.len();
        d.check(
            "AAA",
            &window(trades),
            1e9,
            50.0,
            t(now),
            &cfg.absorption,
            cfg,
        )
    }

    #[test]
    fn absorption_needs_one_sided_flow_and_a_steady_price() {
        let cfg = RadarConfig::default();
        // 6 large sells of 600M over 5 minutes, price steady: absorbed by a buyer
        let sells: Vec<_> = (0..6)
            .map(|i| (i * 60, Side::Sell, 600e6, 20_000.0))
            .collect();
        let ev = absorb(&mut Default::default(), &sells, &mut 0, 360, &cfg).expect("absorption");
        assert_eq!((ev.side, ev.action, ev.n_trades), (Side::Buy, "open", 6));
        // the same amount filled in a 50 s burst is one parent order, not absorption
        let burst: Vec<_> = (0..6)
            .map(|i| (i * 10, Side::Sell, 600e6, 20_000.0))
            .collect();
        assert!(absorb(&mut Default::default(), &burst, &mut 0, 400, &cfg).is_none());
        // same flow but the price falls 1%: the sells are not absorbed
        let mut falling = sells.clone();
        falling.push((400, Side::Sell, 600e6, 19_800.0));
        assert!(absorb(&mut Default::default(), &falling, &mut 0, 400, &cfg).is_none());
        // two-sided flow is not absorption
        let mut mixed = sells.clone();
        mixed.extend((0..4).map(|i| (300 + i, Side::Buy, 600e6, 20_000.0)));
        assert!(absorb(&mut Default::default(), &mixed, &mut 0, 400, &cfg).is_none());
    }

    #[test]
    fn absorption_updates_on_growth_then_cools_down() {
        let cfg = RadarConfig::default();
        let mut d = AbsorptionDetector::default();
        let mut fed = 0;
        // 5 large sells, one a minute (sustained 4 minutes)
        let mut trades: Vec<_> = (0..5)
            .map(|i| (i * 60, Side::Sell, 600e6, 20_000.0))
            .collect();
        let open = absorb(&mut d, &trades, &mut fed, 240, &cfg);
        assert_eq!(open.unwrap().action, "open");
        trades.push((250, Side::Sell, 600e6, 20_000.0));
        let small = absorb(&mut d, &trades, &mut fed, 250, &cfg);
        assert!(small.is_none(), "+20% < update step");
        trades.push((260, Side::Sell, 900e6, 20_000.0));
        let up = absorb(&mut d, &trades, &mut fed, 260, &cfg).unwrap();
        assert_eq!((up.action, up.update), ("update", 1));
        // episode ends (two-sided now) and cannot reopen within the cooldown
        trades.extend((0..8).map(|i| (270 + i, Side::Buy, 700e6, 20_000.0)));
        assert!(absorb(&mut d, &trades, &mut fed, 280, &cfg).is_none());
        trades.extend((0..6).map(|i| (300 + i * 60, Side::Sell, 600e6, 20_000.0)));
        assert!(absorb(&mut d, &trades, &mut fed, 600, &cfg).is_none());
        // after the cooldown, once the old flow left the 15-minute window
        let late = absorb(&mut d, &trades, &mut fed, 280 + 901, &cfg);
        assert_eq!(late.unwrap().action, "open");
    }

    #[test]
    fn accumulation_detects_regular_one_sided_medium_trades() {
        let cfg = RadarConfig::default();
        let a = &cfg.accumulation;
        let mut d = AccumulationDetector::default();
        let mut got = None;
        // 15 buys of 5,000 shares @ 20,000 (100M each) every 90 s = 1.5B over 21 min
        for i in 0..15 {
            let ev = d.on_trade(
                "AAA",
                t(i * 90),
                Side::Buy,
                5_000,
                100e6,
                20_000.0,
                true,
                1.5e9,
                a,
                &cfg,
            );
            if ev.is_some() {
                got = ev;
            }
        }
        let ev = got.expect("accumulation");
        assert_eq!(
            (ev.side, ev.kind, ev.n_trades),
            (Side::Buy, "accumulation", 15)
        );
        assert!(ev
            .explanation
            .iter()
            .any(|c| c.factor == "regularity" && c.normalized > 0.9));
    }

    #[test]
    fn accumulation_ignores_irregular_or_two_sided_flow() {
        let cfg = RadarConfig::default();
        let a = &cfg.accumulation;
        // irregular sizes
        let mut d = AccumulationDetector::default();
        let vols = [
            1_000, 9_000, 2_500, 15_000, 4_000, 700, 12_000, 3_300, 8_800, 1_900, 20_000, 5_100,
        ];
        let mut any = false;
        for (i, v) in vols.iter().enumerate() {
            any |= d
                .on_trade(
                    "AAA",
                    t(i as i64 * 60),
                    Side::Buy,
                    *v,
                    *v as f64 * 20_000.0,
                    20_000.0,
                    true,
                    0.5e9,
                    a,
                    &cfg,
                )
                .is_some();
        }
        assert!(!any, "irregular sizes are ordinary flow");
        // regular but two-sided
        let mut d = AccumulationDetector::default();
        let mut any = false;
        for i in 0..24 {
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            any |= d
                .on_trade(
                    "AAA",
                    t(i * 60),
                    side,
                    5_000,
                    100e6,
                    20_000.0,
                    true,
                    0.5e9,
                    a,
                    &cfg,
                )
                .is_some();
        }
        assert!(!any, "balanced flow is not accumulation");
    }
}
