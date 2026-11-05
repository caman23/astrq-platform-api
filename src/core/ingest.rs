//! Ingest & normalize: validate, dedupe, detect gaps/late ticks, stamp, publish.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use crate::core::bus::Bus;
use crate::core::clock::Clock;
use crate::core::metrics::METRICS;
use crate::core::models::{MarketEvent, Session};
use crate::core::sessions::SessionCalendar;
use crate::core::universe::Universe;

#[derive(Debug, Clone, Default, Serialize)]
pub struct IngestStats {
    pub accepted: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub late: u64,
    pub outside_universe: u64,
}

fn dedupe_key(ev: &MarketEvent) -> String {
    match ev {
        MarketEvent::Tick(t) => match (&t.seq, &t.raw_ref) {
            (Some(seq), _) => format!("tick|{}|{}|seq|{seq}", t.provider, t.symbol),
            (None, Some(r)) => format!("tick|{}|{}|ref|{r}", t.provider, t.symbol),
            // last resort: identical trades in the same timestamp would collapse
            (None, None) => format!(
                "tick|{}|{}|{}|{}|{}",
                t.provider,
                t.symbol,
                t.ts_exchange.to_rfc3339(),
                t.price,
                t.volume
            ),
        },
        // timestamps may be second-resolution: include the payload, not just the time
        other => {
            let mut v = serde_json::to_value(other).unwrap_or_default();
            if let Some(o) = v.as_object_mut() {
                o.remove("ts_received");
            }
            v.to_string()
        }
    }
}

pub type EventHook = Box<dyn Fn(&MarketEvent) + Send + Sync>;

pub struct Ingest {
    pub bus: Arc<Bus<MarketEvent>>,
    pub calendar: Arc<SessionCalendar>,
    pub universe: Arc<RwLock<Universe>>,
    pub clock: Option<Arc<dyn Clock>>,
    pub on_event: Option<EventHook>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
    cap: usize,
    last_seq: HashMap<(String, String), i64>,
    last_ts: HashMap<String, DateTime<Utc>>,
    pub stats: IngestStats,
}

impl Ingest {
    pub fn new(
        bus: Arc<Bus<MarketEvent>>,
        calendar: Arc<SessionCalendar>,
        universe: Arc<RwLock<Universe>>,
        clock: Option<Arc<dyn Clock>>,
    ) -> Self {
        Self {
            bus,
            calendar,
            universe,
            clock,
            on_event: None,
            seen: HashSet::new(),
            seen_order: VecDeque::new(),
            cap: 200_000,
            last_seq: HashMap::new(),
            last_ts: HashMap::new(),
            stats: IngestStats::default(),
        }
    }

    pub fn process(&mut self, mut ev: MarketEvent) -> Option<MarketEvent> {
        if !self.universe.read().contains(ev.symbol()) {
            self.stats.outside_universe += 1;
            return None;
        }
        let key = dedupe_key(&ev);
        if !self.seen.insert(key.clone()) {
            self.stats.duplicates += 1;
            METRICS.inc("ingest_duplicates_total", 1.0, &[]);
            return None;
        }
        self.seen_order.push_back(key);
        if self.seen_order.len() > self.cap {
            if let Some(old) = self.seen_order.pop_front() {
                self.seen.remove(&old);
            }
        }

        ev.set_ingest_mono(Instant::now());
        if let MarketEvent::Tick(t) = &mut ev {
            if let Some(seq) = t.seq {
                let sk = (t.provider.clone(), t.symbol.clone());
                let last = self.last_seq.get(&sk).copied();
                if let Some(l) = last {
                    if seq > l + 1 {
                        self.stats.gaps += (seq - l - 1) as u64;
                        METRICS.inc("ingest_seq_gaps_total", (seq - l - 1) as f64, &[]);
                    }
                }
                if last.is_none_or(|l| seq > l) {
                    self.last_seq.insert(sk, seq);
                }
            }
            match self.last_ts.get(&t.symbol) {
                Some(prev) if t.ts_exchange < *prev => {
                    self.stats.late += 1;
                    METRICS.inc("ingest_late_ticks_total", 1.0, &[]);
                }
                _ => {
                    self.last_ts.insert(t.symbol.clone(), t.ts_exchange);
                }
            }
            if t.session == Session::Closed {
                t.session = self.calendar.session_at(t.ts_exchange);
            }
            METRICS.inc("ticks_total", 1.0, &[]);
            METRICS.observe(
                "feed_latency_seconds",
                (t.ts_received - t.ts_exchange)
                    .num_microseconds()
                    .unwrap_or(0) as f64
                    / 1e6,
                &[],
            );
        }
        if let Some(c) = &self.clock {
            c.advance_to(ev.ts_exchange());
        }
        self.stats.accepted += 1;
        self.bus.publish(&ev);
        if let Some(cb) = &self.on_event {
            cb(&ev);
        }
        Some(ev)
    }
}
