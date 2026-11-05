//! Trade side classification: provider flag > quote rule > tick rule (Lee-Ready style).
//! Inferred sides are approximations with lower confidence; inside-spread trades are flagged.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::models::{Quote, Side, SideSource, Tick};

pub const INSIDE_SPREAD_CONFIDENCE: f64 = 0.5;

pub fn confidence_of(source: SideSource) -> f64 {
    match source {
        SideSource::Provider => 1.0,
        SideSource::QuoteRule => 0.75,
        SideSource::TickRule => 0.5,
        SideSource::Unknown => 0.0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Classified {
    pub side: Side,
    pub source: SideSource,
    pub inside_spread: bool,
}

impl Classified {
    fn new(side: Side, source: SideSource) -> Self {
        Self { side, source, inside_spread: false }
    }
    pub fn confidence(&self) -> f64 {
        if self.source == SideSource::QuoteRule && self.inside_spread {
            INSIDE_SPREAD_CONFIDENCE
        } else {
            confidence_of(self.source)
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SideClassifier {
    pub quote_stale_sec: f64,
    quotes: HashMap<String, (Option<f64>, Option<f64>, DateTime<Utc>)>,
    last_price: HashMap<String, f64>,
    last_dir: HashMap<String, Side>,
}

impl SideClassifier {
    pub fn new(quote_stale_sec: f64) -> Self {
        Self { quote_stale_sec, ..Default::default() }
    }

    pub fn on_quote(&mut self, q: &Quote) {
        self.quotes.insert(q.symbol.clone(), (q.best_bid, q.best_ask, q.ts_exchange));
    }

    pub fn classify(&mut self, t: &Tick) -> Classified {
        let result = self.classify_inner(t);
        if let Some(prev) = self.last_price.get(&t.symbol).copied() {
            if t.price != prev {
                self.last_dir.insert(t.symbol.clone(), if t.price > prev { Side::Buy } else { Side::Sell });
            }
        }
        self.last_price.insert(t.symbol.clone(), t.price);
        result
    }

    fn classify_inner(&self, t: &Tick) -> Classified {
        if t.side != Side::Unknown && t.side_source == SideSource::Provider {
            return Classified::new(t.side, SideSource::Provider);
        }
        if t.session.is_auction() || t.is_negotiated {
            // single-price auction / negotiated deals have no aggressor
            return Classified::new(Side::Unknown, SideSource::Unknown);
        }
        if let Some((Some(bid), Some(ask), ts)) = self.quotes.get(&t.symbol).copied() {
            let age = (t.ts_exchange - ts).num_microseconds().unwrap_or(i64::MAX).unsigned_abs() as f64 / 1e6;
            if age <= self.quote_stale_sec && ask >= bid {
                if t.price >= ask {
                    return Classified::new(Side::Buy, SideSource::QuoteRule);
                }
                if t.price <= bid {
                    return Classified::new(Side::Sell, SideSource::QuoteRule);
                }
                let mid = (bid + ask) / 2.0;
                if t.price > mid {
                    return Classified { side: Side::Buy, source: SideSource::QuoteRule, inside_spread: true };
                }
                if t.price < mid {
                    return Classified { side: Side::Sell, source: SideSource::QuoteRule, inside_spread: true };
                }
            }
        }
        if let Some(prev) = self.last_price.get(&t.symbol).copied() {
            if t.price > prev {
                return Classified::new(Side::Buy, SideSource::TickRule);
            }
            if t.price < prev {
                return Classified::new(Side::Sell, SideSource::TickRule);
            }
            if let Some(d) = self.last_dir.get(&t.symbol) {
                // zero-tick: inherit the last non-zero move
                return Classified::new(*d, SideSource::TickRule);
            }
        }
        Classified::new(Side::Unknown, SideSource::Unknown)
    }
}
