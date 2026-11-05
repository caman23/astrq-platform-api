//! Pure normalization of SSI FastConnect Data messages into canonical events.
//!
//! Envelope (official guide, "Streaming data"): `{"DataType": "X-TRADE", "Content": "<json>"}`.
//! X-TRADE fields: Symbol, TradingDate (DD/MM/YYYY), Time (HH:MM:SS), LastPrice, LastVol,
//! TotalVol, Side ("BU" | "SD" | other = unknown), TradingSession, EstMatchedPrice; X adds
//! BidPrice1/AskPrice1/BidVol1/AskVol1. Key casing is inconsistent in the docs.
//!
//! Assumptions still to verify on real data: Time is Vietnam local time; prices are VND
//! (documented sample Ceiling 24500); TotalVol is cumulative; put-through is not flagged.

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Asia::Ho_Chi_Minh;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::core::config::SsiConfig;
use crate::core::models::{ExpectedPrice, MarketEvent, Quote, Session, Side, SideSource, Tick};
use crate::core::sessions::SessionCalendar;
use crate::providers::cumvol::CumulativeVolumeTracker;

pub const PROVIDER: &str = "ssi";

fn loads(s: &str) -> Option<Value> {
    serde_json::from_str(s).ok().or_else(|| {
        // documented samples carry trailing commas
        let cleaned = regex_lite_trailing_commas(s);
        serde_json::from_str(&cleaned).ok()
    })
}

fn regex_lite_trailing_commas(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == ',' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && (chars[j] == '}' || chars[j] == ']') {
                i += 1;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// Broadcast argument -> (DataType, content object).
pub fn decode_broadcast(arg: &Value) -> Option<(String, Map<String, Value>)> {
    let obj = match arg {
        Value::String(s) => loads(s)?,
        other => other.clone(),
    };
    let dtype = obj
        .get("DataType")
        .or_else(|| obj.get("dataType"))?
        .as_str()?
        .to_string();
    let content = obj.get("Content").or_else(|| obj.get("content"))?;
    let content = match content {
        Value::String(s) => loads(s)?,
        other => other.clone(),
    };
    Some((dtype, content.as_object()?.clone()))
}

fn num(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) if !s.is_empty() => s.parse().ok(),
        _ => None,
    }
}

pub struct SsiNormalizer {
    cfg: SsiConfig,
    calendar: Arc<SessionCalendar>,
    pub stats: BTreeMap<String, u64>,
    pub sessions_seen: BTreeMap<String, u64>,
    pub data_types_seen: BTreeMap<String, u64>,
    cumvol: CumulativeVolumeTracker,
}

impl SsiNormalizer {
    pub fn new(cfg: SsiConfig, calendar: Arc<SessionCalendar>) -> Self {
        Self {
            cfg,
            calendar,
            stats: BTreeMap::new(),
            sessions_seen: BTreeMap::new(),
            data_types_seen: BTreeMap::new(),
            cumvol: CumulativeVolumeTracker::default(),
        }
    }

    fn bump(map: &mut BTreeMap<String, u64>, k: &str) {
        *map.entry(k.to_string()).or_default() += 1;
    }

    fn ts(&mut self, c: &Map<String, Value>, received: DateTime<Utc>) -> DateTime<Utc> {
        if let (Some(d), Some(t)) = (
            c.get("tradingdate").and_then(Value::as_str),
            c.get("time").and_then(Value::as_str),
        ) {
            if let Ok(local) =
                NaiveDateTime::parse_from_str(&format!("{d} {t}"), "%d/%m/%Y %H:%M:%S")
            {
                if let Some(x) = Ho_Chi_Minh.from_local_datetime(&local).earliest() {
                    return x.with_timezone(&Utc);
                }
            }
            Self::bump(&mut self.stats, "bad_timestamp");
        }
        received
    }

    pub fn events(
        &mut self,
        dtype: &str,
        content: &Map<String, Value>,
        received: DateTime<Utc>,
    ) -> Vec<MarketEvent> {
        Self::bump(&mut self.data_types_seen, dtype);
        let c: Map<String, Value> = content
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();
        let symbol = c
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_uppercase();
        if symbol.is_empty() {
            Self::bump(&mut self.stats, "no_symbol");
            return vec![];
        }
        let ts = self.ts(&c, received);
        let mut out = Vec::new();
        if dtype == "X-QUOTE" || dtype == "X" {
            out.extend(self.quote(&symbol, &c, ts, received));
        }
        if dtype == "X-TRADE" || dtype == "X" {
            out.extend(self.trade(&symbol, &c, ts, received));
        }
        out
    }

    fn quote(
        &self,
        symbol: &str,
        c: &Map<String, Value>,
        ts: DateTime<Utc>,
        received: DateTime<Utc>,
    ) -> Option<MarketEvent> {
        let bid = num(c.get("bidprice1")).filter(|x| *x != 0.0);
        let ask = num(c.get("askprice1")).filter(|x| *x != 0.0);
        if bid.is_none() && ask.is_none() {
            return None;
        }
        let m = self.cfg.price_multiplier;
        let vm = self.cfg.volume_multiplier as f64;
        Some(MarketEvent::Quote(Quote {
            provider: PROVIDER.into(),
            symbol: symbol.into(),
            ts_exchange: ts,
            ts_received: received,
            best_bid: bid.map(|b| b * m),
            best_ask: ask.map(|a| a * m),
            bid_size: num(c.get("bidvol1"))
                .filter(|x| *x != 0.0)
                .map(|v| (v * vm) as i64),
            ask_size: num(c.get("askvol1"))
                .filter(|x| *x != 0.0)
                .map(|v| (v * vm) as i64),
            ingest_mono: None,
        }))
    }

    fn trade(
        &mut self,
        symbol: &str,
        c: &Map<String, Value>,
        ts: DateTime<Utc>,
        received: DateTime<Utc>,
    ) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        let code = match c.get("tradingsession") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => String::new(),
        };
        if !code.is_empty() {
            Self::bump(&mut self.sessions_seen, &code);
        }
        let session = self.cfg.session_map.get(&code).copied();
        let cal_session = session.unwrap_or_else(|| self.calendar.session_at(ts));
        if let Some(est) = num(c.get("estmatchedprice")).filter(|x| *x > 0.0) {
            if cal_session.is_auction() {
                out.push(MarketEvent::ExpectedPrice(ExpectedPrice {
                    provider: PROVIDER.into(),
                    symbol: symbol.into(),
                    ts_exchange: ts,
                    ts_received: received,
                    session: cal_session,
                    price: est * self.cfg.price_multiplier,
                    volume: None,
                    ingest_mono: None,
                }));
            }
        }
        let price = num(c.get("lastprice"));
        let vol = num(c.get("lastvol"));
        let total = num(c.get("totalvol"));
        let (Some(price), Some(vol)) = (price.filter(|x| *x > 0.0), vol.filter(|x| *x > 0.0))
        else {
            Self::bump(&mut self.stats, "no_trade_fields");
            return out;
        };
        if !self.cumvol.accept(symbol, total, vol, &mut self.stats) {
            return out; // snapshot repeat / out-of-order
        }
        let px = price * self.cfg.price_multiplier;
        if px < 500.0 {
            Self::bump(&mut self.stats, "price_looks_like_thousands");
        }
        let qty = (vol * self.cfg.volume_multiplier as f64) as i64;
        let side = match c
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "BU" => Side::Buy,
            "SD" => Side::Sell,
            _ => Side::Unknown,
        };
        Self::bump(&mut self.stats, "ticks");
        Self::bump(&mut self.stats, &format!("side_{}", side.as_str()));
        out.push(MarketEvent::Tick(Tick {
            provider: PROVIDER.into(),
            symbol: symbol.into(),
            ts_exchange: ts,
            ts_received: received,
            price: px,
            volume: qty,
            value: px * qty as f64,
            side,
            side_source: if side != Side::Unknown {
                SideSource::Provider
            } else {
                SideSource::Unknown
            },
            session: session.unwrap_or(Session::Closed),
            is_negotiated: false,
            seq: None,
            raw_ref: total.map(|t| format!("tv:{}", t as i64)),
            ingest_mono: None,
        }));
        out
    }

    pub fn summary(&self) -> Value {
        json!({"counters": self.stats, "sessions_seen": self.sessions_seen, "data_types_seen": self.data_types_seen})
    }
}
