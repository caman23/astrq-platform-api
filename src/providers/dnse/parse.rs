//! Pure normalization of DNSE market-data WebSocket messages into canonical events.
//!
//! Field names from the official docs (developers.dnse.com.vn, market-data/connect):
//! tick / tick_extra: symbol, boardId, matchPrice, matchQtty, side (tick_extra only),
//! totalVolumeTraded, tradingSessionId, time{Seconds,Nanos};
//! expected_price: symbol, boardId, expectedTradePrice, expectedTradeQuantity, time.
//! The `dnse` PyPI SDK uses other names (T="t"/"te"/"e", price, volume, timestamp,
//! total_volume); until real traffic is captured both shapes are accepted.
//!
//! Still to verify on live data: price unit (doc samples look like thousand VND), the
//! full set of `side` values, tradingSessionId codes, the data envelope.

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::core::config::DnseConfig;
use crate::core::models::{ExpectedPrice, MarketEvent, Session, Side, SideSource, Tick};
use crate::core::sessions::SessionCalendar;
use crate::providers::cumvol::CumulativeVolumeTracker;

pub const PROVIDER: &str = "dnse";

fn first<'a>(p: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|k| p.get(*k).filter(|v| !v.is_null()))
}

fn num(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) if !s.is_empty() => s.parse().ok(),
        _ => None,
    }
}

fn text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

pub struct DnseNormalizer {
    cfg: DnseConfig,
    calendar: Arc<SessionCalendar>,
    pub stats: BTreeMap<String, u64>,
    pub sessions_seen: BTreeMap<String, u64>,
    pub sides_seen: BTreeMap<String, u64>,
    pub shapes_seen: BTreeMap<String, u64>,
    cumvol: CumulativeVolumeTracker,
}

impl DnseNormalizer {
    pub fn new(cfg: DnseConfig, calendar: Arc<SessionCalendar>) -> Self {
        Self {
            cfg,
            calendar,
            stats: BTreeMap::new(),
            sessions_seen: BTreeMap::new(),
            sides_seen: BTreeMap::new(),
            shapes_seen: BTreeMap::new(),
            cumvol: CumulativeVolumeTracker::default(),
        }
    }

    fn bump(map: &mut BTreeMap<String, u64>, k: &str) {
        *map.entry(k.to_string()).or_default() += 1;
    }

    fn ts(&mut self, p: &Value, received: DateTime<Utc>) -> DateTime<Utc> {
        match first(p, &["time", "timestamp"]) {
            Some(Value::Object(o)) => {
                let secs = num(o.get("Seconds").or_else(|| o.get("seconds")));
                let nanos = num(o.get("Nanos").or_else(|| o.get("nanos"))).unwrap_or(0.0);
                if let Some(s) = secs {
                    if let Some(t) = Utc.timestamp_opt(s as i64, nanos as u32).single() {
                        return t;
                    }
                }
            }
            v @ Some(_) => {
                if let Some(n) = num(v).filter(|n| *n > 0.0) {
                    let micros = if n > 1e12 { n * 1e3 } else { n * 1e6 };
                    if let Some(t) = Utc.timestamp_micros(micros as i64).single() {
                        return t;
                    }
                }
            }
            None => {}
        }
        Self::bump(&mut self.stats, "no_timestamp");
        received
    }

    fn price(&mut self, raw: f64) -> f64 {
        let unit = self.cfg.price_unit.as_str();
        if unit == "thousand" || (unit == "auto" && raw < 1000.0) {
            if unit == "auto" {
                Self::bump(&mut self.stats, "price_scaled_x1000");
            }
            raw * 1000.0
        } else {
            raw
        }
    }

    fn session(&mut self, p: &Value) -> Option<Session> {
        let code = text(first(p, &["tradingSessionId", "trading_session_id"]))?;
        Self::bump(&mut self.sessions_seen, &code);
        self.cfg.session_map.get(&code).copied()
    }

    pub fn events(&mut self, msg: &Value, received: DateTime<Utc>) -> Vec<MarketEvent> {
        let p = msg.get("data").filter(|d| d.is_object()).unwrap_or(msg);
        let t_code = text(msg.get("T").or_else(|| p.get("T"))).unwrap_or_default();
        let symbol = text(first(p, &["symbol", "s"]))
            .unwrap_or_default()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            Self::bump(&mut self.stats, "no_symbol");
            return vec![];
        }
        if p.get("expectedTradePrice").is_some() || t_code == "e" {
            let shape = if p.get("expectedTradePrice").is_some() {
                "expected:doc"
            } else {
                "expected:sdk"
            };
            Self::bump(&mut self.shapes_seen, shape);
            return self.expected(&symbol, p, received);
        }
        if p.get("matchPrice").is_some() || t_code == "t" || t_code == "te" {
            let shape = if p.get("matchPrice").is_some() {
                "trade:doc"
            } else {
                "trade:sdk"
            };
            Self::bump(&mut self.shapes_seen, shape);
            return self.trade(&symbol, p, received);
        }
        Self::bump(&mut self.stats, "unhandled_message");
        vec![]
    }

    fn expected(&mut self, symbol: &str, p: &Value, received: DateTime<Utc>) -> Vec<MarketEvent> {
        let Some(price) = num(first(p, &["expectedTradePrice", "price"])).filter(|x| *x > 0.0)
        else {
            return vec![];
        };
        let ts = self.ts(p, received);
        let session = self
            .session(p)
            .unwrap_or_else(|| self.calendar.session_at(ts));
        if !session.is_auction() {
            Self::bump(&mut self.stats, "expected_outside_auction");
            return vec![];
        }
        let qty = num(first(p, &["expectedTradeQuantity", "volume"]));
        let price = self.price(price);
        vec![MarketEvent::ExpectedPrice(ExpectedPrice {
            provider: PROVIDER.into(),
            symbol: symbol.into(),
            ts_exchange: ts,
            ts_received: received,
            session,
            price,
            volume: qty
                .filter(|q| *q != 0.0)
                .map(|q| (q * self.cfg.volume_multiplier as f64) as i64),
            ingest_mono: None,
        })]
    }

    fn trade(&mut self, symbol: &str, p: &Value, received: DateTime<Utc>) -> Vec<MarketEvent> {
        let price = num(first(p, &["matchPrice", "price"]));
        let vol = num(first(p, &["matchQtty", "volume"]));
        let (Some(price), Some(vol)) = (price.filter(|x| *x > 0.0), vol.filter(|x| *x > 0.0))
        else {
            Self::bump(&mut self.stats, "no_trade_fields");
            return vec![];
        };
        let board = text(first(p, &["boardId", "board_id"])).unwrap_or_else(|| "G1".into());
        let total = num(first(p, &["totalVolumeTraded", "total_volume"]));
        if !self
            .cumvol
            .accept(&format!("{symbol}|{board}"), total, vol, &mut self.stats)
        {
            return vec![];
        }
        let negotiated = self.cfg.putthrough_boards.contains(&board);
        let raw_side = text(first(p, &["side"]))
            .unwrap_or_default()
            .to_ascii_uppercase();
        if !raw_side.is_empty() {
            Self::bump(&mut self.sides_seen, &raw_side);
        }
        let side = if negotiated {
            Side::Unknown
        } else {
            match raw_side.as_str() {
                "BUY" | "B" => Side::Buy,
                "SELL" | "S" => Side::Sell,
                _ => Side::Unknown,
            }
        };
        let px = self.price(price);
        let qty = (vol * self.cfg.volume_multiplier as f64) as i64;
        let session = self.session(p);
        Self::bump(&mut self.stats, "ticks");
        Self::bump(&mut self.stats, &format!("side_{}", side.as_str()));
        let ts = self.ts(p, received);
        vec![MarketEvent::Tick(Tick {
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
            session: session.unwrap_or(Session::Closed), // Closed -> ingest uses the calendar
            is_negotiated: negotiated,
            seq: None,
            raw_ref: total.map(|t| format!("{board}:tv:{}", t as i64)),
            ingest_mono: None,
        })]
    }

    pub fn summary(&self) -> Value {
        json!({
            "counters": self.stats,
            "sessions_seen": self.sessions_seen,
            "sides_seen": self.sides_seen,
            "message_shapes": self.shapes_seen,
        })
    }
}
