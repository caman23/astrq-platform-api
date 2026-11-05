//! Canonical data model shared by providers, the bus, tools and the API.
//!
//! All timestamps are UTC. JSON field names match the original Python service so
//! recorded files, the dashboard and the REST/WebSocket contract stay compatible.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
    #[default]
    Unknown,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
            Side::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SideSource {
    Provider,
    QuoteRule,
    TickRule,
    #[default]
    Unknown,
}

impl SideSource {
    pub fn as_str(self) -> &'static str {
        match self {
            SideSource::Provider => "provider",
            SideSource::QuoteRule => "quote_rule",
            SideSource::TickRule => "tick_rule",
            SideSource::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Session {
    PreOpen,
    Ato,
    ContinuousAm,
    Lunch,
    ContinuousPm,
    Atc,
    PutThrough,
    #[default]
    Closed,
}

impl Session {
    pub fn is_continuous(self) -> bool {
        matches!(self, Session::ContinuousAm | Session::ContinuousPm)
    }
    pub fn is_auction(self) -> bool {
        matches!(self, Session::Ato | Session::Atc)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Session::PreOpen => "pre_open",
            Session::Ato => "ato",
            Session::ContinuousAm => "continuous_am",
            Session::Lunch => "lunch",
            Session::ContinuousPm => "continuous_pm",
            Session::Atc => "atc",
            Session::PutThrough => "put_through",
            Session::Closed => "closed",
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Default,
    PartialOrd,
    Ord,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    #[default]
    Info,
    Notice,
    Alert,
}

impl Severity {
    pub fn rank(self) -> u8 {
        self as u8
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Notice => "notice",
            Severity::Alert => "alert",
        }
    }
    pub fn parse(s: &str) -> Option<Severity> {
        match s {
            "info" => Some(Severity::Info),
            "notice" => Some(Severity::Notice),
            "alert" => Some(Severity::Alert),
            _ => None,
        }
    }
}

fn default_exchange() -> String {
    "HOSE".into()
}
fn default_lot() -> i64 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Instrument {
    pub symbol: String,
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default = "default_lot")]
    pub lot_size: i64,
    #[serde(default)]
    pub sector: Option<String>,
}

impl Instrument {
    pub fn new(symbol: &str, sector: Option<&str>) -> Self {
        Self {
            symbol: symbol.into(),
            exchange: default_exchange(),
            lot_size: 100,
            sector: sector.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub provider: String,
    pub symbol: String,
    pub ts_exchange: DateTime<Utc>,
    pub ts_received: DateTime<Utc>,
    pub price: f64,
    pub volume: i64,
    pub value: f64,
    #[serde(default)]
    pub side: Side,
    #[serde(default)]
    pub side_source: SideSource,
    #[serde(default)]
    pub session: Session,
    #[serde(default)]
    pub is_negotiated: bool,
    #[serde(default)]
    pub seq: Option<i64>,
    #[serde(default)]
    pub raw_ref: Option<String>,
    /// Monotonic reading taken at ingest; only used to measure internal latency.
    #[serde(skip)]
    pub ingest_mono: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub provider: String,
    pub symbol: String,
    pub ts_exchange: DateTime<Utc>,
    pub ts_received: DateTime<Utc>,
    #[serde(default)]
    pub best_bid: Option<f64>,
    #[serde(default)]
    pub best_ask: Option<f64>,
    #[serde(default)]
    pub bid_size: Option<i64>,
    #[serde(default)]
    pub ask_size: Option<i64>,
    #[serde(skip)]
    pub ingest_mono: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedPrice {
    pub provider: String,
    pub symbol: String,
    pub ts_exchange: DateTime<Utc>,
    pub ts_received: DateTime<Utc>,
    pub session: Session,
    pub price: f64,
    #[serde(default)]
    pub volume: Option<i64>,
    #[serde(skip)]
    pub ingest_mono: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignFlow {
    pub provider: String,
    pub symbol: String,
    pub ts_exchange: DateTime<Utc>,
    pub ts_received: DateTime<Utc>,
    pub buy_value: f64,
    pub sell_value: f64,
    pub net_value: f64,
    #[serde(skip)]
    pub ingest_mono: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum MarketEvent {
    Tick(Tick),
    Quote(Quote),
    ExpectedPrice(ExpectedPrice),
    ForeignFlow(ForeignFlow),
}

macro_rules! each {
    ($self:expr, $e:ident => $body:expr) => {
        match $self {
            MarketEvent::Tick($e) => $body,
            MarketEvent::Quote($e) => $body,
            MarketEvent::ExpectedPrice($e) => $body,
            MarketEvent::ForeignFlow($e) => $body,
        }
    };
}

impl MarketEvent {
    pub fn symbol(&self) -> &str {
        each!(self, e => &e.symbol)
    }
    pub fn provider(&self) -> &str {
        each!(self, e => &e.provider)
    }
    pub fn ts_exchange(&self) -> DateTime<Utc> {
        each!(self, e => e.ts_exchange)
    }
    pub fn ts_received(&self) -> DateTime<Utc> {
        each!(self, e => e.ts_received)
    }
    pub fn ingest_mono(&self) -> Option<Instant> {
        each!(self, e => e.ingest_mono)
    }
    pub fn set_ingest_mono(&mut self, t: Instant) {
        each!(self, e => e.ingest_mono = Some(t))
    }
    pub fn event_type(&self) -> &'static str {
        match self {
            MarketEvent::Tick(_) => "tick",
            MarketEvent::Quote(_) => "quote",
            MarketEvent::ExpectedPrice(_) => "expected_price",
            MarketEvent::ForeignFlow(_) => "foreign_flow",
        }
    }
    pub fn as_tick(&self) -> Option<&Tick> {
        match self {
            MarketEvent::Tick(t) => Some(t),
            _ => None,
        }
    }
}

/// One factor's share of a signal's score; makes every score auditable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Contribution {
    pub factor: String,
    pub label: String,
    pub raw: f64,
    pub normalized: f64,
    pub weight: f64,
    pub contribution: f64,
    #[serde(default)]
    pub note: String,
}

fn default_state() -> String {
    "single".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub signal_id: String,
    pub tool_id: String,
    pub tool_version: String,
    pub symbol: String,
    pub ts: DateTime<Utc>,
    pub kind: String,
    #[serde(default)]
    pub side: Side,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub severity: Severity,
    #[serde(default = "default_state")]
    pub state: String,
    #[serde(default)]
    pub features: Map<String, Value>,
    #[serde(default)]
    pub explanation: Vec<Contribution>,
    #[serde(default)]
    pub quality_flags: Vec<String>,
    pub dedupe_key: String,
    pub correlation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub signal_id: String,
    pub channel: String,
    pub rendered_text: String,
    pub sent_at: DateTime<Utc>,
    pub status: String,
}
