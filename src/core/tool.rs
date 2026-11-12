//! Plugin contract. A tool only sees bus events and `ToolContext`; it never talks to a
//! data provider directly. Adding a tool must not require changing the core.

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::clock::Clock;
use crate::core::context::ContextService;
use crate::core::market_rules::MarketRules;
use crate::core::models::{MarketEvent, Signal};
use crate::core::sessions::SessionCalendar;
use crate::core::universe::Universe;

#[derive(Debug, Clone)]
pub struct Subscription {
    pub event_types: HashSet<&'static str>,
    /// `None` = whole current universe.
    pub symbols: Option<HashSet<String>>,
    pub include_negotiated: bool,
}

impl Subscription {
    pub fn new(event_types: &[&'static str]) -> Self {
        Self {
            event_types: event_types.iter().copied().collect(),
            symbols: None,
            include_negotiated: true,
        }
    }

    pub fn matches(&self, ev: &MarketEvent, universe: &Universe) -> bool {
        if !self.event_types.contains(ev.event_type()) {
            return false;
        }
        let in_scope = match &self.symbols {
            None => universe.contains(ev.symbol()),
            Some(s) => s.contains(ev.symbol()),
        };
        if !in_scope {
            return false;
        }
        !(!self.include_negotiated && ev.as_tick().is_some_and(|t| t.is_negotiated))
    }

    pub fn describe(&self) -> Value {
        let mut types: Vec<&str> = self.event_types.iter().copied().collect();
        types.sort();
        let symbols = match &self.symbols {
            None => Value::String("universe".into()),
            Some(s) => {
                let mut v: Vec<&String> = s.iter().collect();
                v.sort();
                serde_json::json!(v)
            }
        };
        serde_json::json!({"event_types": types, "symbols": symbols, "include_negotiated": self.include_negotiated})
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolManifest {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub subscriptions: Vec<Value>,
    pub config_schema: Value,
    pub signal_kinds: Vec<String>,
    pub has_view: bool,
}

#[derive(Clone)]
pub struct ToolContext {
    pub tool_id: String,
    pub universe: Arc<RwLock<Universe>>,
    pub context: Arc<ContextService>,
    pub calendar: Arc<SessionCalendar>,
    pub rules: Arc<MarketRules>,
    pub clock: Arc<dyn Clock>,
}

pub trait Tool: Send {
    fn id(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn manifest(&self) -> ToolManifest;
    /// Effective (validated) configuration, for the dashboard.
    fn config_json(&self) -> Value;
    fn subscriptions(&self) -> Vec<Subscription>;
    fn on_start(&mut self, _ctx: &ToolContext) {}
    /// Must be fast and never block on I/O.
    fn on_event(&mut self, ev: &MarketEvent, ctx: &ToolContext) -> Result<Vec<Signal>>;
    fn on_timer(&mut self, _now: DateTime<Utc>, _ctx: &ToolContext) -> Result<Vec<Signal>> {
        Ok(vec![])
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        None
    }
    fn restore(&mut self, _data: &[u8]) -> Result<()> {
        Ok(())
    }
    /// Optional aggregated view over this tool's signals (dashboard).
    fn view(
        &self,
        _signals: &[Arc<Signal>],
        _ctx: &ToolContext,
        _params: &HashMap<String, String>,
    ) -> Option<Value> {
        None
    }
    /// Optional end-of-session report (Vietnamese).
    fn daily_summary(&self, _ctx: &ToolContext) -> Option<String> {
        None
    }
    /// Trading date the tool's in-memory state belongs to (None = unknown / stateless).
    /// Snapshots are labelled with it, so yesterday's state saved after midnight is not
    /// mistaken for today's.
    fn state_date(&self) -> Option<NaiveDate> {
        None
    }
    /// Compact end-of-day state kept per trading date, so a past session can still be
    /// viewed after the tool has moved on (None = the tool keeps no archive).
    fn archive(&self) -> Option<(NaiveDate, Vec<u8>)> {
        None
    }
    /// View of a past session from its archive (if one was saved) and that day's signals.
    fn archive_view(
        &self,
        _archive: Option<&[u8]>,
        _signals: &[Arc<Signal>],
        _date: NaiveDate,
        _ctx: &ToolContext,
        _params: &HashMap<String, String>,
    ) -> Option<Value> {
        None
    }
}
