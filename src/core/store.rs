//! In-memory store of today's signals (the dashboard's source of truth).

use chrono::NaiveDate;
use std::collections::VecDeque;

use crate::core::models::{Severity, Signal};
use crate::core::sessions::SessionCalendar;

pub struct SignalStore {
    calendar: SessionCalendar,
    max_signals: usize,
    pub trading_date: Option<NaiveDate>,
    pub signals: VecDeque<Signal>,
}

impl SignalStore {
    pub fn new(calendar: SessionCalendar) -> Self {
        Self {
            calendar,
            max_signals: 50_000,
            trading_date: None,
            signals: VecDeque::new(),
        }
    }

    pub fn add(&mut self, s: Signal) {
        let d = self.calendar.trading_date(s.ts);
        match self.trading_date {
            Some(cur) if d < cur => return, // stale signal from a previous day
            Some(cur) if d == cur => {}
            _ => {
                self.trading_date = Some(d);
                self.signals.clear();
            }
        }
        if self.signals.len() == self.max_signals {
            self.signals.pop_front();
        }
        self.signals.push_back(s);
    }

    pub fn by_tool(&self, tool_id: &str) -> Vec<Signal> {
        self.signals
            .iter()
            .filter(|s| s.tool_id == tool_id)
            .cloned()
            .collect()
    }

    pub fn query(
        &self,
        tool_id: Option<&str>,
        symbol: Option<&str>,
        side: Option<&str>,
        min_severity: Option<Severity>,
        limit: usize,
    ) -> Vec<Signal> {
        self.signals
            .iter()
            .rev()
            .filter(|s| tool_id.is_none_or(|t| s.tool_id == t))
            .filter(|s| symbol.is_none_or(|x| s.symbol == x))
            .filter(|s| side.is_none_or(|x| s.side.as_str() == x))
            .filter(|s| min_severity.is_none_or(|m| s.severity.rank() >= m.rank()))
            .take(limit)
            .cloned()
            .collect()
    }
}
