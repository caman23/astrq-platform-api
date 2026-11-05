//! Context/baseline service: non-realtime reference data for tools.
//!
//! The hot path never performs I/O: tools call `ContextService::baseline()` which only
//! reads the in-memory cache, warmed by the scheduler before the session.

use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDate;
use futures_util::{stream, StreamExt};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::core::metrics::METRICS;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Baseline {
    pub symbol: String,
    pub trading_date: NaiveDate,
    /// VND, typical single-trade value over the last N sessions.
    pub median_trade_value: f64,
    pub p95_trade_value: f64,
    pub avg_daily_value: f64,
    /// "high" | "mid" | "low"
    pub liquidity_tier: String,
    pub source: String,
}

#[async_trait]
pub trait ContextProvider: Send + Sync {
    fn name(&self) -> &str;
    async fn baseline(&self, symbol: &str, trading_date: NaiveDate) -> Result<Option<Baseline>>;
}

pub struct NullContextProvider;

#[async_trait]
impl ContextProvider for NullContextProvider {
    fn name(&self) -> &str {
        "none"
    }
    async fn baseline(&self, _symbol: &str, _d: NaiveDate) -> Result<Option<Baseline>> {
        Ok(None)
    }
}

pub struct ContextService {
    pub provider: Arc<dyn ContextProvider>,
    max_retries: u32,
    backoff: Duration,
    cache: RwLock<HashMap<(String, NaiveDate), Option<Baseline>>>,
    pub last_warm_error: Mutex<Option<String>>,
}

impl ContextService {
    pub fn new(provider: Arc<dyn ContextProvider>) -> Self {
        Self {
            provider,
            max_retries: 3,
            backoff: Duration::from_millis(500),
            cache: RwLock::new(HashMap::new()),
            last_warm_error: Mutex::new(None),
        }
    }

    /// Hot-path read. `None` when unknown (caller marks `baseline_cold`).
    pub fn baseline(&self, symbol: &str, d: NaiveDate) -> Option<Baseline> {
        self.cache
            .read()
            .get(&(symbol.to_string(), d))
            .cloned()
            .flatten()
    }

    async fn fetch(&self, symbol: &str, d: NaiveDate) -> Option<Baseline> {
        let mut delay = self.backoff;
        for attempt in 1..=self.max_retries {
            match self.provider.baseline(symbol, d).await {
                Ok(b) => return b,
                Err(e) => {
                    *self.last_warm_error.lock() = Some(format!("{e:#}"));
                    METRICS.inc(
                        "context_errors_total",
                        1.0,
                        &[("provider", self.provider.name())],
                    );
                    if attempt == self.max_retries {
                        tracing::warn!("baseline fetch failed for {symbol}: {e:#}");
                        return None;
                    }
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
        None
    }

    pub async fn warm(&self, symbols: &[String], d: NaiveDate) -> usize {
        let results: Vec<(String, Option<Baseline>)> = stream::iter(symbols.iter().cloned())
            .map(|s| async move {
                let b = self.fetch(&s, d).await;
                (s, b)
            })
            .buffer_unordered(8)
            .collect()
            .await;
        let ok = results.iter().filter(|(_, b)| b.is_some()).count();
        {
            let mut cache = self.cache.write();
            for (s, b) in results {
                cache.insert((s, d), b);
            }
        }
        METRICS.set("baseline_warm_symbols", ok as f64, &[]);
        tracing::info!("baseline warm-up for {d}: {ok} symbols ok");
        ok
    }

    pub fn coverage(&self, symbols: &[String], d: NaiveDate) -> f64 {
        if symbols.is_empty() {
            return 0.0;
        }
        let hits = symbols
            .iter()
            .filter(|s| self.baseline(s, d).is_some())
            .count();
        hits as f64 / symbols.len() as f64
    }
}
