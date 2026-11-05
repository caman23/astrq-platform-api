//! Market data providers. A new data vendor = a new adapter implementing
//! `MarketDataProvider`, nothing else.

pub mod cumvol;
pub mod dnse;
pub mod recorded_baseline;
pub mod replay;
pub mod ssi;
pub mod synthetic;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

use crate::core::models::MarketEvent;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProviderStatus {
    pub connected: bool,
    pub last_event_at: Option<DateTime<Utc>>,
    pub reconnects: u64,
    pub finished: bool,
    pub detail: String,
    /// Vendor-specific diagnostics (shown in the System tab).
    pub stats: Value,
}

pub type StatusHandle = Arc<Mutex<ProviderStatus>>;

/// Callback receiving raw vendor messages (kept to verify units/codes against reality).
pub type RawSink = Arc<dyn Fn(&str, DateTime<Utc>) + Send + Sync>;

#[async_trait]
pub trait MarketDataProvider: Send {
    fn name(&self) -> &'static str;
    fn status(&self) -> StatusHandle;
    async fn connect(&mut self) -> Result<()>;
    async fn subscribe(&mut self, symbols: &[String]) -> Result<()>;
    /// Next normalized event. `Ok(None)` = the stream finished (replay end, simulated
    /// close); `Err` = connection problem (the platform reconnects with backoff).
    async fn next_event(&mut self) -> Result<Option<MarketEvent>>;
    async fn close(&mut self);
}

pub fn new_status(detail: &str) -> StatusHandle {
    Arc::new(Mutex::new(ProviderStatus {
        detail: detail.into(),
        stats: Value::Object(Default::default()),
        ..Default::default()
    }))
}
