//! Replay recorded events (JSONL, one canonical MarketEvent per line). Files written by
//! the Python service are read as-is.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use crate::core::models::MarketEvent;
use crate::providers::{new_status, MarketDataProvider, StatusHandle};

pub fn read_events(path: &Path) -> Result<Vec<MarketEvent>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            serde_json::from_str(l).with_context(|| format!("{}:{}", path.display(), i + 1))
        })
        .collect()
}

pub struct ReplayProvider {
    path: PathBuf,
    speed: f64,
    status: StatusHandle,
    symbols: Option<HashSet<String>>,
    lines: Option<Lines<BufReader<tokio::fs::File>>>,
    first_ts: Option<DateTime<Utc>>,
    wall0: Instant,
    closed: bool,
}

impl ReplayProvider {
    pub fn new(path: PathBuf, speed: f64) -> Self {
        Self {
            status: new_status(&format!(
                "replay {} speed={speed}x",
                path.file_name()
                    .map(|n| n.to_string_lossy())
                    .unwrap_or_default()
            )),
            path,
            speed,
            symbols: None,
            lines: None,
            first_ts: None,
            wall0: Instant::now(),
            closed: false,
        }
    }
}

#[async_trait]
impl MarketDataProvider for ReplayProvider {
    fn name(&self) -> &'static str {
        "replay"
    }
    fn status(&self) -> StatusHandle {
        self.status.clone()
    }
    async fn connect(&mut self) -> Result<()> {
        if !self.path.exists() {
            bail!("replay file not found: {}", self.path.display());
        }
        if self.lines.is_none() {
            let f = tokio::fs::File::open(&self.path).await?;
            self.lines = Some(BufReader::new(f).lines());
            self.wall0 = Instant::now();
        }
        self.status.lock().connected = true;
        Ok(())
    }
    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.symbols = Some(symbols.iter().cloned().collect());
        Ok(())
    }
    async fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        loop {
            if self.closed {
                return Ok(None);
            }
            let Some(lines) = self.lines.as_mut() else {
                bail!("replay not connected")
            };
            let Some(line) = lines.next_line().await? else {
                self.status.lock().finished = true;
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let ev: MarketEvent = serde_json::from_str(&line).context("bad replay line")?;
            if self
                .symbols
                .as_ref()
                .is_some_and(|s| !s.contains(ev.symbol()))
            {
                continue;
            }
            if self.speed > 0.0 {
                let first = *self.first_ts.get_or_insert(ev.ts_exchange());
                let due = (ev.ts_exchange() - first).num_microseconds().unwrap_or(0) as f64
                    / 1e6
                    / self.speed;
                let wait = due - self.wall0.elapsed().as_secs_f64();
                if wait > 0.0 {
                    tokio::time::sleep(std::time::Duration::from_secs_f64(wait)).await;
                }
            }
            self.status.lock().last_event_at = Some(ev.ts_received());
            return Ok(Some(ev));
        }
    }
    async fn close(&mut self) {
        self.closed = true;
        self.status.lock().connected = false;
    }
}
