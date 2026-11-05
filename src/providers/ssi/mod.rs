//! SSI FastConnect Data provider (realtime X-TRADE / X over SignalR).
//!
//! Per (re)connect, driven by the platform feed loop (which owns backoff): token (cached,
//! refreshed 1 h before expiry) -> SignalR negotiate + WS -> SwitchChannels -> normalized
//! events until the socket closes or stays silent too long during a session.

pub mod parse;
pub mod rest;
pub mod signalr;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use crate::core::config::SsiConfig;
use crate::core::models::MarketEvent;
use crate::core::sessions::SessionCalendar;
use crate::providers::{new_status, MarketDataProvider, RawSink, StatusHandle};
use parse::{decode_broadcast, SsiNormalizer};
use rest::SsiRestClient;
use signalr::{AuthError, SignalRClient};

pub struct SsiProvider {
    cfg: SsiConfig,
    calendar: Arc<SessionCalendar>,
    raw_sink: Option<RawSink>,
    status: StatusHandle,
    http: reqwest::Client,
    pub rest: SsiRestClient,
    pub normalizer: SsiNormalizer,
    sr: Option<SignalRClient>,
    symbols: Vec<String>,
    streaming: bool,
    pending: VecDeque<(String, Vec<Value>)>,
    buffer: VecDeque<MarketEvent>,
    closed: bool,
}

impl SsiProvider {
    pub fn new(
        cfg: SsiConfig,
        calendar: Arc<SessionCalendar>,
        raw_sink: Option<RawSink>,
        http: Option<reqwest::Client>,
    ) -> Self {
        let http = http.unwrap_or_else(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .expect("http client")
        });
        Self {
            rest: SsiRestClient::new(cfg.clone(), http.clone()),
            normalizer: SsiNormalizer::new(cfg.clone(), calendar.clone()),
            raw_sink: if cfg.record_raw { raw_sink } else { None },
            cfg,
            calendar,
            status: new_status("SSI FastConnect Data"),
            http,
            sr: None,
            symbols: vec![],
            streaming: false,
            pending: VecDeque::new(),
            buffer: VecDeque::new(),
            closed: false,
        }
    }

    pub fn channel_string(&self) -> String {
        if self.cfg.subscribe_all || self.symbols.is_empty() {
            format!("{}:ALL", self.cfg.channel)
        } else {
            format!("{}:{}", self.cfg.channel, self.symbols.join("-"))
        }
    }

    fn idle_limit(&self) -> Option<Duration> {
        let s = self.calendar.session_at(Utc::now());
        (s.is_continuous() || s.is_auction())
            .then(|| Duration::from_secs_f64(self.cfg.idle_reconnect_sec))
    }

    async fn drop_stream(&mut self) {
        self.streaming = false;
        if let Some(mut sr) = self.sr.take() {
            sr.close().await;
        }
        self.status.lock().connected = false;
    }
}

#[async_trait]
impl MarketDataProvider for SsiProvider {
    fn name(&self) -> &'static str {
        "ssi"
    }
    fn status(&self) -> StatusHandle {
        self.status.clone()
    }

    async fn connect(&mut self) -> Result<()> {
        self.drop_stream().await;
        let token = self.rest.token().await?;
        let mut sr = SignalRClient::new(&self.cfg.stream_url, &token, self.http.clone());
        if let Err(e) = sr.connect().await {
            if e.downcast_ref::<AuthError>().is_some() {
                self.rest.invalidate(); // next attempt fetches a fresh token
            }
            return Err(e);
        }
        self.sr = Some(sr);
        let mut st = self.status.lock();
        st.connected = true;
        st.detail = format!("SSI FastConnect Data · {}", self.cfg.channel);
        Ok(())
    }

    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        let mut s = symbols.to_vec();
        s.sort();
        s.dedup();
        self.symbols = s;
        if self.streaming {
            let ch = self.channel_string();
            if let Some(sr) = self.sr.as_mut() {
                sr.invoke("SwitchChannels", vec![Value::String(ch)]).await?; // universe changed mid-stream
            }
        }
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        if self.sr.is_none() {
            bail!("SSI stream not connected");
        }
        if !self.streaming {
            let ch = self.channel_string();
            self.sr
                .as_mut()
                .expect("sr")
                .invoke("SwitchChannels", vec![Value::String(ch)])
                .await?;
            self.streaming = true;
        }
        loop {
            if self.closed {
                return Ok(None);
            }
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            let limit = self.idle_limit();
            let mut pending = std::mem::take(&mut self.pending);
            let sr = self.sr.as_mut().expect("sr");
            let call = match limit {
                Some(l) => tokio::time::timeout(l, sr.next_call(&mut pending))
                    .await
                    .map_err(|_| anyhow!("SSI stream silent during a session"))??,
                None => sr.next_call(&mut pending).await?,
            };
            self.pending = pending;
            let Some((method, args)) = call else {
                self.status.lock().connected = false;
                bail!("SSI stream closed by server");
            };
            if method == "Error" {
                tracing::warn!(
                    "SSI hub error: {}",
                    serde_json::Value::Array(args)
                        .to_string()
                        .chars()
                        .take(300)
                        .collect::<String>()
                );
                *self
                    .normalizer
                    .stats
                    .entry("hub_errors".into())
                    .or_default() += 1;
                continue;
            }
            if method != "Broadcast" {
                continue;
            }
            let received = Utc::now();
            for arg in &args {
                if let Some(sink) = &self.raw_sink {
                    sink(
                        arg.as_str()
                            .map(String::from)
                            .unwrap_or_else(|| arg.to_string())
                            .as_str(),
                        received,
                    );
                }
                let Some((dtype, content)) = decode_broadcast(arg) else {
                    *self
                        .normalizer
                        .stats
                        .entry("undecodable".into())
                        .or_default() += 1;
                    continue;
                };
                let evs = self.normalizer.events(&dtype, &content, received);
                if !evs.is_empty() {
                    self.status.lock().last_event_at = Some(received);
                }
                self.buffer.extend(evs);
            }
            self.status.lock().stats = self.normalizer.summary();
        }
    }

    async fn close(&mut self) {
        self.closed = true;
        self.drop_stream().await;
    }
}
