//! DNSE LightSpeed API market data provider (WebSocket, JSON encoding).
//!
//! Protocol (official docs, sdk/build_websocket):
//!   connect `{ws_url}?encoding=json` -> server "welcome"
//!   -> `{"action":"auth","api_key","signature","timestamp","nonce"}` within 30 s, where
//!      signature = hex(HMAC-SHA256(api_secret, "{api_key}:{timestamp}:{nonce}")),
//!      timestamp = unix seconds (±5 min), nonce = unix microseconds (never reused)
//!   -> "auth_success" | `{"action":"error","code":"AUTH_FAILED"|...}` then close
//!   -> `{"action":"subscribe","channels":[{"name":"tick_extra.G1.json","symbols":[...]}]}`
//!   server `{"action":"ping","timestamp"}` -> reply `{"action":"pong","timestamp"}`;
//!   connections last at most 8 h ("connection_expired") -> reconnect with a NEW nonce.
//! Streams are counted per channel x symbol (verified live: "MAX_CHANNELS_EXCEEDED:
//! max 200 channels for tier normalUser"; `auth_success.rate_limit` may report less).

pub mod parse;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::core::config::DnseConfig;
use crate::core::models::MarketEvent;
use crate::core::sessions::SessionCalendar;
use crate::providers::{new_status, MarketDataProvider, RawSink, StatusHandle};
use parse::DnseNormalizer;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub fn build_auth_message(api_key: &str, api_secret: &str, now: f64) -> Value {
    let timestamp = now as i64;
    let nonce = ((now * 1_000_000.0) as i64).to_string();
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).expect("hmac key");
    mac.update(format!("{api_key}:{timestamp}:{nonce}").as_bytes());
    json!({
        "action": "auth",
        "api_key": api_key,
        "signature": hex::encode(mac.finalize().into_bytes()),
        "timestamp": timestamp,
        "nonce": nonce,
    })
}

pub struct DnseProvider {
    cfg: DnseConfig,
    calendar: Arc<SessionCalendar>,
    raw_sink: Option<RawSink>,
    status: StatusHandle,
    pub normalizer: DnseNormalizer,
    ws: Option<Ws>,
    symbols: Vec<String>,
    streaming: bool,
    server_limits: Value,
    active: Vec<Value>,
    coverage: serde_json::Map<String, Value>,
    buffer: VecDeque<MarketEvent>,
    closed: bool,
    stats_at: std::time::Instant,
}

impl DnseProvider {
    pub fn new(cfg: DnseConfig, calendar: Arc<SessionCalendar>, raw_sink: Option<RawSink>) -> Self {
        let raw_sink = if cfg.record_raw { raw_sink } else { None };
        Self {
            normalizer: DnseNormalizer::new(cfg.clone(), calendar.clone()),
            cfg,
            calendar,
            raw_sink,
            status: new_status("DNSE LightSpeed API"),
            ws: None,
            symbols: vec![],
            streaming: false,
            server_limits: json!({}),
            active: vec![],
            coverage: serde_json::Map::new(),
            buffer: VecDeque::new(),
            closed: false,
            stats_at: std::time::Instant::now(),
        }
    }

    async fn recv_json(&mut self, timeout: Duration) -> Result<Value> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow!("not connected"))?;
        loop {
            let msg = tokio::time::timeout(timeout, ws.next())
                .await
                .map_err(|_| anyhow!("DNSE: no reply"))?;
            match msg {
                Some(Ok(Message::Text(t))) => {
                    return Ok(serde_json::from_str(&t).unwrap_or(Value::Null))
                }
                Some(Ok(Message::Binary(b))) => {
                    return Ok(serde_json::from_slice(&b).unwrap_or(Value::Null))
                }
                Some(Ok(Message::Close(_))) | None => bail!("DNSE closed the connection"),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e.into()),
            }
        }
    }

    async fn send(&mut self, v: &Value) -> Result<()> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow!("not connected"))?;
        ws.send(Message::Text(v.to_string().into())).await?;
        Ok(())
    }

    fn stream_budget(&self) -> usize {
        match self.server_limits["rate_limit"]["subscriptions_max"].as_u64() {
            Some(n) if n > 0 => self.cfg.max_streams.min(n as usize),
            _ => self.cfg.max_streams,
        }
    }

    /// One subscribe message per channel, in priority order, within the stream budget
    /// (a rejected message subscribes nothing, so channels are sent separately).
    pub fn plan_subscriptions(&mut self) -> Vec<Value> {
        let mut budget = self.stream_budget();
        let mut plan = Vec::new();
        self.coverage.clear();
        for ch in &self.cfg.channels {
            let take: Vec<String> = self.symbols.iter().take(budget).cloned().collect();
            budget -= take.len();
            self.coverage.insert(
                ch.clone(),
                json!(format!("{}/{}", take.len(), self.symbols.len())),
            );
            if take.len() < self.symbols.len() {
                tracing::warn!(
                    "DNSE stream budget: {ch} covers {}/{} symbols",
                    take.len(),
                    self.symbols.len()
                );
            }
            if !take.is_empty() {
                plan.push(json!({"action": "subscribe", "channels": [{"name": format!("{ch}.json"), "symbols": take}]}));
            }
        }
        plan
    }

    async fn send_plan(&mut self) -> Result<()> {
        for msg in std::mem::take(&mut self.active) {
            let mut un = msg.clone();
            un["action"] = json!("unsubscribe");
            self.send(&un).await?;
        }
        let plan = self.plan_subscriptions();
        for m in &plan {
            self.send(m).await?;
        }
        self.active = plan;
        Ok(())
    }

    fn idle_limit(&self) -> Option<Duration> {
        let s = self.calendar.session_at(Utc::now());
        (s.is_continuous() || s.is_auction())
            .then(|| Duration::from_secs_f64(self.cfg.idle_reconnect_sec))
    }

    fn refresh_stats(&self) {
        let mut st = self.normalizer.summary();
        st["subscriptions"] = Value::Object(self.coverage.clone());
        st["server"] = self.server_limits.clone();
        self.status.lock().stats = st;
    }

    async fn control(&mut self, msg: &Value) -> Result<()> {
        match msg["action"].as_str().unwrap_or("") {
            "ping" => {
                let mut pong = json!({"action": "pong"});
                if let Some(ts) = msg.get("timestamp") {
                    pong["timestamp"] = ts.clone();
                }
                self.send(&pong).await?;
            }
            "subscribed" => {
                *self
                    .normalizer
                    .stats
                    .entry("subscribe_acks".into())
                    .or_default() += 1
            }
            "connection_expired" => bail!("DNSE connection expired (8h limit)"),
            "error" => {
                let code = msg["code"].as_str().unwrap_or("?").to_string();
                *self
                    .normalizer
                    .stats
                    .entry(format!("server_error_{code}"))
                    .or_default() += 1;
                tracing::warn!("DNSE error {code}: {}", msg["message"]);
            }
            _ => {}
        }
        Ok(())
    }

    async fn drop_ws(&mut self) {
        self.streaming = false;
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.close(None).await;
        }
        self.status.lock().connected = false;
    }
}

#[async_trait]
impl MarketDataProvider for DnseProvider {
    fn name(&self) -> &'static str {
        "dnse"
    }
    fn status(&self) -> StatusHandle {
        self.status.clone()
    }

    async fn connect(&mut self) -> Result<()> {
        self.drop_ws().await;
        let (Some(key), Some(secret)) = (self.cfg.api_key.clone(), self.cfg.api_secret.clone())
        else {
            bail!("missing DNSE api_key/api_secret");
        };
        // build the URL properly (a base URL without a path must still get "/?encoding=json")
        let mut url =
            reqwest::Url::parse(&self.cfg.ws_url).map_err(|e| anyhow!("bad DNSE ws_url: {e}"))?;
        url.query_pairs_mut().append_pair("encoding", "json");
        let (ws, _) = tokio::time::timeout(Duration::from_secs(20), connect_async(url.as_str()))
            .await
            .map_err(|_| anyhow!("DNSE connect timeout"))??;
        self.ws = Some(ws);
        let welcome = self.recv_json(Duration::from_secs(15)).await?;
        if welcome["action"] != "welcome" {
            tracing::warn!(
                "DNSE: unexpected first message {}",
                welcome.to_string().chars().take(200).collect::<String>()
            );
        }
        let now = Utc::now().timestamp_micros() as f64 / 1e6;
        self.send(&build_auth_message(&key, secret.expose(), now))
            .await?;
        let resp = self.recv_json(Duration::from_secs(15)).await?;
        if resp["action"] == "auth_success" {
            self.server_limits = json!({"capabilities": resp.get("capabilities"), "rate_limit": resp.get("rate_limit")});
        } else {
            let code = resp["code"].as_str().unwrap_or("?").to_string();
            let message = resp["message"].clone();
            self.drop_ws().await;
            if code == "AUTH_FAILED" {
                bail!("DNSE auth failed: {message}");
            }
            bail!("DNSE auth error {code}: {message}");
        }
        let mut st = self.status.lock();
        st.connected = true;
        st.detail = format!("DNSE LightSpeed API · {}", self.cfg.channels.join(", "));
        Ok(())
    }

    async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        let mut s: Vec<String> = symbols.iter().map(|x| x.to_ascii_uppercase()).collect();
        s.sort();
        s.dedup();
        self.symbols = s;
        if self.ws.is_some() && self.streaming {
            self.send_plan().await?; // universe changed mid-stream
        }
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        if self.ws.is_none() {
            bail!("DNSE stream not connected");
        }
        if !self.streaming {
            self.active.clear(); // fresh connection: nothing subscribed yet
            self.send_plan().await?;
            self.streaming = true;
            self.refresh_stats();
        }
        loop {
            if self.closed {
                return Ok(None);
            }
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            let limit = self.idle_limit();
            let ws = self.ws.as_mut().expect("connected");
            let next = match limit {
                Some(l) => tokio::time::timeout(l, ws.next())
                    .await
                    .map_err(|_| anyhow!("DNSE stream silent during a session"))?,
                None => ws.next().await,
            };
            let text = match next {
                Some(Ok(Message::Text(t))) => t.to_string(),
                Some(Ok(Message::Binary(b))) => String::from_utf8_lossy(&b).into_owned(),
                Some(Ok(Message::Close(_))) | None => {
                    self.status.lock().connected = false;
                    bail!("DNSE stream closed");
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    self.status.lock().connected = false;
                    bail!("DNSE stream closed: {e}");
                }
            };
            let Ok(msg) = serde_json::from_str::<Value>(&text) else {
                *self
                    .normalizer
                    .stats
                    .entry("undecodable".into())
                    .or_default() += 1;
                continue;
            };
            if !msg.is_object() {
                continue;
            }
            if msg.get("action").is_some() {
                self.control(&msg).await?;
                continue;
            }
            let received = Utc::now();
            if let Some(sink) = &self.raw_sink {
                sink(&text, received);
            }
            let events = self.normalizer.events(&msg, received);
            if !events.is_empty() {
                self.status.lock().last_event_at = Some(received);
            }
            self.buffer.extend(events);
            if self.stats_at.elapsed() >= Duration::from_secs(1) {
                self.stats_at = std::time::Instant::now();
                self.refresh_stats(); // diagnostics only: at most once per second
            }
        }
    }

    async fn close(&mut self) {
        self.closed = true;
        self.drop_ws().await;
    }
}
