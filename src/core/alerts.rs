//! Alert router: dedupe, per (symbol, side) cooldown with coalescing, per-channel rate
//! limit, severity/tool filters. Channels: dashboard (WebSocket) and Telegram.

use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::core::fmt;
use crate::core::metrics::METRICS;
use crate::core::models::{Alert, Severity, Signal};

#[async_trait]
pub trait Channel: Send + Sync {
    fn name(&self) -> &str;
    fn min_severity(&self) -> Severity;
    fn max_per_minute(&self) -> usize;
    /// `None` = all tools.
    fn enabled_tools(&self) -> Option<HashSet<String>> {
        None
    }
    async fn send(&self, text: &str, signal: Option<&Signal>) -> Result<()>;
}

pub fn render_signal(s: &Signal, merged: u64) -> String {
    let icon = match s.side.as_str() {
        "buy" => "🟢",
        "sell" => "🔴",
        _ => "🟡",
    };
    let mut lines = vec![format!(
        "{icon} {} LỚN · {} · {} {:.0}",
        fmt::side_vi(s.side.as_str()),
        s.symbol,
        fmt::severity_vi(s.severity.as_str()),
        s.score
    )];
    if let Some(Value::Array(summary)) = s.features.get("summary_vi") {
        lines.extend(summary.iter().filter_map(|v| v.as_str().map(String::from)));
    }
    if !s.quality_flags.is_empty() {
        lines.push(format!(
            "Cờ: {}",
            s.quality_flags
                .iter()
                .map(|f| fmt::flag_vi(f))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if merged > 0 {
        lines.push(format!("(+{merged} cập nhật cùng mã đã gộp)"));
    }
    lines.push(format!("Lúc {} (giờ VN)", fmt::hms(s.ts)));
    lines.push(fmt::DISCLAIMER_VI.to_string());
    lines.join("\n")
}

#[derive(Default)]
struct ChannelState {
    sent_keys: HashSet<String>,
    cooldown: HashMap<(String, String), (chrono::DateTime<Utc>, u8)>,
    merged: HashMap<(String, String), u64>,
    sent_wall: VecDeque<f64>,
}

type ToolEnabled = Arc<dyn Fn(&str) -> bool + Send + Sync>;
type Wall = Arc<dyn Fn() -> f64 + Send + Sync>;

pub struct AlertRouter {
    pub channels: Vec<Arc<dyn Channel>>,
    cooldown_sec: f64,
    tool_enabled: ToolEnabled,
    wall: Wall,
    state: Mutex<HashMap<String, ChannelState>>,
    pub history: Mutex<VecDeque<Alert>>,
}

impl AlertRouter {
    pub fn new(
        channels: Vec<Arc<dyn Channel>>,
        cooldown_sec: f64,
        tool_enabled: ToolEnabled,
        wall: Option<Wall>,
    ) -> Self {
        let t0 = Instant::now();
        Self {
            channels,
            cooldown_sec,
            tool_enabled,
            wall: wall.unwrap_or_else(|| Arc::new(move || t0.elapsed().as_secs_f64())),
            state: Mutex::new(HashMap::new()),
            history: Mutex::new(VecDeque::new()),
        }
    }

    /// Pure decision (plus state update when "send"). Returns the reason.
    pub fn decide(&self, s: &Signal, ch: &dyn Channel) -> &'static str {
        if !(self.tool_enabled)(&s.tool_id) {
            return "tool_disabled";
        }
        if ch.enabled_tools().is_some_and(|t| !t.contains(&s.tool_id)) {
            return "tool_filtered";
        }
        if s.severity.rank() < ch.min_severity().rank() {
            return "below_severity";
        }
        let mut all = self.state.lock();
        let st = all.entry(ch.name().to_string()).or_default();
        if st.sent_keys.contains(&s.dedupe_key) {
            return "duplicate";
        }
        let ck = (s.symbol.clone(), s.side.as_str().to_string());
        if let Some((last_ts, last_rank)) = st.cooldown.get(&ck).copied() {
            let within = ((s.ts - last_ts).num_microseconds().unwrap_or(i64::MAX) as f64 / 1e6)
                < self.cooldown_sec;
            if within && s.severity.rank() <= last_rank {
                *st.merged.entry(ck).or_default() += 1;
                return "cooldown";
            }
        }
        let now = (self.wall)();
        while st.sent_wall.front().is_some_and(|t| now - t > 60.0) {
            st.sent_wall.pop_front();
        }
        if st.sent_wall.len() >= ch.max_per_minute() {
            *st.merged.entry(ck).or_default() += 1;
            return "rate_limited";
        }
        st.sent_wall.push_back(now);
        st.sent_keys.insert(s.dedupe_key.clone());
        st.cooldown.insert(ck, (s.ts, s.severity.rank()));
        "send"
    }

    pub async fn handle(&self, s: &Signal) {
        for ch in &self.channels {
            let reason = self.decide(s, ch.as_ref());
            if reason != "send" {
                METRICS.inc(
                    "alerts_suppressed_total",
                    1.0,
                    &[("channel", ch.name()), ("reason", reason)],
                );
                continue;
            }
            let merged = self
                .state
                .lock()
                .get_mut(ch.name())
                .and_then(|st| {
                    st.merged
                        .remove(&(s.symbol.clone(), s.side.as_str().to_string()))
                })
                .unwrap_or(0);
            let text = render_signal(s, merged);
            let status = match ch.send(&text, Some(s)).await {
                Ok(()) => "sent",
                Err(e) => {
                    tracing::warn!("alert channel {} failed: {e}", ch.name());
                    "failed"
                }
            };
            METRICS.inc(
                "alerts_total",
                1.0,
                &[("channel", ch.name()), ("status", status)],
            );
            let mut h = self.history.lock();
            if h.len() == 500 {
                h.pop_front();
            }
            h.push_back(Alert {
                signal_id: s.signal_id.clone(),
                channel: ch.name().into(),
                rendered_text: text,
                sent_at: Utc::now(),
                status: status.into(),
            });
        }
    }

    pub async fn broadcast_text(&self, text: &str) {
        for ch in &self.channels {
            if let Err(e) = ch.send(text, None).await {
                tracing::warn!("broadcast to {} failed: {e}", ch.name());
            }
        }
    }
}

pub type AlertCallback = Arc<dyn Fn(&str, Option<&Signal>) + Send + Sync>;

/// Delivers rendered alerts to an in-process callback (dashboard WebSocket).
pub struct CallbackChannel {
    pub name: String,
    pub callback: AlertCallback,
    pub min_severity: Severity,
    pub max_per_minute: usize,
}

#[async_trait]
impl Channel for CallbackChannel {
    fn name(&self) -> &str {
        &self.name
    }
    fn min_severity(&self) -> Severity {
        self.min_severity
    }
    fn max_per_minute(&self) -> usize {
        self.max_per_minute
    }
    async fn send(&self, text: &str, signal: Option<&Signal>) -> Result<()> {
        (self.callback)(text, signal);
        Ok(())
    }
}

/// Telegram Bot API (sendMessage / getUpdates). The token is never logged.
pub struct TelegramChannel {
    token: String,
    pub chat_id: String,
    pub min_severity: Mutex<Severity>,
    max_per_minute: usize,
    enabled_tools: Option<HashSet<String>>,
    http: reqwest::Client,
}

impl TelegramChannel {
    const API: &'static str = "https://api.telegram.org";

    pub fn new(
        token: String,
        chat_id: String,
        min_severity: Severity,
        max_per_minute: usize,
        enabled_tools: Option<HashSet<String>>,
    ) -> Self {
        Self {
            token,
            chat_id,
            min_severity: Mutex::new(min_severity),
            max_per_minute,
            enabled_tools,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(40))
                .build()
                .expect("http client"),
        }
    }

    fn url(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", Self::API, self.token)
    }

    /// Long-poll getUpdates; only messages from the configured chat are forwarded.
    pub async fn poll_commands(&self, tx: mpsc::Sender<String>) {
        let mut offset: i64 = 0;
        loop {
            let res: Result<()> = async {
                let r = self
                    .http
                    .get(self.url("getUpdates"))
                    .query(&[
                        ("timeout", "30".to_string()),
                        ("offset", offset.to_string()),
                    ])
                    .send()
                    .await?;
                let data: Value = r.json().await?;
                for upd in data["result"].as_array().cloned().unwrap_or_default() {
                    offset = offset.max(upd["update_id"].as_i64().unwrap_or(0) + 1);
                    let msg = &upd["message"];
                    let chat = match &msg["chat"]["id"] {
                        Value::Number(n) => n.to_string(),
                        Value::String(s) => s.clone(),
                        _ => continue,
                    };
                    if chat != self.chat_id {
                        continue;
                    }
                    let text = msg["text"].as_str().unwrap_or("").trim().to_string();
                    if text.starts_with('/') {
                        let _ = tx.send(text).await;
                    }
                }
                Ok(())
            }
            .await;
            if let Err(e) = res {
                // never print the error source: reqwest errors may contain the URL (token)
                tracing::warn!(
                    "telegram polling error ({})",
                    if e.is::<reqwest::Error>() {
                        "network"
                    } else {
                        "decode"
                    }
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn name(&self) -> &str {
        "telegram"
    }
    fn min_severity(&self) -> Severity {
        *self.min_severity.lock()
    }
    fn max_per_minute(&self) -> usize {
        self.max_per_minute
    }
    fn enabled_tools(&self) -> Option<HashSet<String>> {
        self.enabled_tools.clone()
    }
    async fn send(&self, text: &str, _signal: Option<&Signal>) -> Result<()> {
        let r = self
            .http
            .post(self.url("sendMessage"))
            .json(&json!({"chat_id": self.chat_id, "text": text}))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("telegram sendMessage: network error"))?;
        if r.status().as_u16() != 200 {
            bail!("telegram sendMessage HTTP {}", r.status().as_u16());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::models::Side;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn sig(key: &str, sec: i64, sev: Severity, symbol: &str, side: Side) -> Signal {
        let mut features = serde_json::Map::new();
        features.insert("summary_vi".into(), json!(["Nhóm lệnh MỞ"]));
        Signal {
            signal_id: key.into(),
            tool_id: "large_order_radar".into(),
            tool_version: "1".into(),
            symbol: symbol.into(),
            ts: Utc.with_ymd_and_hms(2026, 10, 21, 3, 0, 0).unwrap()
                + chrono::Duration::seconds(sec),
            kind: "block_order_open".into(),
            side,
            score: 50.0,
            severity: sev,
            state: "open".into(),
            features,
            explanation: vec![],
            quality_flags: vec![],
            dedupe_key: key.into(),
            correlation_id: "g".into(),
        }
    }

    fn make(
        max_per_minute: usize,
        min: Severity,
    ) -> (AlertRouter, CallbackChannel, Arc<AtomicU64>) {
        let clock = Arc::new(AtomicU64::new(0));
        let c = clock.clone();
        let ch = CallbackChannel {
            name: "c".into(),
            callback: Arc::new(|_, _| {}),
            min_severity: min,
            max_per_minute,
        };
        let r = AlertRouter::new(
            vec![],
            120.0,
            Arc::new(|_| true),
            Some(Arc::new(move || c.load(Ordering::Relaxed) as f64)),
        );
        (r, ch, clock)
    }

    #[test]
    fn dedupe_cooldown_escalation_rate_limit() {
        let (r, ch, clock) = make(100, Severity::Info);
        let n = Severity::Notice;
        assert_eq!(r.decide(&sig("a", 0, n, "AAA", Side::Buy), &ch), "send");
        assert_eq!(
            r.decide(&sig("a", 500, n, "AAA", Side::Buy), &ch),
            "duplicate"
        );
        assert_eq!(
            r.decide(&sig("b", 30, n, "AAA", Side::Buy), &ch),
            "cooldown"
        );
        assert_eq!(
            r.decide(&sig("c", 40, Severity::Alert, "AAA", Side::Buy), &ch),
            "send"
        );
        assert_eq!(
            r.decide(&sig("d", 50, Severity::Alert, "AAA", Side::Buy), &ch),
            "cooldown"
        );
        assert_eq!(r.decide(&sig("e", 200, n, "AAA", Side::Buy), &ch), "send");
        assert_eq!(r.decide(&sig("f", 201, n, "AAA", Side::Sell), &ch), "send");

        let (r, ch, _) = make(2, Severity::Info);
        assert_eq!(r.decide(&sig("a", 0, n, "AAA", Side::Buy), &ch), "send");
        assert_eq!(r.decide(&sig("b", 0, n, "BBB", Side::Buy), &ch), "send");
        assert_eq!(
            r.decide(&sig("c", 0, n, "CCC", Side::Buy), &ch),
            "rate_limited"
        );
        let _ = clock;
    }

    #[test]
    fn rate_window_slides_and_filters() {
        let (r, ch, clock) = make(1, Severity::Alert);
        assert_eq!(
            r.decide(&sig("a", 0, Severity::Notice, "AAA", Side::Buy), &ch),
            "below_severity"
        );
        assert_eq!(
            r.decide(&sig("b", 0, Severity::Alert, "AAA", Side::Buy), &ch),
            "send"
        );
        assert_eq!(
            r.decide(&sig("c", 0, Severity::Alert, "BBB", Side::Buy), &ch),
            "rate_limited"
        );
        clock.store(61, Ordering::Relaxed);
        assert_eq!(
            r.decide(&sig("d", 0, Severity::Alert, "CCC", Side::Buy), &ch),
            "send"
        );
        let off = AlertRouter::new(vec![], 120.0, Arc::new(|_| false), None);
        assert_eq!(
            off.decide(&sig("e", 0, Severity::Alert, "DDD", Side::Buy), &ch),
            "tool_disabled"
        );
    }

    #[tokio::test]
    async fn coalesced_count_is_rendered() {
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let s2 = sent.clone();
        let ch: Arc<dyn Channel> = Arc::new(CallbackChannel {
            name: "c".into(),
            callback: Arc::new(move |t, _| s2.lock().push(t.to_string())),
            min_severity: Severity::Info,
            max_per_minute: 100,
        });
        let r = AlertRouter::new(vec![ch], 120.0, Arc::new(|_| true), None);
        for (k, sec, sev) in [
            ("a", 0, Severity::Notice),
            ("b", 10, Severity::Notice),
            ("c", 20, Severity::Notice),
            ("d", 30, Severity::Alert),
        ] {
            r.handle(&sig(k, sec, sev, "AAA", Side::Buy)).await;
        }
        let sent = sent.lock();
        assert_eq!(sent.len(), 2);
        assert!(sent[1].contains("(+2 cập nhật cùng mã đã gộp)"));
        assert!(
            sent[0].contains("MUA LỚN · AAA")
                && sent[0].contains("10:00:00")
                && sent[0].contains("khuyến nghị đầu tư")
        );
    }
}
