//! Wires the core together: provider -> ingest -> market bus -> tool runners -> signal
//! bus -> store / recorder / alert router / dashboard listeners. Owns the scheduler
//! (baseline warm-up, EOD summary, retention, universe reload) and remote sync.

use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::core::alerts::{AlertRouter, CallbackChannel, Channel, TelegramChannel};
use crate::core::bus::Bus;
use crate::core::clock::{Clock, EventClock, SimClock, WallClock};
use crate::core::config::Settings;
use crate::core::context::{ContextProvider, ContextService, NullContextProvider};
use crate::core::fmt::DISCLAIMER_VI;
use crate::core::ingest::{Ingest, IngestStats};
use crate::core::market_rules::MarketRules;
use crate::core::metrics::METRICS;
use crate::core::models::{MarketEvent, Severity, Signal};
use crate::core::recorder::JsonlRecorder;
use crate::core::remote_store::{R2Client, RemoteSync};
use crate::core::runner::ToolRunner;
use crate::core::sessions::SessionCalendar;
use crate::core::store::SignalStore;
use crate::core::tool::ToolContext;
use crate::core::universe::Universe;
use crate::providers::dnse::DnseProvider;
use crate::providers::recorded_baseline::RecordedBaselineProvider;
use crate::providers::replay::ReplayProvider;
use crate::providers::ssi::SsiProvider;
use crate::providers::synthetic::{SyntheticContextProvider, SyntheticProvider};
use crate::providers::{MarketDataProvider, RawSink, StatusHandle};
use crate::tools::build_tool;

pub struct Platform {
    pub s: Settings,
    pub calendar: Arc<SessionCalendar>,
    pub rules: Arc<MarketRules>,
    pub universe: Arc<RwLock<Universe>>,
    pub clock: Arc<dyn Clock>,
    pub context: Arc<ContextService>,
    pub market_bus: Arc<Bus<MarketEvent>>,
    pub signal_bus: Arc<Bus<Signal>>,
    pub store: Arc<Mutex<SignalStore>>,
    pub runners: BTreeMap<String, Arc<ToolRunner>>,
    pub router: Arc<AlertRouter>,
    pub telegram: Option<Arc<TelegramChannel>>,
    /// WebSocket fan-out (serialized JSON messages).
    pub listeners: broadcast::Sender<Arc<str>>,
    pub provider_name: &'static str,
    pub provider_status: StatusHandle,
    provider: tokio::sync::Mutex<Option<Box<dyn MarketDataProvider>>>,
    pub data_dir: PathBuf,
    tick_recorder: Option<Arc<JsonlRecorder>>,
    pub signal_recorder: Option<Arc<JsonlRecorder>>,
    raw_recorder: Option<Arc<JsonlRecorder>>,
    pub remote: Option<Arc<RemoteSync>>,
    pub ingest_stats: Arc<Mutex<IngestStats>>,
    pub feed_error: Mutex<Option<String>>,
    pub tick_rate: Mutex<f64>,
    pub last_summary: Mutex<Option<String>>,
    pub started: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    resub_tx: mpsc::Sender<Vec<String>>,
    resub_rx: Mutex<Option<mpsc::Receiver<Vec<String>>>>,
    jobs_done: Mutex<HashMap<&'static str, NaiveDate>>,
}

fn notify(tx: &broadcast::Sender<Arc<str>>, v: Value) {
    if tx.receiver_count() > 0 {
        let _ = tx.send(Arc::from(v.to_string()));
    }
}

impl Platform {
    pub fn new(settings: Settings, stop_at_close: bool) -> Result<Arc<Self>> {
        let s = settings;
        let calendar = Arc::new(SessionCalendar::new(&s.sessions));
        let rules = Arc::new(MarketRules::new(&s.market_rules));
        let universe = Arc::new(RwLock::new(
            Universe::load(&s.path(&s.universe.file)).context("loading universe")?,
        ));
        let data_dir = s.path(&s.storage.data_dir);
        let symbols = universe.read().symbols();

        // ---------------------------------------------------------- provider + clock
        let raw_recorder = matches!(s.provider.kind.as_str(), "dnse" | "ssi").then(|| {
            Arc::new(JsonlRecorder::new(
                data_dir.join("raw").join(&s.provider.kind),
                (*calendar).clone(),
                &format!("raw_{}", s.provider.kind),
            ))
        });
        let sink: Option<RawSink> = raw_recorder.as_ref().map(|r| {
            let r = r.clone();
            Arc::new(move |line: &str, ts| r.write_line(line, ts)) as RawSink
        });
        let (clock, provider): (Arc<dyn Clock>, Box<dyn MarketDataProvider>) =
            match s.provider.kind.as_str() {
                "synthetic" => {
                    let start = SyntheticProvider::start_time(&s.provider.synthetic, &calendar);
                    let clock: Arc<dyn Clock> =
                        Arc::new(SimClock::new(start, s.provider.synthetic.speed));
                    let p = SyntheticProvider::new(
                        s.provider.synthetic.clone(),
                        &symbols,
                        calendar.clone(),
                        rules.clone(),
                        clock.clone(),
                        stop_at_close,
                    );
                    (clock, Box::new(p))
                }
                "replay" => (
                    Arc::new(EventClock::new(None)),
                    Box::new(ReplayProvider::new(
                        s.path(&s.provider.replay.path),
                        s.provider.replay.speed,
                    )),
                ),
                "ssi" => (
                    Arc::new(WallClock),
                    Box::new(SsiProvider::new(
                        s.provider.ssi.clone(),
                        calendar.clone(),
                        sink,
                        None,
                    )),
                ),
                _ => (
                    Arc::new(WallClock),
                    Box::new(DnseProvider::new(
                        s.provider.dnse.clone(),
                        calendar.clone(),
                        sink,
                    )),
                ),
            };
        let provider_name = provider.name();
        let provider_status = provider.status();

        // ---------------------------------------------------------- context
        let ctx_provider: Arc<dyn ContextProvider> = match s.context.kind.as_str() {
            "synthetic" => Arc::new(SyntheticContextProvider {
                seed: s.provider.synthetic.seed,
                rules: (*rules).clone(),
            }),
            "recorded" => Arc::new(RecordedBaselineProvider::new(
                data_dir.join("ticks"),
                data_dir.join("baselines"),
                s.context.lookback_days,
                s.context.min_days,
            )),
            other => {
                if other == "vndirect" {
                    tracing::warn!(
                        "context.kind=vndirect is not implemented yet; running baseline_cold"
                    );
                }
                Arc::new(NullContextProvider)
            }
        };
        let context = Arc::new(ContextService::new(ctx_provider));

        let market_bus = Arc::new(Bus::new("market", s.bus.queue_size));
        let signal_bus = Arc::new(Bus::new("signals", s.bus.queue_size));
        let store = Arc::new(Mutex::new(SignalStore::new((*calendar).clone())));
        let tick_recorder = s.storage.record_ticks.then(|| {
            Arc::new(JsonlRecorder::new(
                data_dir.join("ticks"),
                (*calendar).clone(),
                "ticks",
            ))
        });
        let signal_recorder = s.storage.record_signals.then(|| {
            Arc::new(JsonlRecorder::new(
                data_dir.join("signals"),
                (*calendar).clone(),
                "signals",
            ))
        });

        // ---------------------------------------------------------- tools
        let mut runners = BTreeMap::new();
        for (id, entry) in &s.tools {
            let tool = build_tool(id, entry).with_context(|| format!("tool {id}"))?;
            let ctx = ToolContext {
                tool_id: id.clone(),
                universe: universe.clone(),
                context: context.clone(),
                calendar: calendar.clone(),
                rules: rules.clone(),
                clock: clock.clone(),
            };
            runners.insert(
                id.clone(),
                Arc::new(ToolRunner::new(
                    tool,
                    ctx,
                    entry.clone(),
                    signal_bus.clone(),
                    Some(data_dir.join("snapshots")),
                )),
            );
        }

        // ---------------------------------------------------------- alerts
        let (listeners, _) = broadcast::channel(2000);
        let mut channels: Vec<Arc<dyn Channel>> = Vec::new();
        {
            let tx = listeners.clone();
            channels.push(Arc::new(CallbackChannel {
                name: "dashboard".into(),
                callback: Arc::new(move |text, sig: Option<&Signal>| {
                    notify(
                        &tx,
                        json!({
                            "type": "alert", "text": text,
                            "signal_id": sig.map(|s| s.signal_id.clone()),
                            "symbol": sig.map(|s| s.symbol.clone()),
                            "severity": sig.map(|s| s.severity.as_str()),
                        }),
                    )
                }),
                min_severity: s.alerts.dashboard_min_severity,
                max_per_minute: 600,
            }));
        }
        let tg = &s.alerts.telegram;
        let telegram = match (&tg.bot_token, &tg.chat_id) {
            (Some(token), Some(chat)) if tg.enabled => {
                let t = Arc::new(TelegramChannel::new(
                    token.expose().to_string(),
                    chat.clone(),
                    tg.min_severity,
                    tg.max_per_minute,
                    Some(HashSet::from(["large_order_radar".to_string()])),
                ));
                channels.push(t.clone());
                Some(t)
            }
            _ => None,
        };
        let enabled: HashMap<String, Arc<ToolRunner>> = runners
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let router = Arc::new(AlertRouter::new(
            channels,
            s.alerts.cooldown_sec,
            Arc::new(move |t| enabled.get(t).is_some_and(|r| r.status.lock().enabled)),
            None,
        ));

        let rc = &s.storage.remote;
        let remote = (rc.kind == "r2").then(|| {
            Arc::new(RemoteSync::new(
                R2Client::new(rc.clone(), None),
                data_dir.clone(),
                rc.keep_summary_days,
            ))
        });
        let (resub_tx, resub_rx) = mpsc::channel(4);

        Ok(Arc::new(Self {
            s,
            calendar,
            rules,
            universe,
            clock,
            context,
            market_bus,
            signal_bus,
            store,
            runners,
            router,
            telegram,
            listeners,
            provider_name,
            provider_status,
            provider: tokio::sync::Mutex::new(Some(provider)),
            data_dir,
            tick_recorder,
            signal_recorder,
            raw_recorder,
            remote,
            ingest_stats: Arc::new(Mutex::new(IngestStats::default())),
            feed_error: Mutex::new(None),
            tick_rate: Mutex::new(0.0),
            last_summary: Mutex::new(None),
            started: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            resub_tx,
            resub_rx: Mutex::new(Some(resub_rx)),
            jobs_done: Mutex::new(HashMap::new()),
        }))
    }

    pub fn notify(&self, v: Value) {
        notify(&self.listeners, v);
    }

    // ------------------------------------------------------------ lifecycle

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let today = self.calendar.trading_date(self.clock.now());
        if let Some(r) = &self.remote {
            r.restore(today).await; // disk-less host: bring back summaries + today's signals
        }
        self.reload_today_signals(today);
        let symbols = self.universe.read().symbols();
        self.context.warm(&symbols, today).await;
        self.jobs_done.lock().insert("warm", today);
        for r in [
            &self.tick_recorder,
            &self.signal_recorder,
            &self.raw_recorder,
        ]
        .into_iter()
        .flatten()
        {
            r.start();
        }
        for r in self.runners.values() {
            r.start(&self.market_bus, self.s.bus.queue_size);
        }
        let mut tasks = vec![
            tokio::spawn(self.clone().feed_loop()),
            tokio::spawn(self.clone().signal_loop()),
            tokio::spawn(self.clone().status_loop()),
            tokio::spawn(self.clone().scheduler_loop()),
            tokio::spawn(self.clone().remote_loop()),
        ];
        if let Some(tg) = &self.telegram {
            let (tx, mut rx) = mpsc::channel::<String>(16);
            let tg2 = tg.clone();
            tasks.push(tokio::spawn(async move { tg2.poll_commands(tx).await }));
            let me = self.clone();
            let tg3 = tg.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(cmd) = rx.recv().await {
                    let reply = me.handle_command(&cmd);
                    let _ = tg3.send(&reply, None).await;
                }
            }));
        }
        self.tasks.lock().extend(tasks);
        self.started.store(true, Ordering::Relaxed);
        tracing::info!(
            "platform started: provider={} tools={:?}",
            self.provider_name,
            self.runners.keys().collect::<Vec<_>>()
        );
        Ok(())
    }

    pub async fn stop(&self) {
        for t in self.tasks.lock().drain(..) {
            t.abort();
        }
        for r in self.runners.values() {
            r.stop();
        }
        if let Some(mut p) = self.provider.lock().await.take() {
            p.close().await;
        }
        for r in [
            &self.tick_recorder,
            &self.signal_recorder,
            &self.raw_recorder,
        ]
        .into_iter()
        .flatten()
        {
            r.close().await;
        }
        if let Some(r) = &self.remote {
            // host stopping (e.g. spin-down): persist what we have
            r.upload_signals(self.calendar.trading_date(self.clock.now()))
                .await;
        }
        self.started.store(false, Ordering::Relaxed);
    }

    /// After a restart, reload today's recorded signals so the radar is not empty.
    fn reload_today_signals(&self, today: NaiveDate) {
        let Some(rec) = &self.signal_recorder else {
            return;
        };
        let Ok(text) = std::fs::read_to_string(rec.file_for(today)) else {
            return;
        };
        let mut store = self.store.lock();
        let mut n = 0;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            if let Ok(s) = serde_json::from_str::<Signal>(line) {
                store.add(s);
                n += 1;
            }
        }
        tracing::info!("reloaded {n} signals for {today}");
    }

    // ------------------------------------------------------------ loops

    async fn feed_loop(self: Arc<Self>) {
        let Some(mut provider) = self.provider.lock().await.take() else {
            return;
        };
        let mut resub = self.resub_rx.lock().take();
        let mut ingest = Ingest::new(
            self.market_bus.clone(),
            self.calendar.clone(),
            self.universe.clone(),
            Some(self.clock.clone()),
        );
        if let Some(rec) = self.tick_recorder.clone() {
            ingest.on_event = Some(Box::new(move |ev: &MarketEvent| {
                rec.write(ev, ev.ts_exchange())
            }));
        }
        let mut backoff = 1.0f64;
        loop {
            let res: Result<bool> = async {
                provider.connect().await?;
                let symbols = self.universe.read().symbols();
                provider.subscribe(&symbols).await?;
                *self.feed_error.lock() = None;
                backoff = 1.0;
                let mut stats_at = Instant::now();
                loop {
                    tokio::select! {
                        ev = provider.next_event() => match ev? {
                            Some(ev) => {
                                ingest.process(ev);
                                if stats_at.elapsed() > Duration::from_millis(500) {
                                    stats_at = Instant::now();
                                    *self.ingest_stats.lock() = ingest.stats.clone();
                                }
                            }
                            None => {
                                *self.ingest_stats.lock() = ingest.stats.clone();
                                return Ok(true);
                            }
                        },
                        Some(syms) = async { match resub.as_mut() { Some(r) => r.recv().await, None => std::future::pending().await } } => {
                            provider.subscribe(&syms).await?;
                        }
                    }
                }
            }
            .await;
            match res {
                Ok(true) => {
                    tracing::info!("feed finished");
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    *self.feed_error.lock() = Some(format!("{e:#}"));
                    self.provider_status.lock().reconnects += 1;
                    METRICS.inc("feed_reconnects_total", 1.0, &[]);
                    tracing::warn!("feed error, reconnecting in {backoff:.0}s: {e:#}");
                    tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                    backoff = (backoff * 2.0).min(60.0);
                }
            }
        }
        // keep the provider for a clean close on shutdown
        *self.provider.lock().await = Some(provider);
    }

    async fn signal_loop(self: Arc<Self>) {
        let mut sub = self
            .signal_bus
            .subscribe("platform", |_| true, Some(self.s.bus.queue_size));
        let (alert_tx, mut alert_rx) = mpsc::channel::<Signal>(10_000);
        let router = self.router.clone();
        let alerts = tokio::spawn(async move {
            while let Some(s) = alert_rx.recv().await {
                router.handle(&s).await;
            }
        });
        while let Some(s) = sub.rx.recv().await {
            if let Some(rec) = &self.signal_recorder {
                rec.write(&s, s.ts);
            }
            self.notify(json!({"type": "signal", "signal": s}));
            if alert_tx.try_send(s.clone()).is_err() {
                METRICS.inc("alerts_dropped_total", 1.0, &[]);
            }
            self.store.lock().add(s);
        }
        alerts.abort();
    }

    async fn status_loop(self: Arc<Self>) {
        let mut prev = METRICS.counter("ticks_total", &[]);
        let mut prev_t = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await; // status push cadence (bandwidth-friendly)
            let cur = METRICS.counter("ticks_total", &[]);
            let rate = (cur - prev) / prev_t.elapsed().as_secs_f64().max(1e-6);
            *self.tick_rate.lock() = rate;
            METRICS.set("tick_rate_per_sec", rate, &[]);
            prev = cur;
            prev_t = Instant::now();
            self.notify(json!({"type": "status", "status": self.status()}));
        }
    }

    async fn scheduler_loop(self: Arc<Self>) {
        let mut last_universe_check = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let now = self.clock.now();
            let d = self.calendar.trading_date(now);
            let t = self.calendar.local(now).time();
            if self.calendar.is_trading_day(d) {
                if t >= self.s.context.warmup_time.0
                    && self.jobs_done.lock().get("warm") != Some(&d)
                {
                    self.jobs_done.lock().insert("warm", d);
                    let symbols = self.universe.read().symbols();
                    self.context.warm(&symbols, d).await;
                }
                if t >= self.s.context.eod_summary_time.0
                    && self.jobs_done.lock().get("eod") != Some(&d)
                {
                    self.jobs_done.lock().insert("eod", d);
                    self.eod_summary().await;
                    if let Some(r) = &self.remote {
                        r.upload_signals(d).await;
                        r.upload_day_summary(d).await;
                    }
                }
            }
            if self.jobs_done.lock().get("cleanup") != Some(&d) {
                self.jobs_done.lock().insert("cleanup", d);
                for r in [
                    &self.tick_recorder,
                    &self.signal_recorder,
                    &self.raw_recorder,
                ]
                .into_iter()
                .flatten()
                {
                    r.cleanup(self.s.storage.retention_days);
                }
            }
            if last_universe_check.elapsed().as_secs_f64() > self.s.universe.reload_interval_sec {
                last_universe_check = Instant::now();
                let changed = self.universe.write().maybe_reload();
                if changed {
                    let symbols = self.universe.read().symbols();
                    let _ = self.resub_tx.try_send(symbols.clone());
                    self.context.warm(&symbols, d).await;
                }
            }
        }
    }

    async fn remote_loop(self: Arc<Self>) {
        let Some(remote) = self.remote.clone() else {
            return;
        };
        let interval =
            Duration::from_secs_f64(self.s.storage.remote.signals_upload_interval_sec.max(30.0));
        loop {
            tokio::time::sleep(interval).await;
            remote
                .upload_signals(self.calendar.trading_date(self.clock.now()))
                .await;
        }
    }

    async fn eod_summary(&self) {
        let parts: Vec<String> = self
            .runners
            .values()
            .filter_map(|r| r.tool.lock().daily_summary(&r.ctx))
            .collect();
        if parts.is_empty() {
            return;
        }
        let text = parts.join("\n\n");
        *self.last_summary.lock() = Some(text.clone());
        self.router.broadcast_text(&text).await;
        self.notify(json!({"type": "summary", "text": text}));
    }

    // ------------------------------------------------------------ control & status

    pub fn set_tool_enabled(&self, tool_id: &str, enabled: bool) -> bool {
        match self.runners.get(tool_id) {
            Some(r) => {
                r.set_enabled(enabled);
                true
            }
            None => false,
        }
    }

    /// Telegram commands: /status, /on <tool>, /off <tool>, /min <info|notice|alert>.
    pub fn handle_command(&self, text: &str) -> String {
        let parts: Vec<&str> = text.split_whitespace().collect();
        let cmd = parts
            .first()
            .map(|c| c.split('@').next().unwrap_or("").to_ascii_lowercase())
            .unwrap_or_default();
        match (cmd.as_str(), parts.get(1)) {
            ("/status", _) => {
                let st = self.status();
                let tools: Vec<String> = st["tools"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .map(|t| {
                                format!(
                                    "{}={}({})",
                                    t["tool_id"].as_str().unwrap_or(""),
                                    if t["enabled"].as_bool() == Some(true) {
                                        "bật"
                                    } else {
                                        "tắt"
                                    },
                                    t["state"].as_str().unwrap_or("")
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                format!(
                    "Phiên: {} · Feed: {}\nTick/giây: {:.1} · Tín hiệu hôm nay: {}\nTools: {}",
                    st["session"].as_str().unwrap_or(""),
                    st["provider"]["detail"].as_str().unwrap_or(""),
                    st["tick_rate"].as_f64().unwrap_or(0.0),
                    st["signals_today"],
                    tools.join(", ")
                )
            }
            (c @ ("/on" | "/off"), Some(tool)) if self.runners.contains_key(*tool) => {
                self.set_tool_enabled(tool, c == "/on");
                format!("Đã {} {tool}", if c == "/on" { "bật" } else { "tắt" })
            }
            ("/min", Some(level)) if Severity::parse(level).is_some() => {
                if let Some(tg) = &self.telegram {
                    *tg.min_severity.lock() = Severity::parse(level).expect("checked");
                }
                format!("Mức độ tối thiểu Telegram: {level}")
            }
            _ => "Lệnh: /status, /on <tool>, /off <tool>, /min <info|notice|alert>".into(),
        }
    }

    pub fn status(&self) -> Value {
        let now = self.clock.now();
        let d = self.calendar.trading_date(now);
        let ps = self.provider_status.lock().clone();
        let u = self.universe.read();
        let symbols = u.symbols();
        json!({
            "now": now.to_rfc3339(),
            "trading_date": d.to_string(),
            "session": self.calendar.session_at(now).as_str(),
            "clock": self.clock.kind(),
            "clock_speed": self.clock.speed(),
            "provider": {
                "name": self.provider_name,
                "connected": ps.connected,
                "finished": ps.finished,
                "last_event_at": ps.last_event_at.map(|t| t.to_rfc3339()),
                "reconnects": ps.reconnects,
                "detail": ps.detail,
                "error": *self.feed_error.lock(),
                "stats": ps.stats,
            },
            "ingest": *self.ingest_stats.lock(),
            "tick_rate": (*self.tick_rate.lock() * 10.0).round() / 10.0,
            "latency": {
                "feed": METRICS.summary("feed_latency_seconds", &[]),
                "internal": METRICS.summary("tick_to_signal_seconds", &[("tool", "large_order_radar")]),
            },
            "baseline_coverage": (self.context.coverage(&symbols, d) * 1000.0).round() / 1000.0,
            "universe": {"name": u.meta.name, "size": u.len(), "verified": u.meta.verified, "as_of": u.meta.as_of},
            "signals_today": self.store.lock().signals.len(),
            "tools": self.runners.values().map(|r| r.status_snapshot()).collect::<Vec<_>>(),
            "listeners": self.listeners.receiver_count(),
            "remote_storage": self.remote.as_ref().map(|r| json!({
                "last_upload": r.last_upload.lock().map(|t| t.to_rfc3339()),
                "last_error": *r.last_error.lock(),
            })),
            "disclaimer": DISCLAIMER_VI,
            "server_time": Utc::now().to_rfc3339(),
        })
    }
}
