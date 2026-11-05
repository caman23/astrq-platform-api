//! Runs one tool in isolation: own queue, time budget per call, panic capture,
//! per-call-site circuit breaker, periodic timer on the market clock, snapshots.
//!
//! A tool that errors, panics or is slow only hurts itself: its queue may overflow
//! (counted) but the bus and other tools keep running.

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

use crate::core::bus::Bus;
use crate::core::config::ToolEntry;
use crate::core::metrics::METRICS;
use crate::core::models::{MarketEvent, Signal};
use crate::core::tool::{Subscription, Tool, ToolContext};

#[derive(Debug, Clone, Serialize)]
pub struct ToolStatus {
    pub tool_id: String,
    pub enabled: bool,
    pub state: String,
    pub events: u64,
    pub signals: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub skipped_breaker: u64,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub queue_lag: usize,
    pub dropped: u64,
    pub restored_snapshot: bool,
}

#[derive(Default)]
struct Breaker {
    until: Option<Instant>,
    consecutive: HashMap<&'static str, u32>,
}

pub struct ToolRunner {
    pub tool: Arc<Mutex<Box<dyn Tool>>>,
    pub ctx: ToolContext,
    pub entry: ToolEntry,
    pub subs: Vec<Subscription>,
    signal_bus: Arc<Bus<Signal>>,
    snapshot_dir: Option<PathBuf>,
    pub status: Mutex<ToolStatus>,
    breaker: Mutex<Breaker>,
    lag: AtomicUsize,
    dropped: Arc<AtomicU64>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".into())
}

impl ToolRunner {
    pub fn new(
        tool: Box<dyn Tool>,
        ctx: ToolContext,
        entry: ToolEntry,
        signal_bus: Arc<Bus<Signal>>,
        snapshot_dir: Option<PathBuf>,
    ) -> Self {
        let subs = tool.subscriptions();
        let status = ToolStatus {
            tool_id: tool.id().to_string(),
            enabled: entry.enabled,
            state: "created".into(),
            events: 0,
            signals: 0,
            errors: 0,
            timeouts: 0,
            skipped_breaker: 0,
            consecutive_failures: 0,
            last_error: None,
            queue_lag: 0,
            dropped: 0,
            restored_snapshot: false,
        };
        Self {
            tool: Arc::new(Mutex::new(tool)),
            ctx,
            entry,
            subs,
            signal_bus,
            snapshot_dir,
            status: Mutex::new(status),
            breaker: Mutex::new(Breaker::default()),
            lag: AtomicUsize::new(0),
            dropped: Arc::new(AtomicU64::new(0)),
            tasks: Mutex::new(Vec::new()),
        }
    }

    pub fn tool_id(&self) -> String {
        self.status.lock().tool_id.clone()
    }

    pub fn wants(&self, ev: &MarketEvent) -> bool {
        let u = self.ctx.universe.read();
        self.subs.iter().any(|s| s.matches(ev, &u))
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.status.lock().enabled = enabled;
    }

    pub fn status_snapshot(&self) -> ToolStatus {
        let mut s = self.status.lock().clone();
        s.queue_lag = self.lag.load(Ordering::Relaxed);
        s.dropped = self.dropped.load(Ordering::Relaxed);
        s
    }

    // ------------------------------------------------------------ guarded calls

    fn guard(
        &self,
        site: &'static str,
        limit: Duration,
        f: impl FnOnce(&mut dyn Tool) -> anyhow::Result<Vec<Signal>>,
    ) -> Option<Vec<Signal>> {
        let started = Instant::now();
        let result = {
            let mut tool = self.tool.lock();
            catch_unwind(AssertUnwindSafe(|| f(tool.as_mut())))
        };
        let elapsed = started.elapsed();
        match result {
            Ok(Ok(signals)) => {
                if elapsed > limit {
                    // A synchronous call cannot be interrupted; its result is kept, but the
                    // overrun counts towards the breaker like a timeout would.
                    self.status.lock().timeouts += 1;
                    self.failure(site, format!("{site}: took {:.3}s (budget {:.3}s)", elapsed.as_secs_f64(), limit.as_secs_f64()));
                } else {
                    self.success(site);
                }
                Some(signals)
            }
            Ok(Err(e)) => {
                self.status.lock().errors += 1;
                tracing::error!("tool {} failed in {site}: {e:#}", self.tool_id());
                self.failure(site, format!("{site}: {e:#}"));
                None
            }
            Err(p) => {
                self.status.lock().errors += 1;
                let msg = panic_message(p.as_ref());
                tracing::error!("tool {} panicked in {site}: {msg}", self.tool_id());
                self.failure(site, format!("{site}: panic: {msg}"));
                None
            }
        }
    }

    /// A success only resets its own call site: a healthy on_timer must not mask a
    /// failing on_event.
    fn success(&self, site: &'static str) {
        let mut b = self.breaker.lock();
        b.consecutive.insert(site, 0);
        self.status.lock().consecutive_failures = b.consecutive.values().copied().max().unwrap_or(0);
    }

    fn failure(&self, site: &'static str, msg: String) {
        let mut b = self.breaker.lock();
        let n = {
            let c = b.consecutive.entry(site).or_insert(0);
            *c += 1;
            *c
        };
        let max = b.consecutive.values().copied().max().unwrap_or(0);
        let mut st = self.status.lock();
        st.last_error = Some(msg.clone());
        st.consecutive_failures = max;
        METRICS.inc("tool_failures_total", 1.0, &[("tool", &st.tool_id)]);
        if n >= self.entry.max_consecutive_failures {
            b.until = Some(Instant::now() + Duration::from_secs_f64(self.entry.breaker_cooldown_sec));
            st.state = "breaker_open".into();
            tracing::error!("tool {}: circuit breaker open ({msg})", st.tool_id);
        }
    }

    fn breaker_open(&self) -> bool {
        let mut b = self.breaker.lock();
        match b.until {
            Some(t) if Instant::now() < t => true,
            Some(_) => {
                // cooldown over: half-open, give the tool another chance
                b.until = None;
                b.consecutive.clear();
                let mut st = self.status.lock();
                st.consecutive_failures = 0;
                st.state = "running".into();
                false
            }
            None => false,
        }
    }

    fn emit(&self, signals: Vec<Signal>, ingest_mono: Option<Instant>) {
        if signals.is_empty() {
            return;
        }
        let tool_id = self.tool_id();
        for s in &signals {
            self.signal_bus.publish(s);
        }
        self.status.lock().signals += signals.len() as u64;
        METRICS.inc("signals_total", signals.len() as f64, &[("tool", &tool_id)]);
        if let Some(t) = ingest_mono {
            METRICS.observe("tick_to_signal_seconds", t.elapsed().as_secs_f64(), &[("tool", &tool_id)]);
        }
    }

    pub fn process_event(&self, ev: &MarketEvent) {
        let enabled = {
            let mut st = self.status.lock();
            st.events += 1;
            st.enabled
        };
        if !enabled {
            return;
        }
        if self.breaker_open() {
            self.status.lock().skipped_breaker += 1;
            return;
        }
        let ctx = self.ctx.clone();
        let limit = Duration::from_millis(self.entry.timeout_ms);
        if let Some(sigs) = self.guard("on_event", limit, |t| t.on_event(ev, &ctx)) {
            self.emit(sigs, ev.ingest_mono());
        }
    }

    pub fn process_timer(&self, now: DateTime<Utc>) {
        if !self.status.lock().enabled || self.breaker_open() {
            return;
        }
        let ctx = self.ctx.clone();
        let limit = Duration::from_millis(self.entry.timeout_ms);
        if let Some(sigs) = self.guard("on_timer", limit, |t| t.on_timer(now, &ctx)) {
            self.emit(sigs, None);
        }
    }

    // ------------------------------------------------------------ lifecycle

    pub fn start(self: &Arc<Self>, market_bus: &Bus<MarketEvent>, queue_size: usize) {
        self.restore_snapshot();
        {
            let ctx = self.ctx.clone();
            self.tool.lock().on_start(&ctx);
        }
        let me = self.clone();
        let universe = self.ctx.universe.clone();
        let subs = self.subs.clone();
        let mut sub = market_bus.subscribe(
            &self.tool_id(),
            move |ev| {
                let u = universe.read();
                subs.iter().any(|s| s.matches(ev, &u))
            },
            Some(queue_size),
        );
        let dropped = sub.dropped.clone();
        let events = tokio::spawn({
            let me = me.clone();
            async move {
                while let Some(ev) = sub.rx.recv().await {
                    me.process_event(&ev);
                    me.lag.store(sub.rx.len(), Ordering::Relaxed);
                    me.dropped.store(dropped.load(Ordering::Relaxed), Ordering::Relaxed);
                }
            }
        });
        let timer = tokio::spawn({
            let me = me.clone();
            async move {
                let mut last = me.ctx.clock.now();
                let mut last_snap = Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let now = me.ctx.clock.now();
                    if (now - last).num_milliseconds() as f64 / 1000.0 >= me.entry.timer_interval_sec {
                        last = now;
                        me.process_timer(now);
                    }
                    if me.snapshot_dir.is_some() && last_snap.elapsed() > Duration::from_secs(30) {
                        last_snap = Instant::now();
                        me.save_snapshot();
                    }
                }
            }
        });
        self.tasks.lock().extend([events, timer]);
        self.status.lock().state = "running".into();
    }

    pub fn stop(&self) {
        for t in self.tasks.lock().drain(..) {
            t.abort();
        }
        self.save_snapshot();
        self.status.lock().state = "stopped".into();
    }

    // ------------------------------------------------------------ snapshots

    fn snap_paths(&self) -> Option<(PathBuf, PathBuf)> {
        let dir = self.snapshot_dir.as_ref()?;
        let id = self.tool_id();
        Some((dir.join(format!("{id}.snap")), dir.join(format!("{id}.meta.json"))))
    }

    pub fn save_snapshot(&self) {
        let Some((snap, meta)) = self.snap_paths() else { return };
        let (data, version) = {
            let t = self.tool.lock();
            (t.snapshot(), t.version().to_string())
        };
        let Some(data) = data else { return };
        let today = self.ctx.calendar.trading_date(self.ctx.clock.now());
        let write = || -> std::io::Result<()> {
            std::fs::create_dir_all(snap.parent().expect("dir"))?;
            let tmp = snap.with_extension("tmp");
            std::fs::write(&tmp, &data)?;
            std::fs::rename(&tmp, &snap)?;
            std::fs::write(&meta, serde_json::json!({"version": version, "trading_date": today.to_string()}).to_string())
        };
        if let Err(e) = write() {
            tracing::warn!("tool {} snapshot failed: {e}", self.tool_id());
        }
    }

    fn restore_snapshot(&self) {
        let Some((snap, meta_path)) = self.snap_paths() else { return };
        let (Ok(meta), Ok(data)) = (std::fs::read_to_string(&meta_path), std::fs::read(&snap)) else { return };
        let meta: serde_json::Value = serde_json::from_str(&meta).unwrap_or_default();
        let today = self.ctx.calendar.trading_date(self.ctx.clock.now()).to_string();
        let mut tool = self.tool.lock();
        if meta["version"] != tool.version() || meta["trading_date"] != today.as_str() {
            tracing::info!("tool {}: ignoring stale snapshot {meta}", tool.id());
            return;
        }
        match tool.restore(&data) {
            Ok(()) => {
                self.status.lock().restored_snapshot = true;
                tracing::info!("tool {}: restored snapshot from {today}", tool.id());
            }
            Err(e) => tracing::warn!("tool {}: restore failed: {e:#}", tool.id()),
        }
    }
}
