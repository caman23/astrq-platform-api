//! REST + WebSocket API (same contract as the Python service, so the React dashboard is
//! unchanged). Read-only market analysis: there is no trading endpoint.

pub mod guard;
pub mod server;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Extension;
use axum::{Json, Router};
use bytes::Bytes;
use chrono::NaiveDate;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::core::metrics::METRICS;
use crate::core::models::{Severity, Signal};
use crate::core::platform::Platform;
use crate::tools::large_order_radar::view::FLOW_BUCKETS;
use guard::{ClientIp, Guard};

type AppState = Arc<Platform>;

pub fn router(platform: Arc<Platform>) -> Router {
    let s = &platform.s;
    let guard = Guard::new(&s.api.limits);
    let cache = Arc::new(ResponseCache::new(&s.api.limits));
    let mut app = Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/api/status", get(status))
        .route("/api/history", get(history))
        .route("/api/universe", get(universe))
        .route("/api/signals", get(signals))
        .route("/api/alerts", get(alerts))
        .route("/api/summary", get(summary))
        .route("/api/tools", get(tools))
        .route("/api/tools/{id}/enabled", post(set_enabled))
        .route("/api/tools/{id}/view", get(tool_view))
        .route("/ws", get(ws))
        .layer(Extension(cache))
        .layer(Extension(guard.clone()));

    let dist = s.path(&s.api.frontend_dist);
    if dist.join("index.html").exists() {
        let spa = ServeDir::new(&dist).fallback(ServeFile::new(dist.join("index.html")));
        app = app.fallback_service(spa);
    }

    let origins: Vec<HeaderValue> = s
        .api
        .cors_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    let mut app =
        app.layer(middleware::from_fn(static_cache_headers))
            .layer(middleware::from_fn_with_state(
                platform.clone(),
                origin_secret,
            ));
    // CSP (own scripts only; `frame-ancestors` = sites allowed to embed the radar),
    // nosniff, referrer and permissions policies.
    for (name, value) in guard::security_headers(&s.api.frame_ancestors) {
        app = app.layer(SetResponseHeaderLayer::overriding(name, value));
    }
    app
        // Compress JSON (the radar view is large and free hosts meter egress).
        .layer(CompressionLayer::new())
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(origins))
                .allow_methods([Method::GET, Method::POST])
                .allow_headers([header::CONTENT_TYPE]),
        )
        // outermost: floods are turned away before any other work
        .layer(middleware::from_fn_with_state(
            (platform.clone(), guard),
            guard::guard,
        ))
        .with_state(platform)
}

/// Reject requests that did not come through our Cloudflare domain when
/// `api.origin_secret` is set (Cloudflare adds `X-Astrq-Origin`). Covers WebSocket
/// upgrades too; `/health` stays open for health checks and the keep-alive cron.
async fn origin_secret(State(p): State<AppState>, req: Request, next: Next) -> Response {
    if let Some(secret) = &p.s.api.origin_secret {
        if req.uri().path() != "/health" {
            let got = req
                .headers()
                .get("x-astrq-origin")
                .map(|v| v.as_bytes())
                .unwrap_or_default();
            if !constant_time_eq(got, secret.expose().as_bytes()) {
                return (StatusCode::FORBIDDEN, "Forbidden").into_response();
            }
        }
    }
    next.run(req).await
}

/// Browser caching for the dashboard files. Without an explicit header, browsers guess a
/// lifetime from `Last-Modified` and can keep showing the previous deploy (also inside
/// iframes on other sites). Pages (`/`, `/embed`, …) must always be revalidated; files in
/// `/assets/` have a content hash in their name, so they can be cached for a year.
async fn static_cache_headers(req: Request, next: Next) -> Response {
    let assets = req.uri().path().starts_with("/assets/");
    let mut res = next.run(req).await;
    let h = res.headers_mut();
    if !h.contains_key(header::CACHE_CONTROL) {
        let html = h
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/html"));
        if assets {
            h.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            );
        } else if html {
            h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        }
    }
    res
}

/// Whether the request carries the configured origin secret.
pub(crate) fn origin_ok(p: &Platform, headers: &HeaderMap) -> bool {
    p.s.api.origin_secret.as_ref().is_some_and(|secret| {
        let got = headers
            .get("x-astrq-origin")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        constant_time_eq(got, secret.expose().as_bytes())
    })
}

async fn metrics(State(p): State<AppState>, req: Request) -> Response {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    if guard::metrics_allowed(&p, req.headers(), peer) {
        METRICS.render_prometheus().into_response()
    } else {
        (StatusCode::NOT_FOUND, "Not found").into_response()
    }
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn ready(State(p): State<AppState>) -> Response {
    let connected = p.provider_status.lock().connected;
    let err = p.feed_error.lock().clone();
    if p.started.load(std::sync::atomic::Ordering::Relaxed) && connected && err.is_none() {
        Json(json!({"ready": true})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"detail": {"ready": false, "feed_error": err}})),
        )
            .into_response()
    }
}

async fn status(State(p): State<AppState>) -> Json<Value> {
    Json(p.status())
}

async fn universe(State(p): State<AppState>) -> Json<Value> {
    let u = p.universe.read();
    Json(json!({"meta": u.meta, "instruments": u.instruments()}))
}

fn unprocessable(detail: &str) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

fn not_found(detail: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "detail": detail }))).into_response()
}

fn parse_date(v: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(v, "%Y-%m-%d").ok()
}

/// Ticker as sent by a client: upper-cased, and it must be in the universe — so query
/// strings cannot mint unbounded cache keys or scan for anything else.
fn known_symbol(p: &Platform, raw: &str) -> Option<String> {
    let s = raw.trim().to_ascii_uppercase();
    (s.len() <= 12 && p.universe.read().contains(&s)).then_some(s)
}

#[derive(Deserialize)]
struct SignalQuery {
    tool_id: Option<String>,
    symbol: Option<String>,
    side: Option<String>,
    min_severity: Option<String>,
    limit: Option<usize>,
    date: Option<String>,
}

fn filter_signals<'a>(
    it: impl Iterator<Item = &'a Arc<Signal>>,
    tool_id: Option<&str>,
    symbol: Option<&str>,
    side: Option<&str>,
    min: Option<Severity>,
    limit: usize,
) -> Vec<Arc<Signal>> {
    it.filter(|s| tool_id.is_none_or(|t| s.tool_id == t))
        .filter(|s| symbol.is_none_or(|x| s.symbol == x))
        .filter(|s| side.is_none_or(|x| s.side.as_str() == x))
        .filter(|s| min.is_none_or(|m| s.severity.rank() >= m.rank()))
        .take(limit)
        .cloned()
        .collect()
}

async fn signals(
    State(p): State<AppState>,
    Extension(cache): Extension<Arc<ResponseCache>>,
    Query(q): Query<SignalQuery>,
    headers: HeaderMap,
) -> Response {
    let limit = q.limit.unwrap_or(200);
    if limit > 2000 {
        return unprocessable("limit must be <= 2000");
    }
    let min = match q.min_severity.as_deref() {
        None => None,
        Some(s) => match Severity::parse(s) {
            Some(v) => Some(v),
            None => return unprocessable("bad min_severity"),
        },
    };
    if q.tool_id
        .as_ref()
        .is_some_and(|t| !p.runners.contains_key(t))
    {
        return not_found("unknown tool");
    }
    if q.side
        .as_deref()
        .is_some_and(|s| !matches!(s, "buy" | "sell" | "unknown"))
    {
        return unprocessable("side must be buy, sell or unknown");
    }
    let symbol = match q.symbol.as_deref() {
        None => None,
        Some(raw) => match known_symbol(&p, raw) {
            Some(s) => Some(s),
            None => return not_found("unknown symbol"),
        },
    };
    let date = match q.date.as_deref().map(parse_date) {
        None => None,
        Some(None) => return unprocessable("date must be YYYY-MM-DD"),
        Some(Some(d)) if d == p.live_date() => None,
        Some(Some(d)) => Some(d),
    };
    let key = format!(
        "signals@{date:?}?tool_id={:?}&symbol={symbol:?}&side={:?}&min={:?}&limit={limit}",
        q.tool_id,
        q.side,
        min.map(|m| m.as_str())
    );
    let built = match date {
        // a past session: its signals never change
        Some(d) => {
            let Some(day) = p.archived_day(d).await else {
                return not_found("no data for this date");
            };
            cache
                .get_or_build(&p, key, true, move || {
                    let rows = filter_signals(
                        day.signals.iter().rev(),
                        q.tool_id.as_deref(),
                        symbol.as_deref(),
                        q.side.as_deref(),
                        min,
                        limit,
                    );
                    Some((FROZEN, serde_json::to_vec(&rows).ok()?))
                })
                .await
        }
        None => {
            let p2 = p.clone();
            cache
                .get_or_build(&p, key, false, move || {
                    let st = p2.store.lock();
                    let rows = filter_signals(
                        st.signals.iter().rev(),
                        q.tool_id.as_deref(),
                        symbol.as_deref(),
                        q.side.as_deref(),
                        min,
                        limit,
                    );
                    let version = st.version;
                    drop(st);
                    Some((version, serde_json::to_vec(&rows).ok()?))
                })
                .await
        }
    };
    match built {
        Ok(v) => cached_response(&v, &headers),
        Err(e) => e.into_response(),
    }
}

async fn alerts(State(p): State<AppState>) -> Json<Value> {
    let h: Vec<_> = p.router.history.lock().iter().rev().cloned().collect();
    Json(json!(h))
}

async fn summary(State(p): State<AppState>) -> Json<Value> {
    Json(json!({"text": *p.last_summary.lock()}))
}

/// Sessions that can be viewed: the live one and past ones still on record.
async fn history(State(p): State<AppState>) -> Json<Value> {
    let dates: Vec<String> = p
        .history_dates()
        .await
        .iter()
        .map(|d| d.to_string())
        .collect();
    Json(json!({
        "live_date": p.live_date().to_string(),
        "today": p.calendar.trading_date(p.clock.now()).to_string(),
        "retention_days": p.s.storage.retention_days,
        "dates": dates,
    }))
}

// ------------------------------------------------------------------ response cache

/// A rendered JSON response, shared by every viewer that asks for the same thing while
/// the data is unchanged: serialized and gzip-compressed once.
struct Cached {
    version: u64,
    built: Instant,
    etag: HeaderValue,
    /// Only the compressed body is kept (browsers all accept gzip); the rare client
    /// without gzip gets it inflated on the fly. Keeps many timeline views in memory.
    gzip: Bytes,
}

impl Cached {
    fn encode(version: u64, raw: Vec<u8>) -> Option<Self> {
        // compressed once per data version, so spend the CPU for a smaller download
        let mut gz = GzEncoder::new(Vec::with_capacity(raw.len() / 6), Compression::default());
        gz.write_all(&raw).ok()?;
        let gzip = gz.finish().ok()?;
        let mut h = std::hash::DefaultHasher::new();
        raw.hash(&mut h);
        Some(Self {
            version,
            built: Instant::now(),
            etag: HeaderValue::from_str(&format!("\"{:016x}\"", h.finish())).ok()?,
            gzip: gzip.into(),
        })
    }

    fn size(&self) -> usize {
        self.gzip.len()
    }
}

/// Version tag of responses about past sessions (they never change).
const FROZEN: u64 = u64::MAX;

enum BuildError {
    /// Nothing to show (unknown tool view, …).
    Empty,
    /// Too many builds queued: shed load instead of piling up.
    Busy,
}

impl IntoResponse for BuildError {
    fn into_response(self) -> Response {
        match self {
            BuildError::Empty => not_found("tool has no view"),
            BuildError::Busy => {
                METRICS.inc("http_rejected_total", 1.0, &[("reason", "builders")]);
                let mut r = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Server busy — try again shortly.",
                )
                    .into_response();
                r.headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
                r
            }
        }
    }
}

/// Dashboards and iframes re-fetch the radar view after every signal, so N viewers would
/// otherwise rebuild the same JSON N times. Entries are keyed by path + normalized query
/// and reused while the signal store is unchanged — and for at most `CACHE_MAX_AGE`, so
/// day totals that move without a new signal still show up. Past sessions are frozen and
/// kept for `FROZEN_MAX_AGE`.
///
/// Concurrent misses of one key build once (per-key single flight); at most `max_builds`
/// different keys build at the same time, and a request that cannot start a build within
/// `build_wait` gets 503 — so a flood of distinct queries cannot starve everyone else.
struct ResponseCache {
    entries: parking_lot::Mutex<(HashMap<String, Arc<Cached>>, usize)>,
    inflight: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    builders: Arc<tokio::sync::Semaphore>,
    build_wait: Duration,
}

const CACHE_MAX_AGE: Duration = Duration::from_secs(2);
const FROZEN_MAX_AGE: Duration = Duration::from_secs(600);
/// Memory bound (free hosts have 512 MB); arbitrary query values cannot grow it further.
const CACHE_MAX_BYTES: usize = 32 << 20;
const VIEW_PARAMS: [&str; 10] = [
    "symbol",
    "full",
    "orders",
    "orders_with_trades",
    "trades",
    "at",
    "sectors",
    "flow",
    "bucket",
    "date",
];

impl ResponseCache {
    fn new(limits: &crate::core::config::ApiLimits) -> Self {
        Self {
            entries: Default::default(),
            inflight: Default::default(),
            builders: Arc::new(tokio::sync::Semaphore::new(limits.max_builds.max(1))),
            build_wait: Duration::from_secs_f64(limits.build_wait_sec.max(0.1)),
        }
    }

    fn fresh(&self, key: &str, version: Option<u64>) -> Option<Arc<Cached>> {
        self.entries
            .lock()
            .0
            .get(key)
            .filter(|c| match version {
                Some(v) => c.version == v && c.built.elapsed() < CACHE_MAX_AGE,
                None => c.version == FROZEN && c.built.elapsed() < FROZEN_MAX_AGE,
            })
            .cloned()
    }

    fn put(&self, key: String, v: Arc<Cached>) {
        let mut guard = self.entries.lock();
        let (map, bytes) = &mut *guard;
        if let Some(old) = map.remove(&key) {
            *bytes -= old.size();
        }
        if *bytes + v.size() > CACHE_MAX_BYTES {
            map.clear();
            *bytes = 0;
        }
        *bytes += v.size();
        map.insert(key, v);
    }

    /// Serve `key` from the cache, or run `build` (on a blocking thread). `frozen`: the
    /// data never changes (past session); otherwise entries follow the store version.
    /// `build` returns the data version it rendered and the JSON.
    async fn get_or_build<F>(
        &self,
        p: &Platform,
        key: String,
        frozen: bool,
        build: F,
    ) -> Result<Arc<Cached>, BuildError>
    where
        F: FnOnce() -> Option<(u64, Vec<u8>)> + Send + 'static,
    {
        let version = || (!frozen).then(|| p.store.lock().version);
        if let Some(v) = self.fresh(&key, version()) {
            return Ok(v);
        }
        let flight = self.inflight.lock().entry(key.clone()).or_default().clone();
        let _same_key = flight.lock().await;
        if let Some(v) = self.fresh(&key, version()) {
            return Ok(v); // built by whoever held the key before us
        }
        let permit = match tokio::time::timeout(
            self.build_wait,
            self.builders.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(p)) => p,
            _ => {
                self.inflight.lock().remove(&key);
                return Err(BuildError::Busy);
            }
        };
        let built = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let (version, raw) = build()?;
            Cached::encode(version, raw)
        })
        .await
        .ok()
        .flatten();
        self.inflight.lock().remove(&key);
        let built = Arc::new(built.ok_or(BuildError::Empty)?);
        self.put(key, built.clone());
        Ok(built)
    }
}

/// Validate and canonicalize view parameters: unknown keys dropped, numbers clamped,
/// flags reduced to "1", times to HH:MM, symbols checked against the universe. Every
/// distinct key costs a build, so only meaningful variants may exist.
#[allow(clippy::result_large_err)] // the error is the ready-to-send HTTP response
fn normalize_view_params(
    p: &Platform,
    mut params: HashMap<String, String>,
) -> Result<HashMap<String, String>, Response> {
    params.retain(|k, _| VIEW_PARAMS.contains(&k.as_str()));
    for flag in ["full", "sectors", "flow"] {
        match params.get(flag).map(String::as_str) {
            Some("1" | "true") => {
                params.insert(flag.into(), "1".into());
            }
            Some(_) => {
                params.remove(flag);
            }
            None => {}
        }
    }
    for (k, lo, hi) in [
        ("orders", 1, 30),
        ("orders_with_trades", 0, 30),
        ("trades", 0, 50),
    ] {
        if let Some(v) = params.get(k) {
            match v.parse::<usize>() {
                Ok(n) => {
                    params.insert(k.into(), n.clamp(lo, hi).to_string());
                }
                Err(_) => {
                    params.remove(k);
                }
            }
        }
    }
    if let Some(b) = params.get("bucket") {
        match b.parse::<u32>() {
            Ok(n) if FLOW_BUCKETS.contains(&n) && params.contains_key("flow") => {
                params.insert("bucket".into(), n.to_string());
            }
            _ => {
                params.remove("bucket");
            }
        }
    }
    if let Some(at) = params.get("at") {
        let t = chrono::NaiveTime::parse_from_str(at, "%H:%M")
            .or_else(|_| chrono::NaiveTime::parse_from_str(at, "%H:%M:%S"))
            .map_err(|_| unprocessable("at must be HH:MM or HH:MM:SS (Vietnam time)"))?;
        params.insert("at".into(), t.format("%H:%M").to_string());
    }
    if let Some(raw) = params.get("symbol") {
        let s = known_symbol(p, raw).ok_or_else(|| not_found("unknown symbol"))?;
        params.insert("symbol".into(), s);
    }
    if let Some(d) = params.get("date") {
        let d = parse_date(d).ok_or_else(|| unprocessable("date must be YYYY-MM-DD"))?;
        if d == p.live_date() {
            params.remove("date"); // the live session: same as no date
        } else {
            params.insert("date".into(), d.to_string());
        }
    }
    Ok(params)
}

fn render_view(p: &Platform, id: &str, params: &HashMap<String, String>) -> Option<(u64, Vec<u8>)> {
    let r = p.runners.get(id)?;
    let (version, signals) = {
        let st = p.store.lock();
        (st.version, st.by_tool(id))
    };
    let view = r.tool.lock().view(&signals, &r.ctx, params)?;
    Some((version, serde_json::to_vec(&view).ok()?))
}

async fn tools(State(p): State<AppState>) -> Json<Value> {
    let control = p.s.api.allow_tool_control;
    let out: Vec<Value> = p
        .runners
        .values()
        .map(|r| {
            let t = r.tool.lock();
            json!({"manifest": t.manifest(), "config": t.config_json(), "status": r.status_snapshot(), "control_enabled": control})
        })
        .collect();
    Json(json!(out))
}

#[derive(Deserialize)]
struct EnabledBody {
    enabled: bool,
}

async fn set_enabled(
    State(p): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Response {
    if !p.s.api.allow_tool_control {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"detail": "tool control is disabled on this deployment"})),
        )
            .into_response();
    }
    match p.runners.get(&id) {
        Some(r) => {
            r.set_enabled(body.enabled);
            Json(r.status_snapshot()).into_response()
        }
        None => not_found("unknown tool"),
    }
}

async fn tool_view(
    State(p): State<AppState>,
    Extension(cache): Extension<Arc<ResponseCache>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if !p.runners.contains_key(&id) {
        return not_found("unknown tool");
    }
    let params = match normalize_view_params(&p, params) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let mut parts: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
    parts.sort();
    let key = format!("{id}?{}", parts.join("&"));

    let built = match params.get("date").and_then(|d| parse_date(d)) {
        Some(d) => {
            let Some(day) = p.archived_day(d).await else {
                return not_found("no data for this date");
            };
            let p2 = p.clone();
            cache
                .get_or_build(&p, key, true, move || {
                    let r = p2.runners.get(&id)?;
                    let signals = day.by_tool(&id);
                    let archive = day.archives.get(&id).map(Vec::as_slice);
                    let view = r
                        .tool
                        .lock()
                        .archive_view(archive, &signals, d, &r.ctx, &params)?;
                    Some((FROZEN, serde_json::to_vec(&view).ok()?))
                })
                .await
        }
        None => {
            let p2 = p.clone();
            cache
                .get_or_build(&p, key, false, move || render_view(&p2, &id, &params))
                .await
        }
    };
    match built {
        Ok(v) => cached_response(&v, &headers),
        Err(e) => e.into_response(),
    }
}

fn cached_response(v: &Cached, req: &HeaderMap) -> Response {
    let mut h = HeaderMap::new();
    h.insert(header::ETAG, v.etag.clone());
    // always revalidate; an unchanged view then costs a 304 with no body
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    let matches = req
        .get(header::IF_NONE_MATCH)
        .and_then(|x| x.to_str().ok())
        .is_some_and(|x| {
            x.split(',')
                .any(|t| t.trim() == v.etag.to_str().unwrap_or(""))
        });
    if matches {
        return (StatusCode::NOT_MODIFIED, h).into_response();
    }
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let gzip_ok = req
        .get(header::ACCEPT_ENCODING)
        .and_then(|x| x.to_str().ok())
        .is_some_and(|x| x.split(',').any(|e| e.trim().starts_with("gzip")));
    if gzip_ok {
        // already compressed: CompressionLayer leaves responses with Content-Encoding alone
        h.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        (h, v.gzip.clone()).into_response()
    } else {
        let mut raw = Vec::new();
        match flate2::read::GzDecoder::new(&v.gzip[..]).read_to_end(&mut raw) {
            Ok(_) => (h, raw).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

/// Clients only listen; anything they send is ignored, so frames are kept tiny.
const WS_MAX_MESSAGE: usize = 4 << 10;
/// A client that cannot take a message for this long is dropped (it resyncs from REST).
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(10);

async fn ws(
    State(p): State<AppState>,
    Extension(g): Extension<Arc<Guard>>,
    client: Option<Extension<ClientIp>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let ip = client.map_or(std::net::IpAddr::from([0, 0, 0, 0]), |c| c.0 .0);
    let ticket = if g.limits.enabled {
        match g.ws.acquire(ip) {
            Some(t) => Some(t),
            None => {
                METRICS.inc("http_rejected_total", 1.0, &[("reason", "websocket")]);
                return (StatusCode::TOO_MANY_REQUESTS, "Too many live connections.")
                    .into_response();
            }
        }
    } else {
        None
    };
    upgrade
        .max_message_size(WS_MAX_MESSAGE)
        .max_frame_size(WS_MAX_MESSAGE)
        .on_upgrade(move |socket| async move {
            let _ticket = ticket; // frees the slot when the session ends
            ws_session(p, socket).await
        })
}

async fn send(socket: &mut WebSocket, m: Message) -> bool {
    matches!(
        tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(m)).await,
        Ok(Ok(()))
    )
}

async fn ws_session(p: AppState, mut socket: WebSocket) {
    let mut rx = p.listeners.subscribe();
    let hello = json!({"type": "hello", "status": p.status()}).to_string();
    if !send(&mut socket, Message::Text(hello.into())).await {
        return;
    }
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(text) => {
                    if !send(&mut socket, Message::Text(text)).await {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => continue, // slow client: drop; it resyncs from REST
                Err(RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            },
        }
    }
}
