//! REST + WebSocket API (same contract as the Python service, so the React dashboard is
//! unchanged). Read-only market analysis: there is no trading endpoint.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Extension;
use axum::{Json, Router};
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::core::metrics::METRICS;
use crate::core::models::Severity;
use crate::core::platform::Platform;

type AppState = Arc<Platform>;

pub fn router(platform: Arc<Platform>) -> Router {
    let s = &platform.s;
    let mut app = Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/ready", get(ready))
        .route("/metrics", get(|| async { METRICS.render_prometheus() }))
        .route("/api/status", get(status))
        .route("/api/universe", get(universe))
        .route("/api/signals", get(signals))
        .route("/api/alerts", get(alerts))
        .route("/api/summary", get(summary))
        .route("/api/tools", get(tools))
        .route("/api/tools/{id}/enabled", post(set_enabled))
        .route("/api/tools/{id}/view", get(tool_view))
        .route("/ws", get(ws))
        .layer(Extension(Arc::new(ResponseCache::default())));

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
    let frame = if s.api.frame_ancestors.is_empty() {
        "'none'".to_string()
    } else {
        s.api.frame_ancestors.join(" ")
    };
    let csp = HeaderValue::from_str(&format!("frame-ancestors {frame}"))
        .unwrap_or(HeaderValue::from_static("frame-ancestors 'self'"));

    app.layer(middleware::from_fn_with_state(
        platform.clone(),
        origin_secret,
    ))
    // Only the configured sites may put the radar in an <iframe> (anti-clickjacking).
    .layer(SetResponseHeaderLayer::overriding(
        header::CONTENT_SECURITY_POLICY,
        csp,
    ))
    // Compress JSON (the radar view is large and free hosts meter egress).
    .layer(CompressionLayer::new())
    .layer(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(tower_http::cors::Any),
    )
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

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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

#[derive(Deserialize)]
struct SignalQuery {
    tool_id: Option<String>,
    symbol: Option<String>,
    side: Option<String>,
    min_severity: Option<String>,
    limit: Option<usize>,
}

async fn signals(
    State(p): State<AppState>,
    Extension(cache): Extension<Arc<ResponseCache>>,
    Query(q): Query<SignalQuery>,
    headers: HeaderMap,
) -> Response {
    let limit = q.limit.unwrap_or(200);
    if limit > 2000 {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"detail": "limit must be <= 2000"})),
        )
            .into_response();
    }
    let min = match q.min_severity.as_deref() {
        None => None,
        Some(s) => match Severity::parse(s) {
            Some(v) => Some(v),
            None => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"detail": "bad min_severity"})),
                )
                    .into_response()
            }
        },
    };
    let key = format!(
        "signals?tool_id={:?}&symbol={:?}&side={:?}&min={:?}&limit={limit}",
        q.tool_id,
        q.symbol,
        q.side,
        min.map(|m| m.as_str())
    );
    let p2 = p.clone();
    let built = cache
        .get_or_build(&p, key, move || {
            let (version, rows) = {
                let st = p2.store.lock();
                let rows = st.query(
                    q.tool_id.as_deref(),
                    q.symbol.as_deref(),
                    q.side.as_deref(),
                    min,
                    limit,
                );
                (st.version, rows)
            };
            Some((version, serde_json::to_vec(&rows).ok()?))
        })
        .await;
    match built {
        Some(v) => cached_response(&v, &headers),
        None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn alerts(State(p): State<AppState>) -> Json<Value> {
    let h: Vec<_> = p.router.history.lock().iter().rev().cloned().collect();
    Json(json!(h))
}

async fn summary(State(p): State<AppState>) -> Json<Value> {
    Json(json!({"text": *p.last_summary.lock()}))
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

/// Dashboards and iframes re-fetch the radar view after every signal, so N viewers would
/// otherwise rebuild the same JSON N times. Entries are keyed by path + normalized query
/// and reused while the signal store is unchanged — and for at most `CACHE_MAX_AGE`, so
/// day totals that move without a new signal still show up. Concurrent misses build once.
#[derive(Default)]
struct ResponseCache {
    entries: parking_lot::Mutex<(HashMap<String, Arc<Cached>>, usize)>,
    build: tokio::sync::Mutex<()>,
}

const CACHE_MAX_AGE: Duration = Duration::from_secs(2);
/// Memory bound (free hosts have 512 MB); arbitrary query values cannot grow it further.
const CACHE_MAX_BYTES: usize = 32 << 20;
const VIEW_PARAMS: [&str; 7] = [
    "symbol",
    "full",
    "orders",
    "orders_with_trades",
    "trades",
    "at",
    "sectors",
];

impl ResponseCache {
    fn fresh(&self, key: &str, version: u64) -> Option<Arc<Cached>> {
        self.entries
            .lock()
            .0
            .get(key)
            .filter(|c| c.version == version && c.built.elapsed() < CACHE_MAX_AGE)
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

    /// Serve `key` from the cache, or run `build` (on a blocking thread) once for all
    /// concurrent callers. `build` returns the data version it rendered and the JSON.
    async fn get_or_build<F>(&self, p: &Platform, key: String, build: F) -> Option<Arc<Cached>>
    where
        F: FnOnce() -> Option<(u64, Vec<u8>)> + Send + 'static,
    {
        let version = p.store.lock().version;
        if let Some(v) = self.fresh(&key, version) {
            return Some(v);
        }
        let _one_builder = self.build.lock().await;
        let version = p.store.lock().version; // may have moved while we waited
        if let Some(v) = self.fresh(&key, version) {
            return Some(v);
        }
        let built = tokio::task::spawn_blocking(move || {
            let (version, raw) = build()?;
            Cached::encode(version, raw)
        })
        .await
        .ok()
        .flatten()?;
        let built = Arc::new(built);
        self.put(key, built.clone());
        Some(built)
    }
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
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"detail": "unknown tool"})),
        )
            .into_response(),
    }
}

async fn tool_view(
    State(p): State<AppState>,
    Extension(cache): Extension<Arc<ResponseCache>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let not_found =
        |detail: &str| (StatusCode::NOT_FOUND, Json(json!({ "detail": detail }))).into_response();
    if !p.runners.contains_key(&id) {
        return not_found("unknown tool");
    }
    // Only known parameters take part in the key, so random query strings cannot
    // fill the cache or bypass it.
    let mut params = params;
    params.retain(|k, _| VIEW_PARAMS.contains(&k.as_str()));
    if let Some(at) = params.get("at") {
        let ok = chrono::NaiveTime::parse_from_str(at, "%H:%M").is_ok()
            || chrono::NaiveTime::parse_from_str(at, "%H:%M:%S").is_ok();
        if !ok {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"detail": "at must be HH:MM or HH:MM:SS (Vietnam time)"})),
            )
                .into_response();
        }
    }
    if let Some(sym) = params.get_mut("symbol") {
        *sym = sym.to_ascii_uppercase();
    }
    let mut parts: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
    parts.sort();
    let key = format!("{id}?{}", parts.join("&"));

    let (p2, params2) = (p.clone(), params);
    let built = cache
        .get_or_build(&p, key, move || render_view(&p2, &id, &params2))
        .await;
    match built {
        Some(v) => cached_response(&v, &headers),
        None => not_found("tool has no view"),
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

async fn ws(State(p): State<AppState>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| ws_session(p, socket))
}

async fn ws_session(p: AppState, mut socket: WebSocket) {
    let mut rx = p.listeners.subscribe();
    let hello = json!({"type": "hello", "status": p.status()}).to_string();
    if socket.send(Message::Text(hello.into())).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(text) => {
                    if socket.send(Message::Text(text)).await.is_err() {
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
