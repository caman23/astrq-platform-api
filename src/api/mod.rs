//! REST + WebSocket API (same contract as the Python service, so the React dashboard is
//! unchanged). Read-only market analysis: there is no trading endpoint.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
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
        .route("/ws", get(ws));

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

async fn signals(State(p): State<AppState>, Query(q): Query<SignalQuery>) -> Response {
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
    let rows = p.store.lock().query(
        q.tool_id.as_deref(),
        q.symbol.as_deref(),
        q.side.as_deref(),
        min,
        limit,
    );
    Json(rows).into_response()
}

async fn alerts(State(p): State<AppState>) -> Json<Value> {
    let h: Vec<_> = p.router.history.lock().iter().rev().cloned().collect();
    Json(json!(h))
}

async fn summary(State(p): State<AppState>) -> Json<Value> {
    Json(json!({"text": *p.last_summary.lock()}))
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
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let Some(r) = p.runners.get(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"detail": "unknown tool"})),
        )
            .into_response();
    };
    let signals = p.store.lock().by_tool(&id);
    let view = r.tool.lock().view(&signals, &r.ctx, &params);
    match view {
        Some(v) => Json(v).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"detail": "tool has no view"})),
        )
            .into_response(),
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
                    if socket.send(Message::Text(text.to_string().into())).await.is_err() {
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
