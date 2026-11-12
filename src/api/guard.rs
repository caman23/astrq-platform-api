//! Abuse protection for the public API: who is the client, per-IP rate limiting, a cap on
//! requests in flight, a response timeout, security headers, and WebSocket quotas.
//!
//! Volumetric DDoS is Cloudflare's job (the origin is only reachable through the tunnel /
//! `origin_secret`); this layer keeps one client — or whatever gets through — from
//! exhausting CPU, memory, sockets or the view builder.

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::core::config::ApiLimits;
use crate::core::metrics::METRICS;
use crate::core::platform::Platform;

/// The client address a request is attributed to (inserted by [`guard`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientIp(pub IpAddr);

fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => v.is_loopback() || v.is_private() || v.is_link_local(),
        IpAddr::V6(v) => {
            v.is_loopback() || (v.segments()[0] & 0xfe00) == 0xfc00 // unique local
        }
    }
}

/// Forwarded-for headers are only believed when the request comes from our own proxy:
/// the Cloudflare tunnel on this machine (loopback), a PaaS proxy on a private network,
/// or anything carrying the origin secret (checked before this runs).
pub fn client_ip(headers: &HeaderMap, peer: Option<IpAddr>, origin_verified: bool) -> IpAddr {
    let fallback = peer.unwrap_or(IpAddr::from([0, 0, 0, 0]));
    if !(origin_verified || peer.is_some_and(is_internal)) {
        return fallback;
    }
    let header_ip = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<IpAddr>().ok())
    };
    // Cloudflare sets CF-Connecting-IP; other proxies append to X-Forwarded-For (the last
    // entry is the address the nearest proxy saw).
    header_ip("cf-connecting-ip")
        .or_else(|| {
            headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit(',').next())
                .and_then(|v| v.trim().parse().ok())
        })
        .unwrap_or(fallback)
}

/// IPv6 clients get a whole /64 each (one subscriber), so rotating addresses inside it
/// does not mint fresh rate-limit buckets.
fn bucket_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v) => {
            let s = v.segments();
            IpAddr::V6(Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0))
        }
        v4 => v4,
    }
}

/// Token bucket per client.
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    buckets: Mutex<HashMap<IpAddr, (f64, Instant)>>,
}

const MAX_TRACKED: usize = 50_000;

impl RateLimiter {
    pub fn new(rate: f64, burst: f64) -> Self {
        Self {
            rate: rate.max(0.1),
            burst: burst.max(1.0),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// `Err(seconds until a token is available)` when the client is over its budget.
    pub fn check(&self, ip: IpAddr) -> Result<(), f64> {
        let now = Instant::now();
        let mut b = self.buckets.lock();
        if b.len() > MAX_TRACKED {
            // idle buckets are full anyway: forgetting them changes nothing
            let idle = Duration::from_secs_f64(self.burst / self.rate);
            b.retain(|_, (_, t)| now.duration_since(*t) < idle);
            if b.len() > MAX_TRACKED {
                b.clear(); // address-spraying flood: start over rather than grow
            }
        }
        let (tokens, last) = b.entry(bucket_key(ip)).or_insert((self.burst, now));
        *tokens = (*tokens + now.duration_since(*last).as_secs_f64() * self.rate).min(self.burst);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(())
        } else {
            Err((1.0 - *tokens) / self.rate)
        }
    }
}

/// Live WebSocket sessions, in total and per client.
pub struct WsQuota {
    total: Arc<Semaphore>,
    per_ip: Mutex<HashMap<IpAddr, usize>>,
    max_per_ip: usize,
}

/// Held for the life of a WebSocket session; frees its slots when dropped.
pub struct WsTicket {
    _total: OwnedSemaphorePermit,
    ip: IpAddr,
    quota: Arc<WsQuota>,
}

impl Drop for WsTicket {
    fn drop(&mut self) {
        let mut m = self.quota.per_ip.lock();
        if let Some(n) = m.get_mut(&self.ip) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.ip);
            }
        }
    }
}

impl WsQuota {
    pub fn new(max_total: usize, max_per_ip: usize) -> Arc<Self> {
        Arc::new(Self {
            total: Arc::new(Semaphore::new(max_total.max(1))),
            per_ip: Mutex::new(HashMap::new()),
            max_per_ip: max_per_ip.max(1),
        })
    }

    pub fn acquire(self: &Arc<Self>, ip: IpAddr) -> Option<WsTicket> {
        let ip = bucket_key(ip);
        let total = self.total.clone().try_acquire_owned().ok()?;
        let mut m = self.per_ip.lock();
        let n = m.entry(ip).or_default();
        if *n >= self.max_per_ip {
            return None;
        }
        *n += 1;
        Some(WsTicket {
            _total: total,
            ip,
            quota: self.clone(),
        })
    }
}

pub struct Guard {
    pub limits: ApiLimits,
    pub rate: RateLimiter,
    pub in_flight: Arc<Semaphore>,
    pub ws: Arc<WsQuota>,
}

impl Guard {
    pub fn new(limits: &ApiLimits) -> Arc<Self> {
        Arc::new(Self {
            rate: RateLimiter::new(limits.per_ip_per_sec, limits.per_ip_burst),
            in_flight: Arc::new(Semaphore::new(limits.max_in_flight.max(1))),
            ws: WsQuota::new(limits.max_ws, limits.max_ws_per_ip),
            limits: limits.clone(),
        })
    }
}

fn too_many(retry_after: f64) -> Response {
    METRICS.inc("http_rejected_total", 1.0, &[("reason", "rate_limit")]);
    let secs = retry_after.ceil().max(1.0) as u64;
    let mut r = (
        StatusCode::TOO_MANY_REQUESTS,
        "Too many requests — please slow down.",
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
        r.headers_mut().insert(header::RETRY_AFTER, v);
    }
    r
}

fn busy(reason: &'static str) -> Response {
    METRICS.inc("http_rejected_total", 1.0, &[("reason", reason)]);
    let mut r = (
        StatusCode::SERVICE_UNAVAILABLE,
        "Server busy — try again shortly.",
    )
        .into_response();
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
    r
}

/// Outermost middleware: attribute, rate-limit, cap concurrency, time out.
pub async fn guard(
    State((p, g)): State<(Arc<Platform>, Arc<Guard>)>,
    mut req: Request,
    next: Next,
) -> Response {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    // only a request that proves it came through Cloudflare may name its client; otherwise
    // anyone reaching the origin could drain another visitor's budget with a fake header
    let origin_verified = super::origin_ok(&p, req.headers()) || p.s.api.trust_proxy_headers;
    let ip = client_ip(req.headers(), peer, origin_verified);
    req.extensions_mut().insert(ClientIp(ip));
    if !g.limits.enabled || req.uri().path() == "/health" {
        return next.run(req).await;
    }
    if let Err(wait) = g.rate.check(ip) {
        return too_many(wait);
    }
    let Ok(_permit) = g.in_flight.clone().try_acquire_owned() else {
        return busy("in_flight");
    };
    let limit = Duration::from_secs_f64(g.limits.request_timeout_sec.max(1.0));
    match tokio::time::timeout(limit, next.run(req)).await {
        Ok(r) => r,
        Err(_) => busy("timeout"),
    }
}

/// Security headers on every response. The page only loads its own scripts and styles;
/// `frame-ancestors` lists the sites allowed to embed the radar.
pub fn security_headers(frame_ancestors: &[String]) -> Vec<(header::HeaderName, HeaderValue)> {
    let frame = if frame_ancestors.is_empty() {
        "'none'".to_string()
    } else {
        frame_ancestors.join(" ")
    };
    let csp = format!(
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; font-src 'self'; connect-src 'self' wss: ws:; \
         object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors {frame}"
    );
    vec![
        (
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_str(&csp)
                .unwrap_or(HeaderValue::from_static("frame-ancestors 'self'")),
        ),
        (
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ),
        (
            header::REFERRER_POLICY,
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        ),
        (
            header::HeaderName::from_static("permissions-policy"),
            HeaderValue::from_static("camera=(), microphone=(), geolocation=(), payment=()"),
        ),
        (
            header::HeaderName::from_static("cross-origin-opener-policy"),
            HeaderValue::from_static("same-origin"),
        ),
    ]
}

/// `/metrics` exposes internals: only for direct local requests (not via the tunnel or a
/// proxy), a bearer token, or when explicitly made public.
pub fn metrics_allowed(p: &Platform, headers: &HeaderMap, peer: Option<IpAddr>) -> bool {
    let api = &p.s.api;
    if api.metrics_public {
        return true;
    }
    if let Some(token) = &api.metrics_token {
        let want = format!("Bearer {}", token.expose());
        let got = headers
            .get(header::AUTHORIZATION)
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if super::constant_time_eq(got, want.as_bytes()) {
            return true;
        }
    }
    let proxied = [
        "cf-connecting-ip",
        "x-forwarded-for",
        "x-astrq-origin",
        "cf-ray",
    ]
    .iter()
    .any(|h| headers.contains_key(*h));
    peer.is_some_and(|ip| ip.is_loopback()) && !proxied
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_allows_burst_then_limits() {
        let r = RateLimiter::new(1.0, 3.0);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        assert!(r.check(ip).is_ok());
        assert!(r.check(ip).is_ok());
        assert!(r.check(ip).is_ok());
        let wait = r.check(ip).unwrap_err();
        assert!(wait > 0.0 && wait <= 1.0);
        // another client is unaffected
        assert!(r.check("203.0.113.8".parse().unwrap()).is_ok());
    }

    #[test]
    fn ipv6_clients_share_their_64() {
        let r = RateLimiter::new(1.0, 1.0);
        assert!(r.check("2001:db8:1:2::1".parse().unwrap()).is_ok());
        assert!(r.check("2001:db8:1:2::ffff".parse().unwrap()).is_err());
        assert!(r.check("2001:db8:1:3::1".parse().unwrap()).is_ok());
    }

    #[test]
    fn forwarded_headers_only_from_our_proxy() {
        let mut h = HeaderMap::new();
        h.insert("cf-connecting-ip", "198.51.100.9".parse().unwrap());
        h.insert("x-forwarded-for", "1.1.1.1, 198.51.100.10".parse().unwrap());
        let public: IpAddr = "203.0.113.1".parse().unwrap();
        // straight from the internet: spoofed headers ignored
        assert_eq!(client_ip(&h, Some(public), false), public);
        // via the local tunnel or with a verified origin secret: believed
        let lo: IpAddr = "127.0.0.1".parse().unwrap();
        assert_eq!(
            client_ip(&h, Some(lo), false),
            "198.51.100.9".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            client_ip(&h, Some(public), true),
            "198.51.100.9".parse::<IpAddr>().unwrap()
        );
        // a private-network proxy without Cloudflare: last X-Forwarded-For hop
        h.remove("cf-connecting-ip");
        let lan: IpAddr = "10.0.0.5".parse().unwrap();
        assert_eq!(
            client_ip(&h, Some(lan), false),
            "198.51.100.10".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn websocket_quota_per_ip_and_total() {
        let q = WsQuota::new(3, 2);
        let a: IpAddr = "203.0.113.1".parse().unwrap();
        let b: IpAddr = "203.0.113.2".parse().unwrap();
        let t1 = q.acquire(a).unwrap();
        let _t2 = q.acquire(a).unwrap();
        assert!(q.acquire(a).is_none(), "per-ip cap");
        let _t3 = q.acquire(b).unwrap();
        assert!(q.acquire(b).is_none(), "total cap");
        drop(t1);
        assert!(q.acquire(a).is_some(), "slot freed on drop");
    }
}
