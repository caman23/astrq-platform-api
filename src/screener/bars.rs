//! Daily bars (OHLCV) for the screener, from DNSE OpenAPI or a deterministic synthetic
//! source, with a per-symbol cache.
//!
//! DNSE `GET /price/ohlc` — verified against the official spec page (developers.dnse.com.vn,
//! "Lịch sử OHLC", OpenAPI v2) and the official `dnse` 0.5.0 Python SDK (`_hmac_signer.py`):
//! - query: `symbol`, `type=STOCK`, `resolution=1D`, `from`/`to` (Unix seconds);
//! - headers: `x-api-key`, a date header (`x-aux-date` per the spec, or `date`), and
//!   `X-Signature: Signature keyId="<key>",algorithm="hmac-sha256",headers="(request-target)
//!   <date header>",signature="<sig>",nonce="<nonce>"`, where `sig` = URL-encoded base64 of
//!   HMAC-SHA256(secret, "(request-target): get <path>\n<date header>: <date>\nnonce: <n>").
//!   The signed path has no query string (the SDK signs the path and sends params apart);
//! - response: column arrays `t,o,h,l,c,v` + `nextTime` (0 = no more data).
//!
//! TODO(verify) on live data: price unit (spec examples look like thousand VND — handled
//! by `provider.dnse.price_unit`), and which way `nextTime` pages (both are handled).

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use base64::Engine;
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Utc, Weekday};
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use rand::Rng;
use rand_distr::{Distribution, Normal};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::core::config::{DnseConfig, ScreenerConfig};
use crate::core::market_rules::MarketRules;
use crate::providers::synthetic::{seeded, synthetic_profile};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct DailyBar {
    pub date: NaiveDate,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[async_trait]
pub trait BarsSource: Send + Sync {
    fn name(&self) -> &'static str;
    /// Daily bars of `symbol` between `from` and `to` (inclusive), oldest first.
    async fn daily(&self, symbol: &str, from: NaiveDate, to: NaiveDate) -> Result<Vec<DailyBar>>;
}

// ---------------------------------------------------------------- DNSE

pub const OHLC_PATH: &str = "/price/ohlc";

pub struct DnseBars {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    date_header: String,
    price_unit: String,
    tz: chrono_tz::Tz,
}

/// URL-encode only the three characters base64 can contain (as the auth guide says).
fn encode_b64(s: &str) -> String {
    s.replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
}

/// Headers for one signed request (fresh date + nonce every time).
pub fn sign(
    method: &str,
    path: &str,
    api_key: &str,
    api_secret: &str,
    date_header: &str,
    date: &str,
    nonce: &str,
) -> Vec<(String, String)> {
    let signing = format!(
        "(request-target): {} {path}\n{date_header}: {date}\nnonce: {nonce}",
        method.to_ascii_lowercase()
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).expect("hmac accepts any key");
    mac.update(signing.as_bytes());
    let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    vec![
        ("x-api-key".into(), api_key.into()),
        (date_header.into(), date.into()),
        (
            "X-Signature".into(),
            format!(
                "Signature keyId=\"{api_key}\",algorithm=\"hmac-sha256\",headers=\"(request-target) {date_header}\",signature=\"{}\",nonce=\"{nonce}\"",
                encode_b64(&sig)
            ),
        ),
    ]
}

#[derive(Deserialize)]
struct OhlcPage {
    #[serde(default)]
    t: Vec<i64>,
    #[serde(default)]
    o: Vec<f64>,
    #[serde(default)]
    h: Vec<f64>,
    #[serde(default)]
    l: Vec<f64>,
    #[serde(default)]
    c: Vec<f64>,
    #[serde(default)]
    v: Vec<f64>,
    #[serde(default, rename = "nextTime")]
    next_time: i64,
}

impl DnseBars {
    pub fn new(cfg: &ScreenerConfig, dnse: &DnseConfig, tz: chrono_tz::Tz) -> Option<Self> {
        let api_key = dnse.api_key.clone().filter(|k| !k.is_empty())?;
        let api_secret = dnse.api_secret.as_ref()?.expose().to_string();
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .ok()?;
        Some(Self {
            http,
            base_url: cfg.dnse_rest_url.trim_end_matches('/').to_string(),
            api_key,
            api_secret,
            date_header: cfg.dnse_date_header.to_ascii_lowercase(),
            price_unit: dnse.price_unit.clone(),
            tz,
        })
    }

    fn price(&self, raw: f64) -> f64 {
        match self.price_unit.as_str() {
            "thousand" => raw * 1000.0,
            "auto" if raw < 1000.0 => raw * 1000.0,
            _ => raw,
        }
    }

    async fn page(&self, symbol: &str, from: i64, to: i64) -> Result<OhlcPage> {
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S +0000").to_string();
        let nonce = hex::encode(rand::thread_rng().gen::<[u8; 16]>());
        let mut req = self
            .http
            .get(format!("{}{OHLC_PATH}", self.base_url))
            .query(&[
                ("symbol", symbol.to_string()),
                ("type", "STOCK".to_string()),
                ("resolution", "1D".to_string()),
                ("from", from.to_string()),
                ("to", to.to_string()),
            ]);
        for (k, v) in sign(
            "GET",
            OHLC_PATH,
            &self.api_key,
            &self.api_secret,
            &self.date_header,
            &date,
            &nonce,
        ) {
            req = req.header(k, v);
        }
        let r = req.send().await.context("DNSE ohlc request")?;
        let status = r.status();
        let body = r.text().await.unwrap_or_default();
        if !status.is_success() {
            // the body carries an error code (OA-xxx), never our credentials
            bail!(
                "DNSE ohlc {symbol}: HTTP {} {}",
                status.as_u16(),
                body.chars().take(200).collect::<String>()
            );
        }
        serde_json::from_str(&body).with_context(|| format!("DNSE ohlc {symbol}: bad JSON"))
    }
}

#[async_trait]
impl BarsSource for DnseBars {
    fn name(&self) -> &'static str {
        "dnse"
    }

    async fn daily(&self, symbol: &str, from: NaiveDate, to: NaiveDate) -> Result<Vec<DailyBar>> {
        let ts = |d: NaiveDate, end: bool| {
            let t = if end {
                d.and_hms_opt(23, 59, 59)
            } else {
                d.and_hms_opt(0, 0, 0)
            }
            .expect("valid time");
            self.tz
                .from_local_datetime(&t)
                .earliest()
                .map_or(0, |x| x.timestamp())
        };
        let (lo, hi) = (ts(from, false), ts(to, true));
        let mut by_date: std::collections::BTreeMap<NaiveDate, DailyBar> = Default::default();
        let (mut a, mut b) = (lo, hi);
        for _ in 0..6 {
            let p = self.page(symbol, a, b).await?;
            let n =
                p.t.len()
                    .min(p.o.len())
                    .min(p.h.len())
                    .min(p.l.len())
                    .min(p.c.len());
            for i in 0..n {
                let Some(dt) = self.tz.timestamp_opt(p.t[i], 0).single() else {
                    continue;
                };
                let d = dt.date_naive();
                if d < from || d > to || p.c[i].is_nan() || p.c[i] <= 0.0 {
                    continue;
                }
                by_date.insert(
                    d,
                    DailyBar {
                        date: d,
                        open: self.price(p.o[i]),
                        high: self.price(p.h[i]),
                        low: self.price(p.l[i]),
                        close: self.price(p.c[i]),
                        volume: p.v.get(i).copied().unwrap_or(0.0),
                    },
                );
            }
            // nextTime ≠ 0: the answer was cut short; continue towards whichever end is
            // still missing (direction not stated by the spec — TODO(verify))
            let next = p.next_time;
            if next <= 0 || n == 0 {
                break;
            }
            let (min_t, max_t) = (p.t.iter().min().copied(), p.t.iter().max().copied());
            match (min_t, max_t) {
                (Some(mn), _) if next < mn && next > lo => b = next,
                (_, Some(mx)) if next > mx && next < hi => a = next,
                _ => break,
            }
        }
        Ok(by_date.into_values().collect())
    }
}

// ---------------------------------------------------------------- synthetic

/// Deterministic daily history per symbol (seeded), ending near the synthetic market's
/// reference price: drifting regimes so that trend, momentum and oversold screens all
/// find something. For demos and tests only.
pub struct SyntheticBars {
    pub seed: u64,
    pub rules: Arc<MarketRules>,
}

pub fn weekdays_back(to: NaiveDate, n: usize) -> Vec<NaiveDate> {
    let mut out = Vec::with_capacity(n);
    let mut d = to;
    while out.len() < n {
        if !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
            out.push(d);
        }
        d -= Duration::days(1);
    }
    out.reverse();
    out
}

#[async_trait]
impl BarsSource for SyntheticBars {
    fn name(&self) -> &'static str {
        "synthetic"
    }

    async fn daily(&self, symbol: &str, from: NaiveDate, to: NaiveDate) -> Result<Vec<DailyBar>> {
        let profile = synthetic_profile(symbol, self.seed, &self.rules);
        // the history only depends on the symbol and the seed (stable across days)
        let mut rng = seeded(&format!("bars:{}:{symbol}", self.seed));
        let days = weekdays_back(to, 320);
        let noise = Normal::new(0.0, 1.0).expect("normal");
        let vol = rng.gen_range(0.012..0.028);
        let mut drift = 0.0;
        let mut logp = 0.0f64;
        let mut path = Vec::with_capacity(days.len());
        for _ in &days {
            if rng.gen_bool(0.03) {
                drift = rng.gen_range(-0.004..0.004); // new regime
            }
            logp += drift + vol * noise.sample(&mut rng);
            path.push(logp);
        }
        let shift = profile.ref_price.ln() - path.last().copied().unwrap_or(0.0);
        let base_vol = profile.trades_per_min * 270.0 * profile.median_shares as f64;
        let mut prev = (path[0] + shift).exp();
        let mut out = Vec::new();
        for (d, lp) in days.iter().zip(&path) {
            let close = self.rules.round_to_tick((lp + shift).exp(), "HOSE");
            let open = self
                .rules
                .round_to_tick(prev * (1.0 + 0.4 * vol * noise.sample(&mut rng)), "HOSE");
            let span = close.max(open) * vol * rng.gen_range(0.1..0.9);
            let high = self.rules.round_to_tick(close.max(open) + span, "HOSE");
            let low = self
                .rules
                .round_to_tick((close.min(open) - span).max(1.0), "HOSE");
            let spike = if rng.gen_bool(0.05) {
                rng.gen_range(1.8..3.5)
            } else {
                1.0
            };
            let volume =
                (base_vol * (0.25 * noise.sample(&mut rng)).exp() * spike / 100.0).round() * 100.0;
            prev = close;
            if *d >= from && *d <= to {
                out.push(DailyBar {
                    date: *d,
                    open,
                    high,
                    low,
                    close,
                    volume: volume.max(0.0),
                });
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------- cache

struct Entry {
    fetched: Instant,
    bars: Arc<Vec<DailyBar>>,
}

/// Bars per symbol, reused for `ttl` (shorter while the market is open, when today's bar
/// still moves). Failed fetches are not cached.
#[derive(Default)]
pub struct BarCache {
    entries: Mutex<HashMap<String, Entry>>,
}

impl BarCache {
    pub fn get(&self, symbol: &str, ttl: std::time::Duration) -> Option<Arc<Vec<DailyBar>>> {
        self.entries
            .lock()
            .get(symbol)
            .filter(|e| e.fetched.elapsed() < ttl)
            .map(|e| e.bars.clone())
    }

    pub fn put(&self, symbol: &str, bars: Vec<DailyBar>) -> Arc<Vec<DailyBar>> {
        let bars = Arc::new(bars);
        self.entries.lock().insert(
            symbol.to_string(),
            Entry {
                fetched: Instant::now(),
                bars: bars.clone(),
            },
        );
        bars
    }

    pub fn oldest_age(&self) -> Option<std::time::Duration> {
        self.entries
            .lock()
            .values()
            .map(|e| e.fetched.elapsed())
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Same inputs as the official SDK's signer → same signature string layout.
    #[test]
    fn signature_layout_matches_the_sdk() {
        let h = sign(
            "GET",
            "/price/ohlc",
            "key1",
            "secret1",
            "x-aux-date",
            "Fri, 15 May 2026 07:11:30 +0000",
            "c9a8f88b472c9721fde161e0d89df8cc",
        );
        let m: HashMap<_, _> = h.into_iter().collect();
        assert_eq!(m["x-api-key"], "key1");
        assert_eq!(m["x-aux-date"], "Fri, 15 May 2026 07:11:30 +0000");
        let sig = &m["X-Signature"];
        assert!(sig.starts_with(
            "Signature keyId=\"key1\",algorithm=\"hmac-sha256\",headers=\"(request-target) x-aux-date\",signature=\""
        ));
        assert!(sig.ends_with(",nonce=\"c9a8f88b472c9721fde161e0d89df8cc\""));
        // recompute independently
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret1").unwrap();
        mac.update(
            b"(request-target): get /price/ohlc\nx-aux-date: Fri, 15 May 2026 07:11:30 +0000\nnonce: c9a8f88b472c9721fde161e0d89df8cc",
        );
        let want = encode_b64(
            &base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()),
        );
        assert!(sig.contains(&format!("signature=\"{want}\"")));
        assert!(!want.contains('+') && !want.contains('/') && !want.contains('='));
    }

    #[tokio::test]
    async fn synthetic_bars_are_deterministic_weekdays() {
        let src = SyntheticBars {
            seed: 7,
            rules: Arc::new(MarketRules::new(&Default::default())),
        };
        let to = NaiveDate::from_ymd_opt(2026, 11, 13).unwrap();
        let a = src
            .daily("FPT", to - Duration::days(400), to)
            .await
            .unwrap();
        let b = src
            .daily("FPT", to - Duration::days(400), to)
            .await
            .unwrap();
        assert_eq!(a, b);
        assert!(a.len() > 250);
        assert!(a
            .iter()
            .all(|x| !matches!(x.date.weekday(), Weekday::Sat | Weekday::Sun)));
        assert!(a
            .iter()
            .all(|x| x.low <= x.open.min(x.close) && x.high >= x.open.max(x.close)));
        assert_eq!(a.last().unwrap().date, to);
    }
}
