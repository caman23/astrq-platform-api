//! Indicator screener (Radar chỉ báo): on demand, filter the universe by conditions on
//! daily-bar indicators (RSI, MA, MACD, Bollinger, …). Not a realtime tool — nothing runs
//! until someone asks; bars are cached per symbol.

pub mod bars;
pub mod indicators;

use anyhow::Result;
use futures_util::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::clock::Clock;
use crate::core::config::Settings;
use crate::core::market_rules::MarketRules;
use crate::core::models::Session;
use crate::core::sessions::SessionCalendar;
use crate::core::universe::Universe;
use bars::{BarCache, BarsSource, DailyBar, DnseBars, SyntheticBars};
use indicators as ind;

// ---------------------------------------------------------------- request model

/// One side of a comparison: an indicator with parameters, or a constant.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(default)]
pub struct Operand {
    /// indicator id (see [`catalog`]); absent = constant `value`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fast: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slow: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Condition {
    pub left: Operand,
    pub op: String,
    pub right: Operand,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub struct ScanRequest {
    pub conditions: Vec<Condition>,
    /// "all" (every condition) | "any" (at least one)
    pub logic: String,
    /// restrict to these symbols (e.g. the watchlist); default = whole universe
    pub symbols: Option<Vec<String>>,
}

impl Default for ScanRequest {
    fn default() -> Self {
        Self {
            conditions: vec![],
            logic: "all".into(),
            symbols: None,
        }
    }
}

pub const MAX_CONDITIONS: usize = 8;
pub const OPS: [&str; 6] = ["gt", "gte", "lt", "lte", "cross_above", "cross_below"];

#[derive(Debug)]
pub enum ScanError {
    Invalid(String),
    Busy,
    Source(String),
}

// ---------------------------------------------------------------- catalog

struct Param {
    name: &'static str,
    default: f64,
    min: f64,
    max: f64,
}

struct Spec {
    id: &'static str,
    label: &'static str,
    unit: &'static str,
    params: &'static [Param],
    fields: &'static [(&'static str, &'static str)],
    hint: &'static str,
}

macro_rules! p {
    ($name:literal, $d:expr, $lo:expr, $hi:expr) => {
        Param {
            name: $name,
            default: $d,
            min: $lo,
            max: $hi,
        }
    };
}

static SPECS: &[Spec] = &[
    Spec {
        id: "close",
        label: "Giá đóng cửa",
        unit: "VND",
        params: &[],
        fields: &[],
        hint: "Giá đóng cửa phiên gần nhất",
    },
    Spec {
        id: "volume",
        label: "Khối lượng",
        unit: "cp",
        params: &[],
        fields: &[],
        hint: "Khối lượng khớp phiên gần nhất",
    },
    Spec {
        id: "sma",
        label: "MA (trung bình động)",
        unit: "VND",
        params: &[p!("n", 20.0, 2.0, 250.0)],
        fields: &[],
        hint: "Trung bình giá đóng cửa n phiên",
    },
    Spec {
        id: "ema",
        label: "EMA",
        unit: "VND",
        params: &[p!("n", 20.0, 2.0, 250.0)],
        fields: &[],
        hint: "Trung bình động lũy thừa n phiên",
    },
    Spec {
        id: "rsi",
        label: "RSI",
        unit: "",
        params: &[p!("n", 14.0, 2.0, 100.0)],
        fields: &[],
        hint: "< 30 quá bán · > 70 quá mua",
    },
    Spec {
        id: "macd",
        label: "MACD",
        unit: "",
        params: &[
            p!("fast", 12.0, 2.0, 100.0),
            p!("slow", 26.0, 3.0, 200.0),
            p!("signal", 9.0, 2.0, 50.0),
        ],
        fields: &[
            ("line", "Đường MACD"),
            ("signal", "Đường tín hiệu"),
            ("hist", "Histogram"),
        ],
        hint: "MACD cắt lên đường tín hiệu: động lượng tăng",
    },
    Spec {
        id: "bb",
        label: "Bollinger Bands",
        unit: "",
        params: &[p!("n", 20.0, 5.0, 100.0), p!("k", 2.0, 0.5, 4.0)],
        fields: &[
            ("upper", "Dải trên"),
            ("lower", "Dải dưới"),
            ("pctb", "%B (0 = dải dưới, 1 = dải trên)"),
        ],
        hint: "%B < 0: đóng cửa dưới dải dưới",
    },
    Spec {
        id: "stoch",
        label: "Stochastic %K",
        unit: "",
        params: &[p!("n", 14.0, 3.0, 100.0)],
        fields: &[],
        hint: "< 20 quá bán · > 80 quá mua",
    },
    Spec {
        id: "roc",
        label: "% thay đổi giá",
        unit: "%",
        params: &[p!("n", 20.0, 1.0, 250.0)],
        fields: &[],
        hint: "Tăng/giảm bao nhiêu % sau n phiên",
    },
    Spec {
        id: "vol_ratio",
        label: "KL / TB KL",
        unit: "lần",
        params: &[p!("n", 20.0, 2.0, 120.0)],
        fields: &[],
        hint: "> 2: khối lượng đột biến so với TB n phiên trước",
    },
    Spec {
        id: "avg_value",
        label: "GTGD trung bình",
        unit: "tỷ",
        params: &[p!("n", 20.0, 1.0, 120.0)],
        fields: &[],
        hint: "Giá trị giao dịch TB n phiên (tỷ đồng) — lọc thanh khoản",
    },
    Spec {
        id: "atr",
        label: "ATR (% biến động)",
        unit: "%",
        params: &[p!("n", 14.0, 2.0, 100.0)],
        fields: &[],
        hint: "Biên độ dao động trung bình theo % giá",
    },
    Spec {
        id: "from_high",
        label: "% cách đỉnh",
        unit: "%",
        params: &[p!("n", 250.0, 5.0, 250.0)],
        fields: &[],
        hint: "≥ -3: sát đỉnh n phiên (250 ≈ 1 năm)",
    },
    Spec {
        id: "from_low",
        label: "% trên đáy",
        unit: "%",
        params: &[p!("n", 250.0, 5.0, 250.0)],
        fields: &[],
        hint: "≤ 5: sát đáy n phiên",
    },
];

fn spec(id: &str) -> Option<&'static Spec> {
    SPECS.iter().find(|s| s.id == id)
}

/// Ready-made screens shown as one-click presets.
pub fn presets() -> Value {
    let i = |id: &str| json!({"ind": id});
    let n = |id: &str, n: u32| json!({"ind": id, "n": n});
    let v = |x: f64| json!({"value": x});
    let c = |l: Value, op: &str, r: Value| json!({"left": l, "op": op, "right": r});
    json!([
        {"id": "oversold", "label": "RSI quá bán", "conditions": [c(n("rsi", 14), "lt", v(30.0))]},
        {"id": "overbought", "label": "RSI quá mua", "conditions": [c(n("rsi", 14), "gt", v(70.0))]},
        {"id": "uptrend", "label": "Xu hướng tăng (Giá > MA20 > MA50)", "conditions": [c(i("close"), "gt", n("sma", 20)), c(n("sma", 20), "gt", n("sma", 50))]},
        {"id": "ma_cross", "label": "Giá cắt lên MA20", "conditions": [c(i("close"), "cross_above", n("sma", 20))]},
        {"id": "golden", "label": "MA50 cắt lên MA200", "conditions": [c(n("sma", 50), "cross_above", n("sma", 200))]},
        {"id": "macd_up", "label": "MACD cắt lên tín hiệu", "conditions": [c(json!({"ind": "macd", "field": "line"}), "cross_above", json!({"ind": "macd", "field": "signal"}))]},
        {"id": "volume_spike", "label": "Khối lượng đột biến (> 2× TB20)", "conditions": [c(n("vol_ratio", 20), "gt", v(2.0)), c(n("avg_value", 20), "gt", v(5.0))]},
        {"id": "breakout", "label": "Sát đỉnh 1 năm + KL tăng", "conditions": [c(n("from_high", 250), "gte", v(-3.0)), c(n("vol_ratio", 20), "gt", v(1.5))]},
        {"id": "bb_low", "label": "Đóng cửa dưới dải Bollinger dưới", "conditions": [c(json!({"ind": "bb", "field": "pctb"}), "lt", v(0.0))]},
    ])
}

pub fn catalog() -> Value {
    json!(SPECS
        .iter()
        .map(|s| json!({
            "id": s.id,
            "label": s.label,
            "unit": s.unit,
            "hint": s.hint,
            "params": s.params.iter().map(|p| json!({"name": p.name, "default": p.default, "min": p.min, "max": p.max})).collect::<Vec<_>>(),
            "fields": s.fields.iter().map(|(id, l)| json!({"id": id, "label": l})).collect::<Vec<_>>(),
        }))
        .collect::<Vec<_>>())
}

// ---------------------------------------------------------------- validation

impl Operand {
    /// Fill defaults, check bounds; returns the canonical operand.
    fn normalize(&self) -> Result<Operand, String> {
        match (&self.ind, self.value) {
            (None, Some(v)) if v.is_finite() && v.abs() < 1e15 => Ok(Operand {
                value: Some(v),
                ..Default::default()
            }),
            (None, _) => Err("mỗi vế cần một chỉ báo hoặc một số".into()),
            (Some(id), _) => {
                let s = spec(id).ok_or_else(|| format!("chỉ báo không hỗ trợ: {id}"))?;
                let mut o = Operand {
                    ind: Some(s.id.to_string()),
                    ..Default::default()
                };
                for p in s.params {
                    let raw = match p.name {
                        "n" => self.n.map(f64::from),
                        "fast" => self.fast.map(f64::from),
                        "slow" => self.slow.map(f64::from),
                        "signal" => self.signal.map(f64::from),
                        "k" => self.k,
                        _ => None,
                    }
                    .unwrap_or(p.default);
                    if !(raw.is_finite() && raw >= p.min && raw <= p.max) {
                        return Err(format!(
                            "{} {}: {} ngoài khoảng {}–{}",
                            s.label, p.name, raw, p.min, p.max
                        ));
                    }
                    match p.name {
                        "n" => o.n = Some(raw as u32),
                        "fast" => o.fast = Some(raw as u32),
                        "slow" => o.slow = Some(raw as u32),
                        "signal" => o.signal = Some(raw as u32),
                        "k" => o.k = Some((raw * 100.0).round() / 100.0),
                        _ => {}
                    }
                }
                if let (Some(f), Some(sl)) = (o.fast, o.slow) {
                    if f >= sl {
                        return Err("MACD: fast phải nhỏ hơn slow".into());
                    }
                }
                if !s.fields.is_empty() {
                    let f = self
                        .field
                        .clone()
                        .unwrap_or_else(|| s.fields[0].0.to_string());
                    if !s.fields.iter().any(|(id, _)| *id == f) {
                        return Err(format!("{}: thành phần không hợp lệ: {f}", s.label));
                    }
                    o.field = Some(f);
                }
                Ok(o)
            }
        }
    }

    /// Stable id, e.g. `sma(20)`, `macd(12,26,9).hist`, `bb(20,2).pctb`.
    pub fn key(&self) -> String {
        let Some(id) = &self.ind else {
            return format!("{}", self.value.unwrap_or(0.0));
        };
        let mut args: Vec<String> = vec![];
        for v in [self.n, self.fast, self.slow, self.signal]
            .into_iter()
            .flatten()
        {
            args.push(v.to_string());
        }
        if let Some(k) = self.k {
            args.push(format!("{k}"));
        }
        let mut s = if args.is_empty() {
            id.clone()
        } else {
            format!("{id}({})", args.join(","))
        };
        if let Some(f) = &self.field {
            s.push('.');
            s.push_str(f);
        }
        s
    }

    /// Short Vietnamese label, e.g. "MA20", "RSI14", "MACD (12,26,9) histogram".
    pub fn label(&self) -> String {
        let Some(id) = &self.ind else {
            return format!("{}", self.value.unwrap_or(0.0));
        };
        let n = self.n.map(|n| n.to_string()).unwrap_or_default();
        match id.as_str() {
            "close" => "Giá".into(),
            "volume" => "KL".into(),
            "sma" => format!("MA{n}"),
            "ema" => format!("EMA{n}"),
            "rsi" => format!("RSI{n}"),
            "stoch" => format!("%K{n}"),
            "roc" => format!("%Δ{n}p"),
            "vol_ratio" => format!("KL/TB{n}"),
            "avg_value" => format!("GTGD TB{n} (tỷ)"),
            "atr" => format!("ATR{n} %"),
            "from_high" => format!("% cách đỉnh {n}p"),
            "from_low" => format!("% trên đáy {n}p"),
            "macd" => {
                let f = match self.field.as_deref() {
                    Some("signal") => "signal",
                    Some("hist") => "hist",
                    _ => "line",
                };
                format!(
                    "MACD{} {f}",
                    if (self.fast, self.slow, self.signal) == (Some(12), Some(26), Some(9)) {
                        String::new()
                    } else {
                        format!(
                            "({},{},{})",
                            self.fast.unwrap_or(0),
                            self.slow.unwrap_or(0),
                            self.signal.unwrap_or(0)
                        )
                    }
                )
            }
            "bb" => {
                let f = match self.field.as_deref() {
                    Some("upper") => "trên",
                    Some("lower") => "dưới",
                    _ => "%B",
                };
                format!("BB{n} {f}")
            }
            other => other.to_string(),
        }
    }
}

pub fn normalize(req: &ScanRequest, universe: &Universe) -> Result<ScanRequest, ScanError> {
    if req.conditions.is_empty() {
        return Err(ScanError::Invalid("cần ít nhất một điều kiện".into()));
    }
    if req.conditions.len() > MAX_CONDITIONS {
        return Err(ScanError::Invalid(format!(
            "tối đa {MAX_CONDITIONS} điều kiện"
        )));
    }
    let logic = match req.logic.as_str() {
        "all" | "" => "all",
        "any" => "any",
        _ => return Err(ScanError::Invalid("logic phải là all hoặc any".into())),
    };
    let mut conditions = vec![];
    for c in &req.conditions {
        if !OPS.contains(&c.op.as_str()) {
            return Err(ScanError::Invalid(format!(
                "phép so sánh không hỗ trợ: {}",
                c.op
            )));
        }
        let left = c.left.normalize().map_err(ScanError::Invalid)?;
        let right = c.right.normalize().map_err(ScanError::Invalid)?;
        if left.ind.is_none() {
            return Err(ScanError::Invalid("vế trái phải là một chỉ báo".into()));
        }
        conditions.push(Condition {
            left,
            op: c.op.clone(),
            right,
        });
    }
    let symbols = match &req.symbols {
        None => None,
        Some(list) => {
            if list.len() > 500 {
                return Err(ScanError::Invalid("quá nhiều mã".into()));
            }
            let mut v: Vec<String> = list
                .iter()
                .map(|s| s.trim().to_ascii_uppercase())
                .filter(|s| universe.contains(s))
                .collect();
            v.sort();
            v.dedup();
            Some(v)
        }
    };
    Ok(ScanRequest {
        conditions,
        logic: logic.into(),
        symbols,
    })
}

// ---------------------------------------------------------------- evaluation

/// Indicator series of one symbol, computed once per operand key.
struct Frame<'a> {
    bars: &'a [DailyBar],
    close: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    volume: Vec<f64>,
    memo: HashMap<String, ind::Series>,
}

impl<'a> Frame<'a> {
    fn new(bars: &'a [DailyBar]) -> Self {
        Self {
            bars,
            close: bars.iter().map(|b| b.close).collect(),
            high: bars.iter().map(|b| b.high).collect(),
            low: bars.iter().map(|b| b.low).collect(),
            volume: bars.iter().map(|b| b.volume).collect(),
            memo: HashMap::new(),
        }
    }

    fn series(&mut self, o: &Operand) -> ind::Series {
        let key = o.key();
        if let Some(s) = self.memo.get(&key) {
            return s.clone();
        }
        let len = self.bars.len();
        let n = o.n.unwrap_or(14) as usize;
        let s: ind::Series = match o.ind.as_deref() {
            None => vec![o.value; len],
            Some("close") => self.close.iter().map(|x| Some(*x)).collect(),
            Some("volume") => self.volume.iter().map(|x| Some(*x)).collect(),
            Some("sma") => ind::sma(&self.close, n),
            Some("ema") => ind::ema(&self.close, n),
            Some("rsi") => ind::rsi(&self.close, n),
            Some("stoch") => ind::stoch_k(&self.high, &self.low, &self.close, n),
            Some("roc") => ind::roc(&self.close, n),
            Some("vol_ratio") => ind::volume_ratio(&self.volume, n),
            Some("avg_value") => {
                let v: Vec<f64> = self
                    .close
                    .iter()
                    .zip(&self.volume)
                    .map(|(c, v)| c * v / 1e9)
                    .collect();
                ind::sma(&v, n)
            }
            Some("atr") => ind::atr_pct(&self.high, &self.low, &self.close, n),
            Some("from_high") => ind::from_high(&self.high, &self.close, n),
            Some("from_low") => ind::from_low(&self.low, &self.close, n),
            Some("macd") => {
                let m = ind::macd(
                    &self.close,
                    o.fast.unwrap_or(12) as usize,
                    o.slow.unwrap_or(26) as usize,
                    o.signal.unwrap_or(9) as usize,
                );
                match o.field.as_deref() {
                    Some("signal") => m.signal,
                    Some("hist") => m.hist,
                    _ => m.line,
                }
            }
            Some("bb") => {
                let b = ind::bollinger(&self.close, n, o.k.unwrap_or(2.0));
                match o.field.as_deref() {
                    Some("upper") => b.upper,
                    Some("lower") => b.lower,
                    _ => b.pct_b,
                }
            }
            Some(_) => vec![None; len],
        };
        self.memo.insert(key, s.clone());
        s
    }

    fn last(&mut self, o: &Operand, back: usize) -> Option<f64> {
        let s = self.series(o);
        let i = s.len().checked_sub(1 + back)?;
        s[i]
    }
}

fn check(f: &mut Frame, c: &Condition) -> bool {
    let (l, r) = (f.last(&c.left, 0), f.last(&c.right, 0));
    let (Some(l), Some(r)) = (l, r) else {
        return false; // not enough history
    };
    match c.op.as_str() {
        "gt" => l > r,
        "gte" => l >= r,
        "lt" => l < r,
        "lte" => l <= r,
        "cross_above" | "cross_below" => {
            let (Some(pl), Some(pr)) = (f.last(&c.left, 1), f.last(&c.right, 1)) else {
                return false;
            };
            if c.op == "cross_above" {
                pl <= pr && l > r
            } else {
                pl >= pr && l < r
            }
        }
        _ => false,
    }
}

fn round(x: f64, d: i32) -> f64 {
    let m = 10f64.powi(d);
    (x * m).round() / m
}

/// Evaluate a (normalized) request on one symbol's bars.
pub fn evaluate(req: &ScanRequest, bars: &[DailyBar]) -> Option<Value> {
    let last = bars.last()?;
    let mut f = Frame::new(bars);
    let checks: Vec<bool> = req.conditions.iter().map(|c| check(&mut f, c)).collect();
    let matched = if req.logic == "any" {
        checks.iter().any(|x| *x)
    } else {
        checks.iter().all(|x| *x)
    };
    let mut values = serde_json::Map::new();
    for c in &req.conditions {
        for o in [&c.left, &c.right] {
            if o.ind.is_some() {
                let v = f.last(o, 0).map(|v| round(v, 4));
                values.insert(o.key(), json!(v));
            }
        }
    }
    let prev = bars.len().checked_sub(2).map(|i| bars[i].close);
    let change = prev
        .filter(|p| *p > 0.0)
        .map(|p| round((last.close / p - 1.0) * 100.0, 2));
    let spark: Vec<f64> = bars[bars.len().saturating_sub(40)..]
        .iter()
        .map(|b| b.close)
        .collect();
    Some(json!({
        "matched": matched,
        "checks": checks,
        "date": last.date.to_string(),
        "close": last.close,
        "change_pct": change,
        "volume": last.volume,
        "bars": bars.len(),
        "values": values,
        "spark": spark,
    }))
}

// ---------------------------------------------------------------- service

const RESULT_TTL: Duration = Duration::from_secs(60);
const MAX_CACHED_RESULTS: usize = 200;

pub struct Screener {
    source: Arc<dyn BarsSource>,
    cache: BarCache,
    lookback_days: i64,
    concurrency: usize,
    ttl_open: Duration,
    ttl_closed: Duration,
    universe: Arc<parking_lot::RwLock<Universe>>,
    calendar: Arc<SessionCalendar>,
    clock: Arc<dyn Clock>,
    runs: tokio::sync::Semaphore,
    results: Mutex<HashMap<String, (Instant, Arc<Value>)>>,
    /// one refresh of the bar cache at a time (protects the vendor quota)
    refresh: tokio::sync::Mutex<()>,
}

impl Screener {
    pub fn new(
        s: &Settings,
        universe: Arc<parking_lot::RwLock<Universe>>,
        calendar: Arc<SessionCalendar>,
        rules: Arc<MarketRules>,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        let cfg = &s.screener;
        let dnse = || DnseBars::new(cfg, &s.provider.dnse, calendar.tz);
        let synthetic = || -> Arc<dyn BarsSource> {
            Arc::new(SyntheticBars {
                seed: s.provider.synthetic.seed,
                rules: rules.clone(),
            })
        };
        let source: Arc<dyn BarsSource> = match cfg.source.as_str() {
            "synthetic" => synthetic(),
            "dnse" => match dnse() {
                Some(d) => Arc::new(d),
                None => {
                    tracing::warn!(
                        "screener.source=dnse but no DNSE API key/secret: using synthetic bars"
                    );
                    synthetic()
                }
            },
            // auto: real bars when the live feed is DNSE and its key is configured
            _ => match (s.provider.kind == "dnse").then(dnse).flatten() {
                Some(d) => Arc::new(d),
                None => synthetic(),
            },
        };
        tracing::info!("screener: daily bars from {}", source.name());
        Arc::new(Self {
            source,
            cache: BarCache::default(),
            lookback_days: cfg.lookback_days.clamp(60, 1500),
            concurrency: cfg.concurrency.clamp(1, 16),
            ttl_open: Duration::from_secs_f64(cfg.ttl_open_sec.max(30.0)),
            ttl_closed: Duration::from_secs_f64(cfg.ttl_closed_sec.max(60.0)),
            universe,
            calendar,
            clock,
            runs: tokio::sync::Semaphore::new(2),
            results: Mutex::new(HashMap::new()),
            refresh: tokio::sync::Mutex::new(()),
        })
    }

    pub fn source_name(&self) -> &'static str {
        self.source.name()
    }

    fn ttl(&self) -> Duration {
        let now = self.clock.now();
        match self.calendar.session_at(now) {
            Session::Closed | Session::PreOpen => self.ttl_closed,
            _ => self.ttl_open,
        }
    }

    pub fn meta(&self) -> Value {
        json!({
            "source": self.source.name(),
            "indicators": catalog(),
            "presets": presets(),
            "ops": [
                {"id": "gt", "label": ">"}, {"id": "gte", "label": "≥"},
                {"id": "lt", "label": "<"}, {"id": "lte", "label": "≤"},
                {"id": "cross_above", "label": "cắt lên"}, {"id": "cross_below", "label": "cắt xuống"},
            ],
            "max_conditions": MAX_CONDITIONS,
            "cache_age_sec": self.cache.oldest_age().map(|d| d.as_secs()),
        })
    }

    /// Bars for every symbol, from the cache or the source (missing ones in parallel).
    async fn load(&self, symbols: &[String]) -> (HashMap<String, Arc<Vec<DailyBar>>>, Vec<Value>) {
        let ttl = self.ttl();
        let mut have = HashMap::new();
        let mut missing = vec![];
        for s in symbols {
            match self.cache.get(s, ttl) {
                Some(b) => {
                    have.insert(s.clone(), b);
                }
                None => missing.push(s.clone()),
            }
        }
        let mut errors = vec![];
        if missing.is_empty() {
            return (have, errors);
        }
        let _one = self.refresh.lock().await;
        let to = self.calendar.trading_date(self.clock.now());
        let from = to - chrono::Duration::days(self.lookback_days);
        let fetched: Vec<(String, Result<Vec<DailyBar>>)> = futures_util::stream::iter(missing)
            .map(|sym| async move {
                // another run may have filled it while we waited for the lock
                if let Some(b) = self.cache.get(&sym, ttl) {
                    return (sym, Ok((*b).clone()));
                }
                let r = self.source.daily(&sym, from, to).await;
                (sym, r)
            })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;
        for (sym, r) in fetched {
            match r {
                Ok(bars) if !bars.is_empty() => {
                    let b = self.cache.put(&sym, bars);
                    have.insert(sym, b);
                }
                Ok(_) => errors.push(json!({"symbol": sym, "error": "không có dữ liệu"})),
                Err(e) => {
                    tracing::warn!("screener bars {sym}: {e:#}");
                    errors.push(json!({"symbol": sym, "error": format!("{e:#}").chars().take(160).collect::<String>()}));
                }
            }
        }
        (have, errors)
    }

    pub async fn run(&self, req: &ScanRequest) -> Result<Arc<Value>, ScanError> {
        let req = normalize(req, &self.universe.read())?;
        let key = serde_json::to_string(&req).unwrap_or_default();
        if let Some((t, v)) = self.results.lock().get(&key) {
            if t.elapsed() < RESULT_TTL {
                return Ok(v.clone());
            }
        }
        let _permit = tokio::time::timeout(Duration::from_secs(20), self.runs.acquire())
            .await
            .map_err(|_| ScanError::Busy)?
            .map_err(|_| ScanError::Busy)?;
        let started = Instant::now();
        let (symbols, sectors): (Vec<String>, HashMap<String, Option<String>>) = {
            let u = self.universe.read();
            let all = u.symbols();
            let syms = match &req.symbols {
                Some(list) => list.clone(),
                None => all,
            };
            let sectors = syms
                .iter()
                .map(|s| (s.clone(), u.get(s).and_then(|i| i.sector.clone())))
                .collect();
            (syms, sectors)
        };
        let (bars, errors) = self.load(&symbols).await;
        if bars.is_empty() && !symbols.is_empty() {
            return Err(ScanError::Source(
                errors
                    .first()
                    .and_then(|e| e["error"].as_str())
                    .unwrap_or("không lấy được dữ liệu")
                    .to_string(),
            ));
        }
        let mut rows = vec![];
        let mut as_of: Option<String> = None;
        for sym in &symbols {
            let Some(b) = bars.get(sym) else { continue };
            let Some(mut row) = evaluate(&req, b) else {
                continue;
            };
            let d = row["date"].as_str().map(String::from);
            if d > as_of {
                as_of = d;
            }
            if row["matched"] == true {
                let o = row.as_object_mut().expect("object");
                o.insert("symbol".into(), json!(sym));
                o.insert("sector".into(), json!(sectors.get(sym).cloned().flatten()));
                rows.push(row);
            }
        }
        let columns: Vec<Value> = {
            let mut seen = BTreeMap::new();
            for c in &req.conditions {
                for o in [&c.left, &c.right] {
                    if o.ind.is_some() && o.ind.as_deref() != Some("close") {
                        seen.entry(o.key()).or_insert_with(|| o.label());
                    }
                }
            }
            seen.into_iter()
                .map(|(k, l)| json!({"key": k, "label": l}))
                .collect()
        };
        let conditions: Vec<String> = req
            .conditions
            .iter()
            .map(|c| {
                let op = match c.op.as_str() {
                    "gt" => ">",
                    "gte" => "≥",
                    "lt" => "<",
                    "lte" => "≤",
                    "cross_above" => "cắt lên",
                    _ => "cắt xuống",
                };
                format!("{} {op} {}", c.left.label(), c.right.label())
            })
            .collect();
        let doc = Arc::new(json!({
            "source": self.source.name(),
            "as_of": as_of,
            "logic": req.logic,
            "conditions": conditions,
            "columns": columns,
            "scanned": bars.len(),
            "requested": symbols.len(),
            "matched": rows.len(),
            "errors": errors,
            "took_ms": started.elapsed().as_millis() as u64,
            "rows": rows,
        }));
        let mut cache = self.results.lock();
        if cache.len() >= MAX_CACHED_RESULTS {
            cache.retain(|_, (t, _)| t.elapsed() < RESULT_TTL);
            if cache.len() >= MAX_CACHED_RESULTS {
                cache.clear();
            }
        }
        cache.insert(key, (Instant::now(), doc.clone()));
        Ok(doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn bars(closes: &[f64]) -> Vec<DailyBar> {
        let d0 = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        closes
            .iter()
            .enumerate()
            .map(|(i, c)| DailyBar {
                date: d0 + chrono::Duration::days(i as i64),
                open: *c,
                high: c + 1.0,
                low: c - 1.0,
                close: *c,
                volume: 1000.0,
            })
            .collect()
    }

    fn req(conditions: Value) -> ScanRequest {
        let u = Universe::from_instruments("t", vec![]);
        normalize(
            &ScanRequest {
                conditions: serde_json::from_value(conditions).unwrap(),
                ..Default::default()
            },
            &u,
        )
        .unwrap()
    }

    #[test]
    fn cross_above_only_on_the_crossing_bar() {
        // flat at 10 for 25 bars, then a jump: close crosses above MA20 on the last bar
        let mut c = vec![10.0; 25];
        c.push(12.0);
        let r = req(
            json!([{"left": {"ind": "close"}, "op": "cross_above", "right": {"ind": "sma", "n": 20}}]),
        );
        assert_eq!(evaluate(&r, &bars(&c)).unwrap()["matched"], true);
        c.push(12.5); // still above, but no longer a cross
        assert_eq!(evaluate(&r, &bars(&c)).unwrap()["matched"], false);
    }

    #[test]
    fn rsi_threshold_and_any_logic() {
        let falling: Vec<f64> = (0..40).map(|i| 100.0 - i as f64).collect();
        let r = req(json!([{"left": {"ind": "rsi"}, "op": "lt", "right": {"value": 30}}]));
        let row = evaluate(&r, &bars(&falling)).unwrap();
        assert_eq!(row["matched"], true);
        assert!(row["values"]["rsi(14)"].as_f64().unwrap() < 30.0);
        let mut both = r.clone();
        both.conditions.push(
            serde_json::from_value(
                json!({"left": {"ind": "rsi"}, "op": "gt", "right": {"value": 70}}),
            )
            .unwrap(),
        );
        assert_eq!(evaluate(&both, &bars(&falling)).unwrap()["matched"], false);
        both.logic = "any".into();
        assert_eq!(evaluate(&both, &bars(&falling)).unwrap()["matched"], true);
    }

    #[test]
    fn not_enough_history_never_matches() {
        let r = req(json!([{"left": {"ind": "sma", "n": 200}, "op": "gt", "right": {"value": 0}}]));
        assert_eq!(evaluate(&r, &bars(&[10.0; 50])).unwrap()["matched"], false);
    }

    #[test]
    fn validation_rejects_bad_input() {
        let u = Universe::from_instruments("t", vec![]);
        let bad = |c: Value| {
            normalize(
                &ScanRequest {
                    conditions: serde_json::from_value(c).unwrap(),
                    ..Default::default()
                },
                &u,
            )
            .is_err()
        };
        assert!(bad(
            json!([{"left": {"ind": "nope"}, "op": "gt", "right": {"value": 1}}])
        ));
        assert!(bad(
            json!([{"left": {"ind": "sma", "n": 100000}, "op": "gt", "right": {"value": 1}}])
        ));
        assert!(bad(
            json!([{"left": {"ind": "rsi"}, "op": "drop table", "right": {"value": 1}}])
        ));
        assert!(bad(
            json!([{"left": {"value": 1}, "op": "gt", "right": {"value": 1}}])
        ));
        assert!(bad(
            json!([{"left": {"ind": "macd", "fast": 30, "slow": 20}, "op": "gt", "right": {"value": 0}}])
        ));
        assert!(bad(json!([])));
        // defaults filled, canonical keys
        let r = req(
            json!([{"left": {"ind": "macd", "field": "hist"}, "op": "gt", "right": {"value": 0}}]),
        );
        assert_eq!(r.conditions[0].left.key(), "macd(12,26,9).hist");
    }

    #[test]
    fn presets_are_valid() {
        let u = Universe::from_instruments("t", vec![]);
        for p in presets().as_array().unwrap() {
            let r = ScanRequest {
                conditions: serde_json::from_value(p["conditions"].clone()).unwrap(),
                ..Default::default()
            };
            assert!(normalize(&r, &u).is_ok(), "{}", p["id"]);
        }
    }
}
