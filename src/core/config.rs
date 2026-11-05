//! Settings: YAML file + `.env` + process environment (prefix `ASTRQ_`, nested with `__`).
//!
//! Example: `ASTRQ_PROVIDER__SYNTHETIC__SPEED=60` overrides `provider.synthetic.speed`.
//! Priority: process env > `<root>/.env` > YAML > built-in defaults.
//! Secrets (tokens) must come from the environment, never from the YAML file.

use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveTime};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::core::models::{Session, Severity};

// ------------------------------------------------------------------ helper types

/// Time of day accepting "HH:MM" or "HH:MM:SS".
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HmTime(pub NaiveTime);

impl HmTime {
    pub fn hm(h: u32, m: u32) -> Self {
        Self(NaiveTime::from_hms_opt(h, m, 0).expect("valid time"))
    }
    pub fn hms(h: u32, m: u32, s: u32) -> Self {
        Self(NaiveTime::from_hms_opt(h, m, s).expect("valid time"))
    }
}

impl Serialize for HmTime {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.format("%H:%M:%S").to_string())
    }
}

impl<'de> Deserialize<'de> for HmTime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        NaiveTime::parse_from_str(&s, "%H:%M:%S")
            .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M"))
            .map(HmTime)
            .map_err(|_| de::Error::custom(format!("invalid time '{s}' (expected HH:MM)")))
    }
}

/// A secret string: never printed in Debug output, serialized redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(**********)")
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str("**********")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(Secret(String::deserialize(d)?))
    }
}

fn secret_present(s: &Option<Secret>) -> bool {
    s.as_ref().is_some_and(|v| !v.0.is_empty())
}

fn str_present(s: &Option<String>) -> bool {
    s.as_ref().is_some_and(|v| !v.is_empty())
}

// ------------------------------------------------------------------ sections

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWindow {
    pub session: Session,
    pub start: HmTime,
    pub end: HmTime,
}

fn default_schedule() -> Vec<SessionWindow> {
    // TODO(verify): HOSE session hours must be confirmed against the current regulation.
    let w = |session, (h1, m1), (h2, m2)| SessionWindow {
        session,
        start: HmTime::hm(h1, m1),
        end: HmTime::hm(h2, m2),
    };
    vec![
        w(Session::Ato, (9, 0), (9, 15)),
        w(Session::ContinuousAm, (9, 15), (11, 30)),
        w(Session::Lunch, (11, 30), (13, 0)),
        w(Session::ContinuousPm, (13, 0), (14, 30)),
        w(Session::Atc, (14, 30), (14, 45)),
        w(Session::PutThrough, (14, 45), (15, 0)),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionsConfig {
    pub timezone: String,
    pub enforce_trading_days: bool,
    pub schedule: Vec<SessionWindow>,
    pub holidays: Vec<NaiveDate>,
}

impl Default for SessionsConfig {
    fn default() -> Self {
        Self {
            timezone: "Asia/Ho_Chi_Minh".into(),
            enforce_trading_days: true,
            schedule: default_schedule(),
            holidays: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickSizeRule {
    pub exchange: String,
    #[serde(default)]
    pub below: Option<f64>,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MarketRulesConfig {
    pub tick_sizes: Vec<TickSizeRule>,
}

impl Default for MarketRulesConfig {
    fn default() -> Self {
        // TODO(verify): HOSE tick-size ladder; confirm against current regulation.
        let r = |below, size| TickSizeRule {
            exchange: "HOSE".into(),
            below,
            size,
        };
        Self {
            tick_sizes: vec![
                r(Some(10_000.0), 10.0),
                r(Some(50_000.0), 50.0),
                r(None, 100.0),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SyntheticConfig {
    pub seed: u64,
    /// Simulated seconds per wall second. 0 = as fast as possible (offline runs).
    pub speed: f64,
    pub trading_date: Option<NaiveDate>,
    pub start_time: HmTime,
    pub emit_side_flag: bool,
    pub emit_quotes: bool,
    pub parent_orders_per_hour: f64,
    pub block_trades_per_hour: f64,
    pub put_through_per_hour: f64,
}

impl Default for SyntheticConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            speed: 30.0,
            trading_date: None,
            start_time: HmTime::hms(8, 59, 30),
            emit_side_flag: true,
            emit_quotes: true,
            parent_orders_per_hour: 70.0,
            block_trades_per_hour: 25.0,
            put_through_per_hour: 4.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplayConfig {
    pub path: String,
    pub speed: f64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            path: "data/ticks/replay.jsonl".into(),
            speed: 1.0,
        }
    }
}

/// SSI FastConnect Data (official guide + official client source, see docs/provider-notes.md).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SsiConfig {
    pub api_url: String,
    pub stream_url: String,
    pub consumer_id: Option<String>,
    pub consumer_secret: Option<Secret>,
    /// "X-TRADE" = matched trades with the BU/SD side flag; "X" = trade + best bid/ask.
    pub channel: String,
    pub subscribe_all: bool,
    pub price_multiplier: f64,
    pub volume_multiplier: i64,
    pub session_map: BTreeMap<String, Session>,
    pub record_raw: bool,
    pub idle_reconnect_sec: f64,
}

impl Default for SsiConfig {
    fn default() -> Self {
        Self {
            api_url: "https://fc-data.ssi.com.vn/".into(),
            stream_url: "https://fc-datahub.ssi.com.vn/".into(),
            consumer_id: None,
            consumer_secret: None,
            channel: "X-TRADE".into(),
            subscribe_all: false,
            price_multiplier: 1.0,
            volume_multiplier: 1,
            session_map: BTreeMap::new(),
            record_raw: true,
            idle_reconnect_sec: 180.0,
        }
    }
}

/// DNSE LightSpeed API market data WebSocket (official developer docs).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DnseConfig {
    pub ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<Secret>,
    /// Channel names without the encoding suffix; earlier channels get the stream budget first.
    pub channels: Vec<String>,
    pub putthrough_boards: Vec<String>,
    /// Verified live: streams are counted per channel x symbol (normalUser tier = 100-200).
    pub max_streams: usize,
    /// "auto" | "vnd" | "thousand". TODO(verify) on live data.
    pub price_unit: String,
    pub volume_multiplier: i64,
    pub session_map: BTreeMap<String, Session>,
    pub record_raw: bool,
    pub idle_reconnect_sec: f64,
}

impl Default for DnseConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://ws-openapi.dnse.com.vn/v1/stream".into(),
            api_key: None,
            api_secret: None,
            channels: vec!["tick_extra.G1".into(), "expected_price.G1".into()],
            putthrough_boards: vec!["T1".into(), "T3".into()],
            max_streams: 200,
            price_unit: "auto".into(),
            volume_multiplier: 1,
            session_map: BTreeMap::new(),
            record_raw: true,
            idle_reconnect_sec: 240.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    /// synthetic | replay | dnse | ssi
    pub kind: String,
    pub synthetic: SyntheticConfig,
    pub replay: ReplayConfig,
    pub ssi: SsiConfig,
    pub dnse: DnseConfig,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            kind: "synthetic".into(),
            synthetic: SyntheticConfig::default(),
            replay: ReplayConfig::default(),
            ssi: SsiConfig::default(),
            dnse: DnseConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UniverseConfig {
    pub file: String,
    pub reload_interval_sec: f64,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            file: "config/universe_vn100.yaml".into(),
            reload_interval_sec: 300.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BusConfig {
    pub queue_size: usize,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self { queue_size: 20_000 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ToolEntry {
    pub enabled: bool,
    /// Kept for compatibility with the Python config (`pkg.mod:Class`); tools are
    /// resolved by id in Rust.
    pub module: Option<String>,
    pub config: Value,
    pub timeout_ms: u64,
    pub max_consecutive_failures: u32,
    pub breaker_cooldown_sec: f64,
    /// In market-clock seconds.
    pub timer_interval_sec: f64,
}

impl Default for ToolEntry {
    fn default() -> Self {
        Self {
            enabled: true,
            module: None,
            config: Value::Object(Map::new()),
            timeout_ms: 250,
            max_consecutive_failures: 5,
            breaker_cooldown_sec: 30.0,
            timer_interval_sec: 5.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelegramConfig {
    pub enabled: bool,
    pub bot_token: Option<Secret>,
    pub chat_id: Option<String>,
    pub min_severity: Severity,
    pub max_per_minute: usize,
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token: None,
            chat_id: None,
            min_severity: Severity::Alert,
            max_per_minute: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertsConfig {
    pub cooldown_sec: f64,
    pub dashboard_min_severity: Severity,
    pub telegram: TelegramConfig,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            cooldown_sec: 120.0,
            dashboard_min_severity: Severity::Info,
            telegram: TelegramConfig::default(),
        }
    }
}

/// Optional S3-compatible object storage (Cloudflare R2) for disk-less hosts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RemoteStorageConfig {
    /// none | r2
    pub kind: String,
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<Secret>,
    pub region: String,
    pub prefix: String,
    pub signals_upload_interval_sec: f64,
    pub keep_summary_days: usize,
}

impl Default for RemoteStorageConfig {
    fn default() -> Self {
        Self {
            kind: "none".into(),
            endpoint: None,
            bucket: None,
            access_key_id: None,
            secret_access_key: None,
            region: "auto".into(),
            prefix: "astrq/".into(),
            signals_upload_interval_sec: 900.0,
            keep_summary_days: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    pub record_ticks: bool,
    pub record_signals: bool,
    pub snapshot_interval_sec: f64,
    pub retention_days: u64,
    pub remote: RemoteStorageConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".into(),
            record_ticks: true,
            record_signals: true,
            snapshot_interval_sec: 30.0,
            retention_days: 14,
            remote: RemoteStorageConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextConfig {
    /// synthetic | recorded | none | vndirect
    pub kind: String,
    pub lookback_days: usize,
    pub min_days: usize,
    pub cache_ttl_sec: f64,
    pub warmup_time: HmTime,
    pub eod_summary_time: HmTime,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            kind: "synthetic".into(),
            lookback_days: 10,
            min_days: 3,
            cache_ttl_sec: 6.0 * 3600.0,
            warmup_time: HmTime::hm(8, 30),
            eod_summary_time: HmTime::hm(15, 5),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub cors_origins: Vec<String>,
    pub frontend_dist: String,
    /// Sites allowed to embed the dashboard in an <iframe> (CSP frame-ancestors).
    pub frame_ancestors: Vec<String>,
    /// When set, every request except /health must carry `X-Astrq-Origin: <secret>`.
    pub origin_secret: Option<Secret>,
    /// Allow enabling/disabling tools from the web UI/API (off on public deployments).
    pub allow_tool_control: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 8000,
            cors_origins: vec!["http://localhost:5173".into()],
            frontend_dist: "frontend/dist".into(),
            frame_ancestors: vec![
                "'self'".into(),
                "https://astrq.io.vn".into(),
                "https://*.astrq.io.vn".into(),
            ],
            origin_secret: None,
            allow_tool_control: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub root_dir: PathBuf,
    pub log_level: String,
    pub provider: ProviderConfig,
    pub universe: UniverseConfig,
    pub sessions: SessionsConfig,
    pub market_rules: MarketRulesConfig,
    pub bus: BusConfig,
    pub tools: BTreeMap<String, ToolEntry>,
    pub alerts: AlertsConfig,
    pub storage: StorageConfig,
    pub context: ContextConfig,
    pub api: ApiConfig,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("."),
            log_level: "INFO".into(),
            provider: ProviderConfig::default(),
            universe: UniverseConfig::default(),
            sessions: SessionsConfig::default(),
            market_rules: MarketRulesConfig::default(),
            bus: BusConfig::default(),
            tools: BTreeMap::new(),
            alerts: AlertsConfig::default(),
            storage: StorageConfig::default(),
            context: ContextConfig::default(),
            api: ApiConfig::default(),
        }
    }
}

impl Settings {
    pub fn path(&self, rel: &str) -> PathBuf {
        let p = Path::new(rel);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            self.root_dir.join(p)
        }
    }

    /// Fail fast at startup with the same messages as the original service.
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        let tg = &self.alerts.telegram;
        if tg.enabled && (!secret_present(&tg.bot_token) || !str_present(&tg.chat_id)) {
            errors.push(
                "alerts.telegram.enabled requires ASTRQ_ALERTS__TELEGRAM__BOT_TOKEN and \
                 ASTRQ_ALERTS__TELEGRAM__CHAT_ID"
                    .to_string(),
            );
        }
        let r = &self.storage.remote;
        if r.kind == "r2"
            && !(str_present(&r.endpoint)
                && str_present(&r.bucket)
                && str_present(&r.access_key_id)
                && secret_present(&r.secret_access_key))
        {
            errors.push(
                "storage.remote.kind=r2 requires ASTRQ_STORAGE__REMOTE__ENDPOINT, __BUCKET, \
                 __ACCESS_KEY_ID and __SECRET_ACCESS_KEY"
                    .to_string(),
            );
        }
        match self.provider.kind.as_str() {
            "synthetic" | "replay" => {}
            "dnse" => {
                let d = &self.provider.dnse;
                if !str_present(&d.api_key) || !secret_present(&d.api_secret) {
                    errors.push(
                        "provider.kind=dnse requires ASTRQ_PROVIDER__DNSE__API_KEY and \
                         ASTRQ_PROVIDER__DNSE__API_SECRET (EntradeX → Thông tin tài khoản → \
                         LightSpeed API)"
                            .to_string(),
                    );
                }
            }
            "ssi" => {
                let s = &self.provider.ssi;
                if !str_present(&s.consumer_id) || !secret_present(&s.consumer_secret) {
                    errors.push(
                        "provider.kind=ssi requires ASTRQ_PROVIDER__SSI__CONSUMER_ID and \
                         ASTRQ_PROVIDER__SSI__CONSUMER_SECRET (create them on SSI iBoard)"
                            .to_string(),
                    );
                }
            }
            other => errors.push(format!("unknown provider.kind '{other}'")),
        }
        if matches!(self.provider.kind.as_str(), "ssi" | "dnse") && self.context.kind == "synthetic"
        {
            errors.push(
                "context.kind=synthetic only matches the synthetic market; with a real feed set \
                 ASTRQ_CONTEXT__KIND=recorded (or none)"
                    .to_string(),
            );
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!(
                "Cấu hình không hợp lệ:\n{}",
                errors
                    .iter()
                    .map(|e| format!("  - {e}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        }
    }
}

// ------------------------------------------------------------------ loading

fn merge(base: &mut Value, over: Value) {
    match (base, over) {
        (Value::Object(b), Value::Object(o)) => {
            for (k, v) in o {
                match b.get_mut(&k) {
                    Some(existing) if existing.is_object() && v.is_object() => merge(existing, v),
                    _ => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (b, o) => *b = o,
    }
}

/// Convert an env string using the type of the value already at that path.
fn typed(existing: Option<&Value>, raw: &str) -> Value {
    let t = raw.trim();
    match existing {
        Some(Value::Number(_)) => {
            if let Ok(i) = t.parse::<i64>() {
                return Value::from(i);
            }
            if let Ok(f) = t.parse::<f64>() {
                return Value::from(f);
            }
            Value::String(raw.into())
        }
        Some(Value::Bool(_)) => match t.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Value::Bool(true),
            "false" | "0" | "no" | "off" => Value::Bool(false),
            _ => Value::String(raw.into()),
        },
        Some(Value::Array(_)) => serde_json::from_str(t).unwrap_or_else(|_| loose_list(t)),
        Some(Value::Object(_)) => {
            serde_json::from_str(t).unwrap_or_else(|_| Value::String(raw.into()))
        }
        _ if t.starts_with('[') || t.starts_with('{') => {
            serde_json::from_str(t).unwrap_or_else(|_| Value::String(raw.into()))
        }
        _ => Value::String(raw.into()),
    }
}

/// `.env` parsers strip double quotes from unquoted values (`["'self'","https://x"]`
/// arrives as `['self',https://x]`), so accept a plain comma-separated list too.
fn loose_list(t: &str) -> Value {
    let inner = t.trim().trim_start_matches('[').trim_end_matches(']');
    Value::Array(
        inner
            .split(',')
            .map(|x| x.trim().trim_matches('"').to_string())
            .filter(|x| !x.is_empty())
            .map(Value::String)
            .collect(),
    )
}

fn set_path(root: &mut Value, path: &[String], raw: &str) {
    let mut cur = root;
    for (i, key) in path.iter().enumerate() {
        let last = i + 1 == path.len();
        if !cur.is_object() {
            *cur = Value::Object(Map::new());
        }
        let obj = cur.as_object_mut().expect("object");
        if last {
            let v = typed(obj.get(key), raw);
            obj.insert(key.clone(), v);
            return;
        }
        cur = obj
            .entry(key.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }
}

/// Apply `ASTRQ_*` variables (already merged: `.env` first, process env last).
pub fn apply_env(base: &mut Value, vars: &[(String, String)]) {
    for (k, v) in vars {
        let Some(rest) = k.strip_prefix("ASTRQ_") else {
            continue;
        };
        if rest == "CONFIG" {
            continue;
        }
        let path: Vec<String> = rest
            .to_ascii_lowercase()
            .split("__")
            .map(String::from)
            .collect();
        set_path(base, &path, v);
    }
}

pub fn settings_from_value(mut base: Value, root: &Path) -> Result<Settings> {
    base["root_dir"] = Value::String(root.to_string_lossy().into_owned());
    let s: Settings = serde_json::from_value(base).context("cấu hình không hợp lệ")?;
    s.validate()?;
    Ok(s)
}

/// Load settings. Priority: process env > `<root>/.env` > YAML file > defaults.
/// The YAML defaults to `./config/settings.yaml` (or `$ASTRQ_CONFIG`); `<root>` is the
/// directory containing `config/`.
pub fn load_settings(config_path: Option<&Path>, use_dotenv: bool) -> Result<Settings> {
    let raw = match config_path {
        Some(p) => p.to_path_buf(),
        None => std::env::var("ASTRQ_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| "config/settings.yaml".into()),
    };
    let path = std::path::absolute(&raw).unwrap_or(raw);
    let parent = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let root = if parent.file_name().is_some_and(|n| n == "config") {
        parent.parent().map(Path::to_path_buf).unwrap_or(parent)
    } else {
        parent
    };

    let mut base = serde_json::to_value(Settings::default())?;
    if path.exists() {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let yaml: Value =
            serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        if !yaml.is_null() {
            merge(&mut base, yaml);
        }
    }

    let mut vars: Vec<(String, String)> = Vec::new();
    let env_file = root.join(".env");
    if use_dotenv && env_file.exists() {
        for item in dotenvy::from_path_iter(&env_file).context("reading .env")? {
            let (k, v) = item.context("parsing .env")?;
            vars.push((k, v));
        }
    }
    vars.extend(std::env::vars().filter(|(k, _)| k.starts_with("ASTRQ_")));
    apply_env(&mut base, &vars);
    settings_from_value(base, &root)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(env: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("config")).unwrap();
        let cfg = dir.path().join("config/settings.yaml");
        std::fs::write(
            &cfg,
            "provider:\n  synthetic:\n    speed: 10\nlog_level: WARNING\n",
        )
        .unwrap();
        std::fs::write(dir.path().join(".env"), env).unwrap();
        (dir, cfg)
    }

    #[test]
    fn dotenv_overrides_yaml_with_typed_values() {
        let (_d, cfg) = project(
            "ASTRQ_PROVIDER__SYNTHETIC__SPEED=45\nASTRQ_LOG_LEVEL=DEBUG\n\
             ASTRQ_ALERTS__TELEGRAM__ENABLED=true\nASTRQ_ALERTS__TELEGRAM__BOT_TOKEN=123:abc\n\
             ASTRQ_ALERTS__TELEGRAM__CHAT_ID=-10042\n\
             ASTRQ_API__FRAME_ANCESTORS=[\"'self'\",\"http://localhost:5500\"]\n",
        );
        let s = load_settings(Some(&cfg), true).unwrap();
        assert_eq!(s.provider.synthetic.speed, 45.0);
        assert_eq!(s.log_level, "DEBUG");
        assert_eq!(s.alerts.telegram.chat_id.as_deref(), Some("-10042")); // stays a string
        assert_eq!(
            s.alerts.telegram.bot_token.as_ref().unwrap().expose(),
            "123:abc"
        );
        assert!(
            !format!("{s:?}").contains("123:abc"),
            "secret must not be printed"
        );
        assert_eq!(
            s.api.frame_ancestors,
            vec!["'self'", "http://localhost:5500"]
        );
        let s2 = load_settings(Some(&cfg), false).unwrap();
        assert_eq!(s2.provider.synthetic.speed, 10.0); // .env ignored
    }

    #[test]
    fn validation_messages() {
        let (_d, cfg) = project("ASTRQ_ALERTS__TELEGRAM__ENABLED=true\n");
        let err = load_settings(Some(&cfg), true).unwrap_err().to_string();
        assert!(err.contains("BOT_TOKEN"), "{err}");

        let mut base = serde_json::to_value(Settings::default()).unwrap();
        apply_env(&mut base, &[("ASTRQ_PROVIDER__KIND".into(), "dnse".into())]);
        let err = settings_from_value(base.clone(), Path::new("."))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("DNSE__API_KEY") && err.contains("recorded"),
            "{err}"
        );
        apply_env(
            &mut base,
            &[
                ("ASTRQ_PROVIDER__DNSE__API_KEY".into(), "k".into()),
                (
                    "ASTRQ_PROVIDER__DNSE__API_SECRET".into(),
                    "hidden-123".into(),
                ),
                ("ASTRQ_CONTEXT__KIND".into(), "recorded".into()),
            ],
        );
        let s = settings_from_value(base, Path::new(".")).unwrap();
        assert!(!format!("{s:?}").contains("hidden-123"));
    }
}
