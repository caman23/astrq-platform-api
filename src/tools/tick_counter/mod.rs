//! Trivial sample tool: proves the plugin boundary (counts ticks per symbol).

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::core::models::{MarketEvent, Severity, Side, Signal};
use crate::core::tool::{Subscription, Tool, ToolContext, ToolManifest};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct TickCounterConfig {
    /// Phát tín hiệu mỗi khi một mã đạt bội số này
    pub milestone: u64,
}

impl Default for TickCounterConfig {
    fn default() -> Self {
        Self { milestone: 2000 }
    }
}

pub struct TickCounter {
    cfg: TickCounterConfig,
    counts: BTreeMap<String, u64>,
}

impl TickCounter {
    pub const ID: &'static str = "tick_counter";

    pub fn new(mut cfg: TickCounterConfig) -> Self {
        cfg.milestone = cfg.milestone.max(1);
        Self {
            cfg,
            counts: BTreeMap::new(),
        }
    }

    pub fn from_config(v: &Value) -> Result<Self> {
        Ok(Self::new(serde_json::from_value(v.clone())?))
    }
}

impl Tool for TickCounter {
    fn id(&self) -> &'static str {
        Self::ID
    }
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    fn manifest(&self) -> ToolManifest {
        ToolManifest {
            id: Self::ID.into(),
            version: self.version().into(),
            name: "Đếm tick (tool mẫu)".into(),
            description: "Tool mẫu: đếm số tick theo mã, phát tín hiệu theo mốc.".into(),
            subscriptions: self
                .subscriptions()
                .iter()
                .map(Subscription::describe)
                .collect(),
            config_schema: serde_json::to_value(schemars::schema_for!(TickCounterConfig))
                .unwrap_or_default(),
            signal_kinds: vec!["tick_count_milestone".into()],
            has_view: false,
        }
    }
    fn config_json(&self) -> Value {
        serde_json::to_value(&self.cfg).unwrap_or_default()
    }
    fn subscriptions(&self) -> Vec<Subscription> {
        let mut s = Subscription::new(&["tick"]);
        s.include_negotiated = false;
        vec![s]
    }
    fn on_event(&mut self, ev: &MarketEvent, _ctx: &ToolContext) -> Result<Vec<Signal>> {
        let Some(t) = ev.as_tick() else {
            return Ok(vec![]);
        };
        let n = self.counts.entry(t.symbol.clone()).or_insert(0);
        *n += 1;
        if !(*n).is_multiple_of(self.cfg.milestone) {
            return Ok(vec![]);
        }
        let key = format!("{}:{}:{n}", Self::ID, t.symbol);
        let mut features = Map::new();
        features.insert("count".into(), json!(*n));
        features.insert(
            "summary_vi".into(),
            json!([format!("{} đạt {n} tick", t.symbol)]),
        );
        Ok(vec![Signal {
            signal_id: Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()).to_string(),
            tool_id: Self::ID.into(),
            tool_version: "1.0.0".into(),
            symbol: t.symbol.clone(),
            ts: t.ts_exchange,
            kind: "tick_count_milestone".into(),
            side: Side::Unknown,
            score: 0.0,
            severity: Severity::Info,
            state: "single".into(),
            features,
            explanation: vec![],
            quality_flags: vec![],
            dedupe_key: key,
            correlation_id: format!("{}-count", t.symbol),
        }])
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self.counts).ok()
    }
    fn restore(&mut self, data: &[u8]) -> Result<()> {
        self.counts = serde_json::from_slice(data)?;
        Ok(())
    }
}
