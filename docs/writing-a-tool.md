# Writing a tool

A tool is a type that implements `core::tool::Tool`. No core change is needed. The full
sample is in `backend-rs/src/tools/tick_counter/mod.rs`. A minimal one looks like this:

```rust
use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::models::{MarketEvent, Signal};
use crate::core::tool::{Subscription, Tool, ToolContext, ToolManifest};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct VolumeSpikeConfig {
    /// Spike if minute volume ≥ k × average
    pub k: f64,
}
impl Default for VolumeSpikeConfig {
    fn default() -> Self { Self { k: 5.0 } }
}

pub struct VolumeSpike { cfg: VolumeSpikeConfig }

impl VolumeSpike {
    pub const ID: &'static str = "volume_spike";
    pub fn from_config(v: &Value) -> Result<Self> {
        Ok(Self { cfg: serde_json::from_value(v.clone())? })
    }
}

impl Tool for VolumeSpike {
    fn id(&self) -> &'static str { Self::ID }
    fn version(&self) -> &'static str { "0.1.0" }
    fn manifest(&self) -> ToolManifest { /* name, description, config schema, signal kinds */ }
    fn config_json(&self) -> Value { serde_json::to_value(&self.cfg).unwrap_or_default() }
    fn subscriptions(&self) -> Vec<Subscription> {
        let mut s = Subscription::new(&["tick"]);
        s.include_negotiated = false;
        vec![s]
    }
    fn on_event(&mut self, ev: &MarketEvent, ctx: &ToolContext) -> Result<Vec<Signal>> {
        // fast, no I/O; read baselines via ctx.context
        Ok(vec![])
    }
}
```

To register the tool:

1. Add `pub mod volume_spike;` to `backend-rs/src/tools/mod.rs`.
2. Add one arm in `build_tool`.
3. Enable it in `config/settings.yaml`:

```yaml
tools:
  volume_spike:
    enabled: true
    config: { k: 6 }
```

## Rules

- Only consume bus events and `ToolContext`. Never call a vendor directly.
- Use event timestamps or the `now` passed to `on_timer`. Never use `Utc::now()`.
- Keep `on_event` fast. The default budget is 250 ms, and 5 consecutive failures (errors
  or panics) open the breaker.
- Make `signal_id` and `dedupe_key` deterministic, for example `Uuid::new_v5` of the
  dedupe key.
- Put user-facing lines in `features["summary_vi"]` (Vietnamese). Telegram and the
  dashboard render them generically.
- Implement `snapshot()`/`restore()` if the tool keeps intraday state.
- Optional: `view(...)` adds a dashboard view at `/api/tools/<id>/view`, and
  `daily_summary(ctx)` adds a section to the EOD report.
- Test with `core::offline::run_offline`, which is deterministic. Add golden scenarios in
  `backend-rs/tests/scenarios.rs`.
