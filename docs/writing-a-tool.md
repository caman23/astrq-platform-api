# Writing a tool

A tool is a class; no core change is needed. Minimal example (see
`backend/astrq/tools/tick_counter/` for the full sample):

```python
from typing import ClassVar
from pydantic import BaseModel, Field
from astrq.core.models import MarketEvent, Signal, Tick
from astrq.core.tool import BaseTool, Subscription, ToolContext

class VolumeSpikeConfig(BaseModel):
    k: float = Field(5.0, description="Spike if minute volume ≥ k × average")

class VolumeSpike(BaseTool):
    id: ClassVar[str] = "volume_spike"
    version: ClassVar[str] = "0.1.0"
    name: ClassVar[str] = "Đột biến khối lượng"
    config_model: ClassVar[type[BaseModel]] = VolumeSpikeConfig
    signal_kinds: ClassVar[list[str]] = ["volume_spike"]

    def subscriptions(self) -> list[Subscription]:
        return [Subscription(frozenset({"tick"}), include_negotiated=False)]

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        ...  # fast, no blocking I/O; read baselines via ctx.context.baseline(...)
        return []
```

Register it in `config/settings.yaml`:

```yaml
tools:
  volume_spike:
    enabled: true
    module: mypkg.volume_spike:VolumeSpike
    config: { k: 6 }
```

Rules

- Only consume bus events and `ToolContext`; never call a vendor directly.
- Use event timestamps or the `now` passed to `on_timer` — never `datetime.now()`.
- Keep `on_event` fast (default timeout 250 ms; 5 consecutive failures open the breaker).
- Make `signal_id` / `dedupe_key` deterministic (e.g. `uuid5` of the dedupe key).
- Put user-facing lines in `features["summary_vi"]` (Vietnamese) — Telegram and the
  dashboard render them generically.
- Implement `snapshot()/restore()` if the tool keeps intraday state.
- Optional: `view(signals, ctx, params)` for a dashboard view at
  `/api/tools/<id>/view`, and `daily_summary(ctx)` for the EOD report.
- Test with `astrq.core.offline.run_offline` (deterministic) and add golden scenarios.
