# Open questions

1. ✅ **Tick vendor: SSI FastConnect Data** (adapter implemented, see provider-notes.md).
   Still open: package limits (symbols per subscription, message rate) and terms on
   storing/redistributing data — ask SSI; we only store locally meanwhile.
2. ✅ **Aggressor flag:** documented (`Side` BU/SD). To confirm on live data that it is
   populated for most trades (watch `side_unknown` in `stats.counters`).
3. **Units, time zone, session codes, put-through** in SSI messages — verify on the first
   live session (checklist in provider-notes.md).
4. ✅ **VN100 constituents:** `astrq ssi-universe` pulls the official list from SSI
   `IndexComponents`; re-run after each rebalancing. Sector labels remain manual.
5. **Session hours, tick sizes, holidays:** `config/settings.yaml` values marked
   `TODO(verify)`; load the official holiday calendar.
6. **Baselines:** `context.kind=recorded` computes them from our own recorded ticks;
   the first `min_days` (3) sessions run `baseline_cold` (absolute thresholds only).
7. **Deployment:** laptop or VPS? Required uptime? (Compose file not yet tested.)
8. **Alert channel:** Telegram implemented (opt-in). Others? Multi-user needed soon?
