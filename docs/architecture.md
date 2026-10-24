# Architecture

## Goal

A platform of independent, read-only market-analysis tools. Tool #1 is the
Large Order Radar for VN100. Design test for every decision: *adding tool #2 or
#3 must not require changing the core.*

## Data flow

```
 MarketDataProvider (synthetic | replay | dnse* | ssi*)        ContextProvider (synthetic | none | vndirect*)
          │ events()                                                   │ (scheduler warm-up, cached)
          ▼                                                            ▼
     Ingest: universe filter, dedupe, seq-gap & late detection,   ContextService (TTL cache, retry/backoff,
             session stamping, ingest timestamp, recorder         hot-path reads are memory-only)
          │                                                            │
          ▼                                                            │
  market bus (InMemoryBus, bounded per-subscriber queues) ─────────────┤
          │                  │                                        │
     ToolRunner(radar)   ToolRunner(tick_counter)   … (one per tool: own queue, timeout,
          │                  │                      circuit breaker, timer on market clock,
          ▼                  ▼                      snapshot/restore)
                  signal bus
                      │
      ┌───────────────┼──────────────────┬────────────────────┐
  SignalStore     JSONL recorder     AlertRouter            WebSocket listeners
  (today, API)   (data/signals)     dedupe · cooldown ·     (dashboard)
                                     coalescing · rate limit
                                     → dashboard, Telegram
```
`*` = skeleton / not implemented (see open questions).

## Core components (backend/astrq/core)

| Module | Responsibility |
|---|---|
| `models.py` | Pydantic v2 canonical model: `Instrument`, `Tick`, `Quote`, `ExpectedPrice`, `ForeignFlow`, `Signal`, `Contribution`, `Alert`. All timestamps tz-aware UTC. |
| `config.py` | YAML + env (`ASTRQ_…__…`) settings, validated at startup. |
| `sessions.py`, `market_rules.py` | Config-driven session calendar (VN time) and tick-size ladder. |
| `clock.py` | `WallClock` (live), `SimClock` (accelerated synthetic), `EventClock` (replay). Detection timers use the market clock. |
| `bus.py` | `InMemoryBus[T]`; publish never blocks, slow subscribers drop and are counted. |
| `ingest.py` | Validation, dedupe (by seq or content), seq gaps, late ticks, session fill, feed latency. |
| `tool.py` | Plugin contract (`Tool` protocol, `BaseTool`, `Subscription`, `ToolManifest`, `ToolContext`). |
| `registry.py` | Loads tools from config `module:Class`, validates their config model. |
| `runner.py` | Per-tool isolation: timeout per call, per-call-site consecutive failure count, circuit breaker with half-open recovery, periodic snapshot. |
| `context.py` | Baseline cache; `baseline_cold` when missing. |
| `alerts.py` | Router + dashboard and Telegram channels (sendMessage / getUpdates commands). |
| `platform.py` | Wiring, feed reconnect with backoff, scheduler (warm-up, EOD summary, retention, universe reload). |
| `offline.py` | Deterministic pipeline for tests/backtests (timers driven by event time). |

## Large Order Radar (tools/large_order_radar)

1. **Side**: provider flag → quote rule (fresh best bid/ask; inside-spread flagged, lower confidence) → tick rule (zero-tick inherits) → unknown. `side_source` and confidence travel with every trade. Auctions and put-through have no aggressor.
2. **Thresholds**: absolute (default 500M VND) and relative (k × median trade value, k per liquidity tier, with a floor). Triggers recorded per trade.
3. **Parent grouping**: same symbol + side + continuous session, gap ≤ G s, price within N ticks of the group's last price. Lifecycle OPEN (total ≥ clamp(25×median, 500M, 1.5B)) → UPDATE (+50%) → CLOSE (idle 90 s, lunch/auction/close, or a breaking trade).
4. **Per-symbol window** (15 min, O(1) updates): large buy/sell, net, large ratio, value/min, price change; plus day totals.
5. **Score** 0–100 = weighted normalized factors (value, relative size, velocity, concentration, directional price impact, side confidence). Every signal carries `explanation` with each factor's contribution. Severity: info < 40 ≤ notice < 65 ≤ alert.
6. **Sessions**: ATO/ATC prints → `block_auction` (no grouping); put-through → `block_putthrough`, kept out of matched-order metrics, severity capped at notice.
7. **Absorption** signal: config flag, off (not implemented in v1).

## Dashboard

React + Vite + TypeScript, native WebSocket. The radar places each symbol at a
fixed angle inside its sector's wedge; the distance from the center encodes the
day's peak radar score; blip size encodes large-trade value; shape + color
encode the dominant side; brightness follows the sweep beam and fades as the
symbol's last signal ages. Hover → order details with child trades; click → pinned
drawer with the full history and score breakdown. Side panel: live signal feed,
VN100 ranking table (net large / score), system health and tool toggles.

## Non-functional

- Latency: `feed_latency_seconds` (vendor ts → received) is separated from
  `tick_to_signal_seconds` (internal, monotonic clock). Measured on the synthetic
  feed at 60× (~2,000 ticks/s): internal p95 ≈ 3–4 ms.
- Determinism: same seed/input ⇒ identical event stream and signal ids (tests).
- Restart: tool snapshots every 30 s and on stop; restored only for the same
  trading day and tool version. Today's signals are reloaded from JSONL.

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Vendor feed lacks aggressor flag | Lower side accuracy | Quote/tick rule with confidence + `side_inferred` flag; measure accuracy once real data is recorded |
| Vendor limits on concurrent symbols | Cannot cover VN100 on one connection | Adapter must shard subscriptions; verify limits first |
| Data redistribution / retention terms | Legal | Record only locally; retention 14 days default; confirm terms before sharing anything |
| Thresholds tuned only on synthetic data | Noisy or silent radar | Record real sessions, run `make backtest`, tune config; no claims of predictive power |
| In-memory bus | Lost in-flight events on crash | Acceptable single-tenant; Redis Streams per ADR-0002 when needed |
