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

## Core components (backend-rs/src/core)

Rust (tokio + axum) since ADR-0006; module names below keep their v1 roles.

| Module | Responsibility |
|---|---|
| `models.rs` | Serde canonical model: `Instrument`, `Tick`, `Quote`, `ExpectedPrice`, `ForeignFlow`, `Signal`, `Contribution`, `Alert`. All timestamps tz-aware UTC. |
| `config.rs` | YAML + env (`ASTRQ_…__…`) settings, validated at startup. |
| `sessions.rs`, `market_rules.rs` | Config-driven session calendar (VN time) and tick-size ladder. |
| `clock.rs` | `WallClock` (live), `SimClock` (accelerated synthetic), `EventClock` (replay). Detection timers use the market clock. |
| `bus.rs` | `InMemoryBus[T]`; publish never blocks, slow subscribers drop and are counted. |
| `ingest.rs` | Validation, dedupe (by seq or content), seq gaps, late ticks, session fill, feed latency. |
| `tool.rs` | Plugin contract (`Tool` protocol, `BaseTool`, `Subscription`, `ToolManifest`, `ToolContext`). |
| `tools/mod.rs` | `build_tool`: builds compiled-in tools by id, validates their config. |
| `runner.rs` | Per-tool isolation: panic capture, time budget per call, per-call-site consecutive failure count, circuit breaker with half-open recovery, periodic snapshot. |
| `context.rs` | Baseline cache; `baseline_cold` when missing. |
| `alerts.rs` | Router + dashboard and Telegram channels (sendMessage / getUpdates commands). |
| `platform.rs` | Wiring, feed reconnect with backoff, scheduler (warm-up, EOD summary, retention, universe reload). |
| `offline.rs` | Deterministic pipeline for tests/backtests (timers driven by event time). |

## Large Order Radar (tools/large_order_radar)

1. **Side**: provider flag → quote rule (fresh best bid/ask; inside-spread flagged, lower confidence) → tick rule (zero-tick inherits) → unknown. `side_source` and confidence travel with every trade. Auctions and put-through have no aggressor.
2. **Thresholds**: absolute (default 500M VND) and relative (k × median trade value, k per liquidity tier, with a floor). Triggers recorded per trade.
3. **Parent grouping**: same symbol + side + continuous session, gap ≤ G s, price within N ticks of the group's last price. Lifecycle OPEN (total ≥ clamp(25×median, 500M, 1.5B)) → UPDATE (+50%) → CLOSE (idle 90 s, lunch/auction/close, or a breaking trade).
4. **Per-symbol window** (15 min, O(1) updates): large buy/sell, net, large ratio, value/min, price change; plus day totals.
5. **Score** 0–100 = weighted normalized factors (value, relative size, velocity, concentration, directional price impact, side confidence). Every signal carries `explanation` with each factor's contribution. Severity: info < 40 ≤ notice < 65 ≤ alert.
6. **Sessions**: ATO/ATC prints → `block_auction` (no grouping); put-through → `block_putthrough`, kept out of matched-order metrics, severity capped at notice.
7. **Absorption** (`absorption`, v1.1): in the 15-minute window, large trades on one side
   (≥ `min_value_k` × the symbol's group-open threshold, ≥ 70 % of large value, ≥ 5
   trades, sustained ≥ 3 minutes) while those trades' own execution prices move ≤ 1 tick
   their way. Comparing same-side prints avoids the bid/ask bounce. Signal side = the
   absorber (large sells absorbed → buy). Episodes: OPEN, UPDATE on +50 %, 15-minute
   cooldown.
8. **Stealth accumulation / distribution** (`accumulation`, v1.1): medium trades
   (≥ 2 × median, below the large bar, not inside an opened block group) on one side over
   30 minutes: ≥ 10 trades, ≥ 75 % of medium value, total ≥ the group-open threshold, and
   regular sizes (volume CV ≤ 0.35 or one size ≥ 40 % of trades). This is what TWAP/iceberg
   execution looks like and what block detection cannot see.
9. **Follow-through** (v1.1): each order's reference price at its first signal; the price
   prevailing 5/15/30 minutes later is resolved exactly (last trade before the first trade
   past the due time, or on the timer) from per-symbol FIFOs, O(1) per tick. The view's
   `stats` table gives, per severity and signal family, the share of buy/sell orders whose
   price moved their way and the average directional move. `astrq backtest` prints the
   same table (`follow_through`) for recorded days. It is a description of the past, not
   a forecast.

Cost of 7–9 on the hot path: +0.2 µs per event (≈ 12 % offline: 565k → 505k events/s on
a full synthetic day), with no per-tick allocation; at live VN100 rates (~330 events/s)
that is ~0.007 % of a core. Old signals are unchanged (golden files identical).

## Dashboard

React + Vite + TypeScript, native WebSocket. The radar places each symbol at a
fixed angle inside its sector's wedge; the distance from the center encodes the
day's peak radar score; blip size encodes large-trade value; shape + color
encode the dominant side; brightness follows the sweep beam and fades as the
symbol's last signal ages. Hover → order details with child trades; click → pinned
drawer with the full history and score breakdown. Side panel: live signal feed,
VN100 ranking table (net large / score), sector heatmap, accuracy table, system health
and tool toggles. A timeline under the radar replays the day (`?at=HH:MM` views, rendered
once per moment and shared through the response cache).

### Serving many viewers

Every dashboard and iframe re-fetches the radar view after each radar signal. The API
renders each view (and `/api/signals` query) **once per signal-store version**: the JSON
is serialized and gzip-compressed once, shared by all viewers, revalidated with
`ETag`/`304 Not Modified`, and rebuilt at most every 2 s even without a new signal (day
totals can move without one). Concurrent misses wait for a single build. The store holds
`Arc<Signal>`, so readers never deep-copy signals, and WebSocket clients share one
message buffer per broadcast. Measured locally on a full synthetic day (20 concurrent
clients): view 16 → 120,000 req/s, p50 1.4 s → 0.14 ms, 138 KB → 44 KB per download;
radar tick-to-signal p95 unchanged (~0.7 ms) under that load.
`cargo run --release --example loadtest` and `--example viewbench` reproduce this.

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
