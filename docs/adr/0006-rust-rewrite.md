# ADR-0006: Rewrite the backend in Rust

Status: accepted (2026-11)

## Context

The v1 backend (Python 3.12, asyncio, FastAPI, Pydantic) worked but ran on Render's free
plan (0.1 CPU, 512 MB), where per-event overhead and memory use set the headroom for more
tools and more symbols.

## Decision

Rewrite the whole backend as one Rust crate, `backend-rs/` (tokio + axum). The React
dashboard, the HTTP/WS API contract, the config file and env variables (`ASTRQ_A__B`),
and the on-disk formats (JSONL signals, snapshots, R2 objects) all stay the same.

- The plugin model (ADR-0001) stays: `trait Tool` with sync `on_event`/`on_timer`. Tools
  are compiled in and registered in `tools/mod.rs::build_tool`, not loaded by
  `module:Class` path.
- Isolation stays: `ToolRunner` catches panics (`catch_unwind`), keeps a circuit breaker
  per call site, and enforces a time budget.
- Proof of behavioural parity: the 8 golden scenarios produced by the Python version
  (`backend-rs/tests/golden/*.json`) are reproduced **byte for byte**. Python-compatible
  rounding (`round` half-to-even) is implemented in `core/fmt.rs`.

## Measurements (same machine, live-like load of ~330 events/s)

| | Python | Rust |
|---|---|---|
| CPU time per event | 171 µs | 51 µs |
| CPU use | 6 % | 1.6 % |
| RSS | 74 MB | 17.6 MB |
| Offline replay | ~35 k ev/s | ~545 k ev/s |
| Docker image | ~250 MB | ~90 MB (single static-ish binary, 11 MB) |

## Consequences

- One binary (`astrq serve|simulate|backtest|dnse-check|ssi-check|ssi-universe|r2-check`).
- Adding a tool requires a rebuild. This is acceptable because all tools live in this repo.
- The Python implementation remains in git history (commit `c1a2b00` and earlier).
