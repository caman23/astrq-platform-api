//! ASTRQ Stock Tools Platform — read-only realtime analysis tools for Vietnamese equities.
//!
//! Layout mirrors the original Python service: `core` (models, config, bus, ingest,
//! tool runner, alerts, platform), `providers` (market data + baselines), `tools`
//! (plugins), `api` (REST + WebSocket).

pub mod core;
pub mod tools;
