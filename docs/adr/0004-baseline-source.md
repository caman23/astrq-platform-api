# ADR-0004: Baseline (context) source

**Context.** Relative thresholds need per-symbol "typical trade value". The
user has a VNDirect MCP connection; MCP is designed for Claude, not for backend
hot paths. Community projects use public VNDirect endpoints whose terms and
stability are unverified.

**Decision.** `ContextProvider` interface + `ContextService` cache (TTL, retry
with backoff, memory-only reads on the hot path, warm-up before the session).
v1 ships `SyntheticContextProvider` (consistent with the synthetic market) and
`NullContextProvider`. `context.kind: vndirect` is reserved but not implemented.

**Preferred real source.** Compute baselines from *our own recorded ticks*
(median / p95 trade value over the last N sessions) — no third-party dependency,
exactly the statistic the radar needs. Daily value / foreign flow / corporate
events can come from VNDirect once terms are confirmed.

**Consequences.** Until real baselines exist, live signals carry
`baseline_cold` and rely on absolute thresholds.
