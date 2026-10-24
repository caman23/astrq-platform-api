# ADR-0001: In-process plugin model for tools

**Context.** The platform must host many tools; adding one must not touch the core.
Tools must be isolated so one failing tool cannot stall others.

**Decision.** Tools are Python classes implementing the `Tool` protocol
(`astrq/core/tool.py`), discovered from config as `module:Class`. Each tool
declares subscriptions, a Pydantic config model (its JSON schema becomes the
manifest's `config_schema`) and its signal kinds. The core runs each tool in a
`ToolRunner` with its own bounded queue, a per-call timeout, a circuit breaker
and snapshot/restore. Tools receive data only through bus events and
`ToolContext` (no direct provider access). Optional `view()` / `daily_summary()`
hooks let a tool expose dashboard views and EOD reports without core changes.

**Why.** Simple to operate (one process), fast (no serialization on the hot
path), testable (the same runner is used offline), and still isolates failures.

**Consequences.** A CPU-bound tool can still block the event loop (timeouts only
cancel at `await` points). If that becomes real, move such a tool into a
subprocess runner behind the same contract. Multi-tenant deployment would add a
per-tenant config layer on top of the registry.
