# ADR-0003: JSONL files first, TimescaleDB later

**Context.** Ticks and signals must be stored without blocking detection. The
spec suggests PostgreSQL + TimescaleDB.

**Decision.** v1 writes daily JSONL files (`data/ticks/YYYY-MM-DD.jsonl`,
`data/signals/…`) through an async, batched recorder with a bounded queue.
Tick files are the replay format, so every recorded real session becomes a test
fixture. Retention: files older than `storage.retention_days` are deleted daily.

**Why.** Zero infrastructure, trivially inspectable, directly replayable.

**Consequences.** No ad-hoc SQL history queries; the API serves only today's
signals from memory. Migrate to TimescaleDB (hypertable on ts, continuous
aggregates for the "keep aggregates after N days" policy) when history queries
or multi-day dashboards are needed. Provider terms on storing data must be
confirmed first (open question #1).
