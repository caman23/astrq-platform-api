# ADR-0002: In-memory bus first, Redis Streams later

**Context.** The spec defaults to Redis Streams. The target is one user on a
laptop/small VPS; Docker was not available while building v1.

**Decision.** Ship `InMemoryBus[T]` behind a `Bus` protocol. Publishing never
blocks; each subscriber has a bounded queue and overflow is counted
(`bus_dropped_total`). Redis Streams is deferred until we need multiple
processes (e.g. subprocess tools, separate API/ingest) or durable replay of
in-flight events.

**Consequences.** Events in flight are lost on crash (signals already emitted are
on disk via the JSONL recorder; tool state via snapshots). Adding Redis = a new
`Bus` implementation + a compose service; tools do not change.
