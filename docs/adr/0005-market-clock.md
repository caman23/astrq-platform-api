# ADR-0005: Market clock abstraction

**Context.** The system must be testable when the market is closed and
deterministic on replay, while idle timeouts and session logic depend on time.

**Decision.** All detection timers and the scheduler use a `Clock`:
`WallClock` (live), `SimClock` (synthetic, speed multiplier), `EventClock`
(replay: time = latest exchange timestamp). The offline harness drives tool
timers from event time on fixed absolute boundaries, so a run split by a
snapshot/restore fires timers at the same instants as an uninterrupted run.

**Consequences.** Tools must never call `datetime.now()`; they use the event
timestamp or the `now` passed to `on_timer`. Internal latency is still measured
with the monotonic wall clock.
