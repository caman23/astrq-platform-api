"""Deterministic offline pipeline (tests, golden files, backtests).

Runs events through the real Ingest and ToolRunner code paths, driving tool
timers from event time, so identical input always yields identical signals.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from astrq.core.bus import InMemoryBus
from astrq.core.clock import EventClock
from astrq.core.config import Settings, ToolEntry
from astrq.core.context import ContextService
from astrq.core.ingest import Ingest
from astrq.core.market_rules import MarketRules
from astrq.core.models import MarketEvent, Signal
from astrq.core.registry import load_tool
from astrq.core.runner import ToolRunner
from astrq.core.sessions import SessionCalendar
from astrq.core.tool import Tool, ToolContext
from astrq.core.universe import Universe


@dataclass
class OfflineResult:
    signals: list[Signal]
    ingest_stats: dict[str, int]
    runners: dict[str, ToolRunner] = field(default_factory=dict)


async def run_offline(
    events: Iterable[MarketEvent],
    settings: Settings,
    universe: Universe,
    context: ContextService,
    tools: dict[str, tuple[Tool, ToolEntry]] | None = None,
    warm: bool = True,
    flush: bool = True,
) -> OfflineResult:
    calendar = SessionCalendar(settings.sessions)
    rules = MarketRules(settings.market_rules)
    clock = EventClock()
    market_bus: InMemoryBus[MarketEvent] = InMemoryBus("market")
    signal_bus: InMemoryBus[Signal] = InMemoryBus("signals")
    collector = signal_bus.subscribe("collector", maxsize=10_000_000)
    ingest = Ingest(market_bus, calendar, universe, advance_clock=clock.advance_to)

    if tools is None:
        tools = {
            tid: (load_tool(tid, entry), entry)
            for tid, entry in settings.tools.items()
            if entry.enabled
        }
    runners: dict[str, ToolRunner] = {}
    for tid, (tool, entry) in tools.items():
        ctx = ToolContext(
            tool_id=tid,
            config=getattr(tool, "config", entry),
            universe=universe,
            context=context,
            calendar=calendar,
            rules=rules,
            clock=clock,
            log=logging.getLogger(f"astrq.tool.{tid}"),
        )
        runners[tid] = ToolRunner(tool, ctx, entry, market_bus, signal_bus, snapshot_dir=None)
        await tool.on_start(ctx)

    subs = {tid: r.tool.subscriptions() for tid, r in runners.items()}
    next_timer: dict[str, datetime] = {}
    warmed: set[object] = set()
    last_ts: datetime | None = None

    for raw in events:
        ev = ingest.process(raw)
        if ev is None:
            continue
        d = calendar.trading_date(ev.ts_exchange)
        if warm and d not in warmed:
            warmed.add(d)
            await context.warm(universe.symbols, d)
        last_ts = ev.ts_exchange if last_ts is None else max(last_ts, ev.ts_exchange)
        for tid, r in runners.items():
            step = timedelta(seconds=r.entry.timer_interval_sec)
            if tid not in next_timer:
                # align the cadence to absolute multiples of the interval so a run
                # split in two (snapshot/restore) fires timers at the same instants
                secs = r.entry.timer_interval_sec
                first = (ev.ts_exchange.timestamp() // secs + 1) * secs
                next_timer[tid] = datetime.fromtimestamp(first, ev.ts_exchange.tzinfo)
            nt = next_timer[tid]
            while ev.ts_exchange >= nt:
                await r.process_timer(nt)
                nt += step
            next_timer[tid] = nt
            if any(s.matches(ev, universe) for s in subs[tid]):
                await r.process_event(ev)

    if flush and last_ts is not None:
        # Keep ticking timers until the session ends (as the live clock would), so
        # idle groups close at their real idle time; then a final tick at close.
        d = calendar.trading_date(last_ts)
        close_at = calendar.at_local(d, max(w.end for w in settings.sessions.schedule))
        end = max(close_at, last_ts + timedelta(seconds=1))
        for tid, r in runners.items():
            step = timedelta(seconds=r.entry.timer_interval_sec)
            nt = next_timer.get(tid, end)
            while nt < end:
                await r.process_timer(nt)
                nt += step
            await r.process_timer(end)

    out: list[Signal] = []
    while not collector.queue.empty():
        out.append(collector.get_nowait())
    return OfflineResult(signals=out, ingest_stats=dict(ingest.stats), runners=runners)
