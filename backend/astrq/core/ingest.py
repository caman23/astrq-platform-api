"""Ingest & normalize: validate, dedupe, detect gaps/late ticks, stamp, publish."""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from astrq.core.bus import Bus
from astrq.core.metrics import METRICS
from astrq.core.models import MarketEvent, Session, Tick
from astrq.core.sessions import SessionCalendar
from astrq.core.universe import Universe
from astrq.providers.base import MarketDataProvider

log = logging.getLogger(__name__)


def _dedupe_key(ev: MarketEvent) -> tuple[Any, ...]:
    if isinstance(ev, Tick):
        if ev.seq is not None:
            return ("tick", ev.provider, ev.symbol, "seq", ev.seq)
        if ev.raw_ref is not None:  # vendor-unique id (e.g. SSI cumulative volume)
            return ("tick", ev.provider, ev.symbol, "ref", ev.raw_ref)
        # last resort: identical trades in the same timestamp would collapse
        return ("tick", ev.provider, ev.symbol, ev.ts_exchange, ev.price, ev.volume)
    # timestamps may be second-resolution: include the payload, not just the time
    return (ev.event_type, ev.model_dump_json(exclude={"ts_received"}))


class Ingest:
    def __init__(
        self,
        bus: Bus[MarketEvent],
        calendar: SessionCalendar,
        universe: Universe,
        on_event: Callable[[MarketEvent], None] | None = None,
        advance_clock: Callable[[Any], None] | None = None,
        dedupe_capacity: int = 200_000,
    ) -> None:
        self.bus = bus
        self.calendar = calendar
        self.universe = universe
        self.on_event = on_event
        self.advance_clock = advance_clock
        self._seen: OrderedDict[tuple[Any, ...], None] = OrderedDict()
        self._cap = dedupe_capacity
        self._last_seq: dict[tuple[str, str], int] = {}
        self._last_ts: dict[str, Any] = {}
        self.stats = {"accepted": 0, "duplicates": 0, "gaps": 0, "late": 0, "outside_universe": 0}

    def process(self, ev: MarketEvent) -> MarketEvent | None:
        if ev.symbol not in self.universe:
            self.stats["outside_universe"] += 1
            return None
        key = _dedupe_key(ev)
        if key in self._seen:
            self.stats["duplicates"] += 1
            METRICS.inc("ingest_duplicates_total")
            return None
        self._seen[key] = None
        if len(self._seen) > self._cap:
            self._seen.popitem(last=False)

        updates: dict[str, Any] = {"ingest_mono": time.monotonic()}
        if isinstance(ev, Tick):
            if ev.seq is not None:
                sk = (ev.provider, ev.symbol)
                last = self._last_seq.get(sk)
                if last is not None and ev.seq > last + 1:
                    self.stats["gaps"] += ev.seq - last - 1
                    METRICS.inc("ingest_seq_gaps_total", ev.seq - last - 1)
                if last is None or ev.seq > last:
                    self._last_seq[sk] = ev.seq
            prev_ts = self._last_ts.get(ev.symbol)
            if prev_ts is not None and ev.ts_exchange < prev_ts:
                self.stats["late"] += 1
                METRICS.inc("ingest_late_ticks_total")
            else:
                self._last_ts[ev.symbol] = ev.ts_exchange
            if ev.session is Session.CLOSED:
                updates["session"] = self.calendar.session_at(ev.ts_exchange)
            METRICS.inc("ticks_total")
            METRICS.observe(
                "feed_latency_seconds", (ev.ts_received - ev.ts_exchange).total_seconds()
            )
        ev = ev.model_copy(update=updates)
        if self.advance_clock is not None:
            self.advance_clock(ev.ts_exchange)
        self.stats["accepted"] += 1
        self.bus.publish(ev)
        if self.on_event is not None:
            self.on_event(ev)
        return ev

    async def run(self, provider: MarketDataProvider) -> None:
        async for ev in provider.events():
            self.process(ev)
