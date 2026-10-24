"""In-memory store of today's signals (the dashboard's source of truth).

Durable history goes to the JSONL signal recorder; see docs/adr/0003-storage.md.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from datetime import date

from astrq.core.models import Severity, Signal
from astrq.core.sessions import SessionCalendar


class SignalStore:
    def __init__(self, calendar: SessionCalendar, max_signals: int = 50_000) -> None:
        self.calendar = calendar
        self.max_signals = max_signals
        self.trading_date: date | None = None
        self.signals: deque[Signal] = deque(maxlen=max_signals)

    def add(self, s: Signal) -> None:
        d = self.calendar.trading_date(s.ts)
        if self.trading_date is None or d > self.trading_date:
            self.trading_date = d
            self.signals.clear()
        elif d < self.trading_date:
            return  # stale signal from a previous day
        self.signals.append(s)

    def load(self, signals: Iterable[Signal]) -> int:
        n = 0
        for s in signals:
            self.add(s)
            n += 1
        return n

    def by_tool(self, tool_id: str) -> list[Signal]:
        return [s for s in self.signals if s.tool_id == tool_id]

    def query(
        self,
        tool_id: str | None = None,
        symbol: str | None = None,
        side: str | None = None,
        min_severity: Severity | None = None,
        limit: int = 200,
    ) -> list[Signal]:
        out: list[Signal] = []
        for s in reversed(self.signals):
            if tool_id and s.tool_id != tool_id:
                continue
            if symbol and s.symbol != symbol:
                continue
            if side and s.side.value != side:
                continue
            if min_severity and s.severity.rank < min_severity.rank:
                continue
            out.append(s)
            if len(out) >= limit:
                break
        return out
