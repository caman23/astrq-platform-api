"""Clocks. Detection and scheduling run on a *market clock* so the same code
works live (wall clock), in accelerated simulation, and in replay (event time).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class WallClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class SimClock:
    """Simulated time starting at `start`, advancing `speed`x faster than wall time.

    speed == 0 means "manual": time only moves via `advance_to` (offline runs).
    """

    def __init__(self, start: datetime, speed: float) -> None:
        self._start = start
        self._speed = speed
        self._mono0 = time.monotonic()
        self._manual = start

    @property
    def speed(self) -> float:
        return self._speed

    def now(self) -> datetime:
        if self._speed <= 0:
            return self._manual
        return self._start + timedelta(seconds=(time.monotonic() - self._mono0) * self._speed)

    def advance_to(self, ts: datetime) -> None:
        if ts > self._manual:
            self._manual = ts


class EventClock:
    """Time = latest exchange timestamp seen (replay). Falls back to a start value."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(1970, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        return self._now

    def advance_to(self, ts: datetime) -> None:
        if ts > self._now:
            self._now = ts
