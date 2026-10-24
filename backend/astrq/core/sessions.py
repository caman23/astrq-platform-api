"""Trading calendar and session detection (config-driven, Vietnam time)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from astrq.core.config import SessionsConfig
from astrq.core.models import Session


class SessionCalendar:
    def __init__(self, cfg: SessionsConfig) -> None:
        self.cfg = cfg
        self.tz = ZoneInfo(cfg.timezone)
        self._holidays = set(cfg.holidays)
        self._windows = sorted(cfg.schedule, key=lambda w: w.start)

    def local(self, ts: datetime) -> datetime:
        return ts.astimezone(self.tz)

    def trading_date(self, ts: datetime) -> date:
        return self.local(ts).date()

    def is_trading_day(self, d: date) -> bool:
        if not self.cfg.enforce_trading_days:
            return True
        return d.weekday() < 5 and d not in self._holidays

    def session_at(self, ts: datetime) -> Session:
        loc = self.local(ts)
        if not self.is_trading_day(loc.date()):
            return Session.CLOSED
        t = loc.time()
        if self._windows and t < self._windows[0].start:
            return Session.PRE_OPEN
        for w in self._windows:
            if w.start <= t < w.end:
                return w.session
        return Session.CLOSED

    def at_local(self, d: date, t: time) -> datetime:
        """Combine a local date and time-of-day into an aware UTC datetime."""
        return datetime.combine(d, t, tzinfo=self.tz).astimezone(UTC)

    def next_trading_day(self, d: date) -> date:
        n = d + timedelta(days=1)
        while not self.is_trading_day(n):
            n += timedelta(days=1)
        return n
