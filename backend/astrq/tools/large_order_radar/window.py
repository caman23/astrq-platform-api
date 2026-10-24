"""Per-symbol sliding-window flow metrics, updated online in O(1) amortized."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from astrq.core.models import Side


@dataclass
class DayTotals:
    large_buy: float = 0.0
    large_sell: float = 0.0
    large_unknown: float = 0.0
    putthrough_value: float = 0.0
    putthrough_count: int = 0
    groups_opened: int = 0
    auction_blocks: int = 0
    max_score: float = 0.0

    @property
    def net_large(self) -> float:
        return self.large_buy - self.large_sell


# (ts_epoch, side, value, flagged, price)
Entry = tuple[float, str, float, bool, float]


@dataclass
class SymbolWindow:
    window_sec: float
    entries: deque[Entry] = field(default_factory=deque)
    active_buy: float = 0.0
    active_sell: float = 0.0
    large_buy: float = 0.0
    large_sell: float = 0.0

    def _apply(self, e: Entry, sign: float) -> None:
        _, side, value, flagged, _ = e
        if side == Side.BUY.value:
            self.active_buy += sign * value
            if flagged:
                self.large_buy += sign * value
        elif side == Side.SELL.value:
            self.active_sell += sign * value
            if flagged:
                self.large_sell += sign * value

    def add(self, ts: float, side: Side, value: float, flagged: bool, price: float) -> None:
        e: Entry = (ts, side.value, value, flagged, price)
        self.entries.append(e)
        self._apply(e, 1.0)
        self.evict(ts)

    def evict(self, now: float) -> None:
        cutoff = now - self.window_sec
        while self.entries and self.entries[0][0] < cutoff:
            self._apply(self.entries.popleft(), -1.0)

    def side_concentration(self, side: Side) -> float:
        """Share of aggressive value on `side` coming from large/child-sized trades."""
        if side is Side.BUY:
            return self.large_buy / self.active_buy if self.active_buy > 0 else 0.0
        if side is Side.SELL:
            return self.large_sell / self.active_sell if self.active_sell > 0 else 0.0
        return 0.0

    def snapshot(self) -> dict[str, Any]:
        total = self.active_buy + self.active_sell
        first_price = self.entries[0][4] if self.entries else 0.0
        last_price = self.entries[-1][4] if self.entries else 0.0
        span = (self.entries[-1][0] - self.entries[0][0]) if len(self.entries) > 1 else 0.0
        large = self.large_buy + self.large_sell
        return {
            "window_sec": self.window_sec,
            "large_buy": round(self.large_buy),
            "large_sell": round(self.large_sell),
            "net_large": round(self.large_buy - self.large_sell),
            "active_buy": round(self.active_buy),
            "active_sell": round(self.active_sell),
            "large_ratio": round(large / total, 4) if total > 0 else 0.0,
            "large_value_per_min": round(large / max(span / 60, 1.0)),
            "price_change_pct": round((last_price - first_price) / first_price * 100, 3)
            if first_price
            else 0.0,
        }

    def dump(self) -> list[Entry]:
        return list(self.entries)

    def load(self, entries: list[list[Any]]) -> None:
        self.entries = deque()
        self.active_buy = self.active_sell = self.large_buy = self.large_sell = 0.0
        for ts, side, value, flagged, price in entries:
            e: Entry = (float(ts), str(side), float(value), bool(flagged), float(price))
            self.entries.append(e)
            self._apply(e, 1.0)
