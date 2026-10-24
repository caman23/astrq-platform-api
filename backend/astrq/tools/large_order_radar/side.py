"""Trade side classification: provider flag > quote rule > tick rule (Lee-Ready style).

Inferred sides are approximations; their confidence is lower and trades inside
the spread are flagged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from astrq.core.models import Quote, Side, SideSource, Tick

CONFIDENCE = {
    SideSource.PROVIDER: 1.0,
    SideSource.QUOTE_RULE: 0.75,
    SideSource.TICK_RULE: 0.5,
    SideSource.UNKNOWN: 0.0,
}
INSIDE_SPREAD_CONFIDENCE = 0.5


@dataclass(frozen=True)
class Classified:
    side: Side
    source: SideSource
    inside_spread: bool = False

    @property
    def confidence(self) -> float:
        if self.source is SideSource.QUOTE_RULE and self.inside_spread:
            return INSIDE_SPREAD_CONFIDENCE
        return CONFIDENCE[self.source]


class SideClassifier:
    def __init__(self, quote_stale_sec: float = 15.0) -> None:
        self.quote_stale_sec = quote_stale_sec
        self._quotes: dict[str, tuple[float | None, float | None, datetime]] = {}
        self._last_price: dict[str, float] = {}
        self._last_dir: dict[str, Side] = {}

    def on_quote(self, q: Quote) -> None:
        self._quotes[q.symbol] = (q.best_bid, q.best_ask, q.ts_exchange)

    def classify(self, t: Tick) -> Classified:
        result = self._classify(t)
        prev = self._last_price.get(t.symbol)
        if prev is not None and t.price != prev:
            self._last_dir[t.symbol] = Side.BUY if t.price > prev else Side.SELL
        self._last_price[t.symbol] = t.price
        return result

    def _classify(self, t: Tick) -> Classified:
        if t.side is not Side.UNKNOWN and t.side_source is SideSource.PROVIDER:
            return Classified(t.side, SideSource.PROVIDER)
        if t.session.is_auction or t.is_negotiated:
            # single-price auction / negotiated deals have no aggressor
            return Classified(Side.UNKNOWN, SideSource.UNKNOWN)
        q = self._quotes.get(t.symbol)
        if q is not None:
            bid, ask, ts = q
            fresh = abs((t.ts_exchange - ts).total_seconds()) <= self.quote_stale_sec
            if fresh and bid is not None and ask is not None and ask >= bid:
                if t.price >= ask:
                    return Classified(Side.BUY, SideSource.QUOTE_RULE)
                if t.price <= bid:
                    return Classified(Side.SELL, SideSource.QUOTE_RULE)
                mid = (bid + ask) / 2
                if t.price > mid:
                    return Classified(Side.BUY, SideSource.QUOTE_RULE, inside_spread=True)
                if t.price < mid:
                    return Classified(Side.SELL, SideSource.QUOTE_RULE, inside_spread=True)
        prev = self._last_price.get(t.symbol)
        if prev is not None:
            if t.price > prev:
                return Classified(Side.BUY, SideSource.TICK_RULE)
            if t.price < prev:
                return Classified(Side.SELL, SideSource.TICK_RULE)
            last_dir = self._last_dir.get(t.symbol)
            if last_dir is not None:  # zero-tick: inherit last non-zero move
                return Classified(last_dir, SideSource.TICK_RULE)
        return Classified(Side.UNKNOWN, SideSource.UNKNOWN)

    def dump(self) -> dict[str, Any]:
        return {
            "quotes": {s: [b, a, ts.isoformat()] for s, (b, a, ts) in self._quotes.items()},
            "last_price": self._last_price,
            "last_dir": {s: d.value for s, d in self._last_dir.items()},
        }

    def load(self, data: dict[str, Any]) -> None:
        self._quotes = {
            s: (b, a, datetime.fromisoformat(ts)) for s, (b, a, ts) in data["quotes"].items()
        }
        self._last_price = {s: float(p) for s, p in data["last_price"].items()}
        self._last_dir = {s: Side(d) for s, d in data["last_dir"].items()}
