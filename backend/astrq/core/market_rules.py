"""Exchange rules (tick size ladder) loaded from config."""

from __future__ import annotations

from astrq.core.config import MarketRulesConfig


class MarketRules:
    def __init__(self, cfg: MarketRulesConfig) -> None:
        self._by_exchange: dict[str, list[tuple[float, float]]] = {}
        for r in cfg.tick_sizes:
            bound = r.below if r.below is not None else float("inf")
            self._by_exchange.setdefault(r.exchange.upper(), []).append((bound, r.size))
        for rules in self._by_exchange.values():
            rules.sort()

    def tick_size(self, price: float, exchange: str = "HOSE") -> float:
        rules = self._by_exchange.get(exchange.upper())
        if not rules:
            return 10.0
        for bound, size in rules:
            if price < bound:
                return size
        return rules[-1][1]

    def round_to_tick(self, price: float, exchange: str = "HOSE") -> float:
        step = self.tick_size(price, exchange)
        return round(round(price / step) * step, 2)
