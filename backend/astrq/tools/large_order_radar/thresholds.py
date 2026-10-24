"""Absolute and per-symbol relative thresholds."""

from __future__ import annotations

from astrq.core.context import Baseline
from astrq.tools.large_order_radar.config import RadarConfig


class Thresholds:
    def __init__(self, cfg: RadarConfig) -> None:
        self.cfg = cfg

    def relative_k(self, baseline: Baseline) -> float:
        return self.cfg.tier_relative_k.get(baseline.liquidity_tier, self.cfg.relative_k)

    def large_triggers(self, value: float, baseline: Baseline | None) -> list[str]:
        """Which 'large trade' thresholds this single trade satisfies."""
        out: list[str] = []
        if value >= self.cfg.abs_threshold_value:
            out.append("absolute")
        if baseline is not None and baseline.median_trade_value > 0:
            bar = max(
                self.cfg.relative_min_value, self.relative_k(baseline) * baseline.median_trade_value
            )
            if value >= bar:
                out.append("relative")
        return out

    def is_child_candidate(self, value: float, baseline: Baseline | None) -> bool:
        """May this trade be part of a split parent order?"""
        if baseline is None or baseline.median_trade_value <= 0:
            return value >= self.cfg.child_min_value
        bar = max(
            self.cfg.child_floor_value, self.cfg.child_relative_k * baseline.median_trade_value
        )
        return value >= bar

    def group_open_value(self, baseline: Baseline | None) -> float:
        if baseline is None or baseline.median_trade_value <= 0:
            return self.cfg.group_open_value
        rel = self.cfg.group_relative_k * baseline.median_trade_value
        return max(self.cfg.abs_threshold_value, min(self.cfg.group_open_value, rel))

    def auction_value(self, baseline: Baseline | None) -> float:
        if baseline is None:
            return self.cfg.auction_min_value
        return max(
            self.cfg.auction_min_value, self.cfg.auction_relative_k * baseline.median_trade_value
        )
