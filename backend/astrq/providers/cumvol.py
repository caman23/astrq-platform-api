"""Cumulative-volume tracking for feeds without sequence numbers.

Vendors (SSI TotalVol, DNSE totalVolumeTraded) send the day's cumulative matched
volume with every trade. That lets us drop repeated snapshots and estimate how
much volume we missed between two received trades.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Hashable


class CumulativeVolumeTracker:
    def __init__(self, stats: Counter[str]) -> None:
        self.stats = stats
        self._last: dict[Hashable, float] = {}

    def accept(self, key: Hashable, total: float | None, volume: float) -> bool:
        """False if this trade is a repeat/out-of-order snapshot that must be dropped."""
        if total is None:
            return True
        prev = self._last.get(key)
        if prev is not None:
            if total <= prev:
                self.stats["repeat_dropped"] += 1
                return False
            missed = total - prev - volume
            if missed > 0:
                self.stats["missed_volume"] += int(missed)
        self._last[key] = total
        return True
