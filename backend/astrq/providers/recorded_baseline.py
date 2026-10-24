"""Baselines computed from our own recorded tick files (ADR-0004).

For each finished trading day `data/ticks/YYYY-MM-DD.jsonl`, a per-symbol summary
(median / p95 trade value, matched value) of continuous-session, non-negotiated
trades is computed once and cached in `data/baselines/YYYY-MM-DD.json`. The
baseline for day D aggregates the last `lookback_days` summaries before D; with
fewer than `min_days` recorded sessions a symbol stays `baseline_cold`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

from astrq.core.context import Baseline

log = logging.getLogger(__name__)

CONTINUOUS = {"continuous_am", "continuous_pm"}
# Liquidity tiers by average daily matched value (VND). Starting points; tune on data.
TIER_HIGH = 200e9
TIER_MID = 50e9


def _quantile(sorted_vals: list[float], q: float) -> float:
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


def summarize_day(tick_file: Path) -> dict[str, dict[str, float]]:
    values: dict[str, list[float]] = {}
    with tick_file.open(encoding="utf-8") as fh:
        for line in fh:
            if '"event_type":"tick"' not in line:
                continue
            ev = json.loads(line)
            if ev.get("is_negotiated") or ev.get("session") not in CONTINUOUS:
                continue
            values.setdefault(ev["symbol"], []).append(float(ev["value"]))
    out: dict[str, dict[str, float]] = {}
    for sym, vals in values.items():
        vals.sort()
        out[sym] = {
            "n": len(vals),
            "median": _quantile(vals, 0.5),
            "p95": _quantile(vals, 0.95),
            "total": sum(vals),
        }
    return out


class RecordedBaselineProvider:
    name = "recorded"

    def __init__(
        self, ticks_dir: Path, cache_dir: Path, lookback_days: int = 10, min_days: int = 3
    ) -> None:
        self.ticks_dir = ticks_dir
        self.cache_dir = cache_dir
        self.lookback_days = lookback_days
        self.min_days = min_days
        self._by_date: dict[date, dict[str, Baseline]] = {}
        self._lock = asyncio.Lock()

    def _day_summary(self, d: date) -> dict[str, dict[str, float]] | None:
        cached = self.cache_dir / f"{d.isoformat()}.json"
        if cached.exists():
            data: dict[str, dict[str, float]] = json.loads(cached.read_text())
            return data
        src = self.ticks_dir / f"{d.isoformat()}.jsonl"
        if not src.exists():
            return None
        summary = summarize_day(src)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cached.write_text(json.dumps(summary))
        return summary

    def _compute(self, d: date) -> dict[str, Baseline]:
        # A day is available if we have its raw ticks or an already computed summary
        # (summaries may be restored from remote storage on disk-less hosts).
        known: set[date] = set()
        for f in [
            *self.ticks_dir.glob("????-??-??.jsonl"),
            *self.cache_dir.glob("????-??-??.json"),
        ]:
            try:
                known.add(date.fromisoformat(f.stem))
            except ValueError:
                continue
        days = sorted((x for x in known if x < d), reverse=True)[: self.lookback_days]
        per_symbol: dict[str, list[dict[str, float]]] = {}
        for fd in days:
            summary = self._day_summary(fd)
            for sym, s in (summary or {}).items():
                per_symbol.setdefault(sym, []).append(s)
        out: dict[str, Baseline] = {}
        for sym, rows in per_symbol.items():
            if len(rows) < self.min_days:
                continue
            avg_daily = sum(r["total"] for r in rows) / len(rows)
            out[sym] = Baseline(
                symbol=sym,
                trading_date=d,
                median_trade_value=median(r["median"] for r in rows),
                p95_trade_value=median(r["p95"] for r in rows),
                avg_daily_value=avg_daily,
                liquidity_tier="high"
                if avg_daily >= TIER_HIGH
                else "mid"
                if avg_daily >= TIER_MID
                else "low",
                source=f"recorded:{len(rows)}d",
            )
        log.info("recorded baselines for %s: %d symbols from %d days", d, len(out), len(days))
        return out

    async def baseline(self, symbol: str, trading_date: date) -> Baseline | None:
        async with self._lock:
            if trading_date not in self._by_date:
                self._by_date[trading_date] = await asyncio.to_thread(self._compute, trading_date)
        return self._by_date[trading_date].get(symbol)

    def describe(self) -> dict[str, Any]:
        return {"lookback_days": self.lookback_days, "min_days": self.min_days}
