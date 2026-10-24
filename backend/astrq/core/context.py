"""Context/baseline service: non-realtime reference data for tools.

The hot path never performs I/O: tools call `ContextService.baseline()` which
only reads the in-memory cache. The cache is warmed by the scheduler before the
session (and lazily refreshed in the background).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable
from datetime import date
from typing import Protocol

from pydantic import BaseModel

from astrq.core.metrics import METRICS

log = logging.getLogger(__name__)


class Baseline(BaseModel):
    symbol: str
    trading_date: date
    median_trade_value: float  # VND, typical single-trade value over the last N sessions
    p95_trade_value: float
    avg_daily_value: float
    liquidity_tier: str  # "high" | "mid" | "low"
    source: str


class ContextProvider(Protocol):
    name: str

    async def baseline(self, symbol: str, trading_date: date) -> Baseline | None: ...


class NullContextProvider:
    name = "none"

    async def baseline(self, symbol: str, trading_date: date) -> Baseline | None:
        return None


class ContextService:
    def __init__(
        self,
        provider: ContextProvider,
        ttl_sec: float = 6 * 3600,
        max_retries: int = 3,
        backoff_sec: float = 0.5,
    ) -> None:
        self.provider = provider
        self.ttl_sec = ttl_sec
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self._cache: dict[tuple[str, date], tuple[float, Baseline | None]] = {}
        self.last_warm_error: str | None = None

    def baseline(self, symbol: str, trading_date: date) -> Baseline | None:
        """Hot-path read. Returns None when unknown (caller marks `baseline_cold`)."""
        hit = self._cache.get((symbol, trading_date))
        if hit is None:
            return None
        expires, value = hit
        if expires < time.monotonic():
            return value  # stale but still useful; refreshed by the next warm-up
        return value

    async def _fetch(self, symbol: str, trading_date: date) -> Baseline | None:
        delay = self.backoff_sec
        for attempt in range(1, self.max_retries + 1):
            try:
                return await self.provider.baseline(symbol, trading_date)
            except Exception as exc:
                self.last_warm_error = f"{type(exc).__name__}: {exc}"
                METRICS.inc("context_errors_total", provider=self.provider.name)
                if attempt == self.max_retries:
                    log.warning("baseline fetch failed for %s: %s", symbol, exc)
                    return None
                await asyncio.sleep(delay)
                delay *= 2
        return None

    async def warm(self, symbols: Iterable[str], trading_date: date, concurrency: int = 8) -> int:
        sem = asyncio.Semaphore(concurrency)
        ok = 0

        async def one(sym: str) -> None:
            nonlocal ok
            async with sem:
                b = await self._fetch(sym, trading_date)
            self._cache[(sym, trading_date)] = (time.monotonic() + self.ttl_sec, b)
            if b is not None:
                ok += 1

        await asyncio.gather(*(one(s) for s in symbols))
        METRICS.set("baseline_warm_symbols", ok)
        log.info("baseline warm-up for %s: %d symbols ok", trading_date, ok)
        return ok

    def coverage(self, symbols: Iterable[str], trading_date: date) -> float:
        syms = list(symbols)
        if not syms:
            return 0.0
        hits = sum(1 for s in syms if self.baseline(s, trading_date) is not None)
        return hits / len(syms)
