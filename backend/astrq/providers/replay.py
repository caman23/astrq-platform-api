"""Replay recorded events (JSONL, one canonical MarketEvent per line)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from pathlib import Path

from pydantic import TypeAdapter

from astrq.core.models import MarketEvent
from astrq.providers.base import ProviderStatus

EVENT_ADAPTER: TypeAdapter[MarketEvent] = TypeAdapter(MarketEvent)


def read_events(path: Path) -> list[MarketEvent]:
    out: list[MarketEvent] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(EVENT_ADAPTER.validate_json(line))
    return out


class ReplayProvider:
    name = "replay"

    def __init__(self, path: Path, speed: float = 1.0) -> None:
        self.path = path
        self.speed = speed
        self.status = ProviderStatus()
        self._symbols: set[str] | None = None
        self._closed = False

    async def connect(self) -> None:
        if not self.path.exists():
            raise FileNotFoundError(f"replay file not found: {self.path}")
        self.status.connected = True
        self.status.detail = f"replay {self.path.name} speed={self.speed}x"

    async def subscribe(self, symbols: Sequence[str], channels: Sequence[str]) -> None:
        self._symbols = set(symbols)

    async def close(self) -> None:
        self._closed = True
        self.status.connected = False

    async def events(self) -> AsyncIterator[MarketEvent]:
        first_ts: datetime | None = None
        loop = asyncio.get_running_loop()
        wall0 = loop.time()
        with self.path.open(encoding="utf-8") as fh:
            for line in fh:
                if self._closed:
                    return
                line = line.strip()
                if not line:
                    continue
                ev = EVENT_ADAPTER.validate_json(line)
                if self._symbols is not None and ev.symbol not in self._symbols:
                    continue
                if self.speed > 0:
                    first_ts = first_ts or ev.ts_exchange
                    due = (ev.ts_exchange - first_ts).total_seconds() / self.speed
                    wait = due - (loop.time() - wall0)
                    if wait > 0:
                        await asyncio.sleep(wait)
                self.status.last_event_at = ev.ts_received
                yield ev
        self.status.finished = True
