"""Market data provider interface. A new data vendor = a new adapter, nothing else."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from astrq.core.models import MarketEvent


@dataclass
class ProviderStatus:
    connected: bool = False
    last_event_at: datetime | None = None
    reconnects: int = 0
    finished: bool = False
    detail: str = ""
    stats: dict[str, Any] = field(default_factory=dict)  # vendor-specific diagnostics


class MarketDataProvider(Protocol):
    name: str
    status: ProviderStatus

    async def connect(self) -> None: ...

    async def subscribe(self, symbols: Sequence[str], channels: Sequence[str]) -> None: ...

    def events(self) -> AsyncIterator[MarketEvent]: ...

    async def close(self) -> None: ...
