"""Trivial sample tool: proves the plugin boundary (counts ticks per symbol)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, Field

from astrq.core.models import MarketEvent, Severity, Signal, Tick
from astrq.core.tool import BaseTool, Subscription, ToolContext


class TickCounterConfig(BaseModel):
    milestone: int = Field(2000, ge=1, description="Phát tín hiệu mỗi khi một mã đạt bội số này")


class TickCounter(BaseTool):
    id: ClassVar[str] = "tick_counter"
    version: ClassVar[str] = "1.0.0"
    name: ClassVar[str] = "Đếm tick (tool mẫu)"
    description: ClassVar[str] = "Tool mẫu: đếm số tick theo mã, phát tín hiệu theo mốc."
    config_model: ClassVar[type[TickCounterConfig]] = TickCounterConfig
    signal_kinds: ClassVar[list[str]] = ["tick_count_milestone"]

    def __init__(self, config: TickCounterConfig) -> None:
        super().__init__(config)
        self.cfg = config
        self.counts: dict[str, int] = {}

    def subscriptions(self) -> list[Subscription]:
        return [Subscription(frozenset({"tick"}), include_negotiated=False)]

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        if not isinstance(event, Tick):
            return []
        n = self.counts.get(event.symbol, 0) + 1
        self.counts[event.symbol] = n
        if n % self.cfg.milestone:
            return []
        key = f"{self.id}:{event.symbol}:{n}"
        return [
            Signal(
                signal_id=str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
                tool_id=self.id,
                tool_version=self.version,
                symbol=event.symbol,
                ts=event.ts_exchange,
                kind="tick_count_milestone",
                severity=Severity.INFO,
                features={"count": n, "summary_vi": [f"{event.symbol} đạt {n} tick"]},
                dedupe_key=key,
                correlation_id=f"{event.symbol}-count",
            )
        ]

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]:
        return []

    def snapshot(self) -> bytes:
        return json.dumps(self.counts).encode()

    def restore(self, data: bytes) -> None:
        self.counts = {k: int(v) for k, v in json.loads(data).items()}
