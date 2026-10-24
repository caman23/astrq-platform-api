"""A failing or slow tool must not affect other tools (fault isolation)."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel

from astrq.core.config import ToolEntry
from astrq.core.context import ContextService
from astrq.core.models import MarketEvent, Signal
from astrq.core.offline import run_offline
from astrq.core.tool import BaseTool, Subscription, ToolContext
from astrq.tools.tick_counter import TickCounter, TickCounterConfig
from tests.helpers import FixedContext, settings, universe
from tests.scenarios import split_parent


class _Cfg(BaseModel):
    pass


class Exploding(BaseTool):
    id: ClassVar[str] = "exploding"
    version: ClassVar[str] = "0"
    config_model: ClassVar[type[BaseModel]] = _Cfg

    def subscriptions(self) -> list[Subscription]:
        return [Subscription(frozenset({"tick"}))]

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        raise RuntimeError("boom")

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]:
        raise RuntimeError("boom")


class Slow(Exploding):
    id: ClassVar[str] = "slow"

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        await asyncio.sleep(1)
        return []

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]:
        return []


async def test_faulty_tools_do_not_affect_others() -> None:
    events = split_parent()
    entry = ToolEntry(
        module="x:y", timeout_ms=20, max_consecutive_failures=3, breaker_cooldown_sec=60
    )
    tools = {
        "tick_counter": (TickCounter(TickCounterConfig(milestone=1)), ToolEntry(module="x:y")),
        "exploding": (Exploding(_Cfg()), entry),
        "slow": (Slow(_Cfg()), entry),
    }
    res = await run_offline(
        events, settings(), universe(), ContextService(FixedContext()), tools=tools
    )
    counted = [s for s in res.signals if s.tool_id == "tick_counter"]
    n_ticks = sum(1 for e in events if e.event_type == "tick")
    assert len(counted) == n_ticks  # healthy tool saw every tick

    boom = res.runners["exploding"].status
    assert boom.state == "breaker_open"
    assert 3 <= boom.errors < boom.events  # breaker stopped calling it after 3 in a row
    assert boom.skipped_breaker > 0
    slow = res.runners["slow"].status
    assert slow.timeouts == 3 and slow.state == "breaker_open"
