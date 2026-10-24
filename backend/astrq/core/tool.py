"""Plugin contract. A tool only sees bus events and `ToolContext`; it never talks
to a data provider directly. Adding a tool must not require changing the core.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Protocol, runtime_checkable

from pydantic import BaseModel

from astrq.core.clock import Clock
from astrq.core.context import ContextService
from astrq.core.market_rules import MarketRules
from astrq.core.models import MarketEvent, Signal, Tick
from astrq.core.sessions import SessionCalendar
from astrq.core.universe import Universe


@dataclass(frozen=True)
class Subscription:
    event_types: frozenset[str]
    symbols: frozenset[str] | None = None  # None = whole current universe
    include_negotiated: bool = True

    def matches(self, event: MarketEvent, universe: Universe) -> bool:
        if event.event_type not in self.event_types:
            return False
        if self.symbols is None:
            if event.symbol not in universe:
                return False
        elif event.symbol not in self.symbols:
            return False
        return not (not self.include_negotiated and isinstance(event, Tick) and event.is_negotiated)

    def describe(self) -> dict[str, Any]:
        return {
            "event_types": sorted(self.event_types),
            "symbols": "universe" if self.symbols is None else sorted(self.symbols),
            "include_negotiated": self.include_negotiated,
        }


class ToolManifest(BaseModel):
    id: str
    version: str
    name: str
    description: str
    subscriptions: list[dict[str, Any]]
    config_schema: dict[str, Any]
    signal_kinds: list[str]
    has_view: bool = False


@dataclass
class ToolContext:
    tool_id: str
    config: BaseModel
    universe: Universe
    context: ContextService
    calendar: SessionCalendar
    rules: MarketRules
    clock: Clock
    log: logging.Logger = field(default_factory=lambda: logging.getLogger("astrq.tool"))


@runtime_checkable
class Tool(Protocol):
    id: str
    version: str

    def manifest(self) -> ToolManifest: ...

    def subscriptions(self) -> list[Subscription]: ...

    async def on_start(self, ctx: ToolContext) -> None: ...

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]: ...

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]: ...

    def snapshot(self) -> bytes: ...

    def restore(self, data: bytes) -> None: ...

    async def on_stop(self) -> None: ...


class BaseTool:
    """Convenience base with no-op defaults. Subclasses set the class attributes."""

    id: ClassVar[str]
    version: ClassVar[str]
    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    config_model: ClassVar[type[BaseModel]]
    signal_kinds: ClassVar[list[str]] = []

    def __init__(self, config: BaseModel) -> None:
        self.config = config

    def manifest(self) -> ToolManifest:
        return ToolManifest(
            id=self.id,
            version=self.version,
            name=self.name or self.id,
            description=self.description,
            subscriptions=[s.describe() for s in self.subscriptions()],
            config_schema=self.config_model.model_json_schema(),
            signal_kinds=list(self.signal_kinds),
            has_view=callable(getattr(self, "view", None)),
        )

    def subscriptions(self) -> list[Subscription]:
        return []

    async def on_start(self, ctx: ToolContext) -> None:
        return None

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        return []

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]:
        return []

    def snapshot(self) -> bytes:
        return b""

    def restore(self, data: bytes) -> None:
        return None

    async def on_stop(self) -> None:
        return None


class ToolView(Protocol):
    """Optional: a tool may expose an aggregated view built from its own signals."""

    def view(
        self, signals: Sequence[Signal], ctx: ToolContext, params: dict[str, str] | None = None
    ) -> dict[str, Any]: ...
