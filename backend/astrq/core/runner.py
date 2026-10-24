"""Runs one tool in isolation: own queue, timeout per call, circuit breaker,
periodic timer on the market clock, and snapshot/restore to disk.

A tool that raises or is slow only hurts itself: its queue may overflow (counted)
but the bus and other tools keep running.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Awaitable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from astrq.core.bus import Bus, Subscriber
from astrq.core.config import ToolEntry
from astrq.core.metrics import METRICS
from astrq.core.models import MarketEvent, Signal
from astrq.core.tool import Tool, ToolContext

log = logging.getLogger(__name__)


@dataclass
class ToolStatus:
    tool_id: str
    enabled: bool = True
    state: str = "created"
    events: int = 0
    signals: int = 0
    errors: int = 0
    timeouts: int = 0
    skipped_breaker: int = 0
    consecutive_failures: int = 0
    last_error: str | None = None
    queue_lag: int = 0
    dropped: int = 0
    restored_snapshot: bool = False


class ToolRunner:
    def __init__(
        self,
        tool: Tool,
        ctx: ToolContext,
        entry: ToolEntry,
        market_bus: Bus[MarketEvent],
        signal_bus: Bus[Signal],
        snapshot_dir: Path | None = None,
        queue_size: int = 20_000,
    ) -> None:
        self.tool = tool
        self.ctx = ctx
        self.entry = entry
        self.market_bus = market_bus
        self.signal_bus = signal_bus
        self.snapshot_dir = snapshot_dir
        self.queue_size = queue_size
        self.status = ToolStatus(tool_id=tool.id, enabled=entry.enabled)
        self._sub: Subscriber[MarketEvent] | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._breaker_until = 0.0
        self._consecutive: dict[str, int] = {}
        self._subs = tool.subscriptions()

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._restore()
        await self._guard(self.tool.on_start(self.ctx), "on_start", timeout=30.0)
        universe = self.ctx.universe
        self._sub = self.market_bus.subscribe(
            self.tool.id,
            lambda ev: any(s.matches(ev, universe) for s in self._subs),
            maxsize=self.queue_size,
        )
        self._tasks = [
            asyncio.create_task(self._event_loop(), name=f"tool-{self.tool.id}-events"),
            asyncio.create_task(self._timer_loop(), name=f"tool-{self.tool.id}-timer"),
        ]
        self.status.state = "running"

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        if self._sub is not None:
            self.market_bus.unsubscribe(self._sub)
        self.save_snapshot()
        await self._guard(self.tool.on_stop(), "on_stop", timeout=10.0)
        self.status.state = "stopped"

    def set_enabled(self, enabled: bool) -> None:
        self.status.enabled = enabled

    # ------------------------------------------------------------ processing

    async def _guard(
        self, coro: Awaitable[list[Signal] | None], where: str, timeout: float | None = None
    ) -> list[Signal] | None:
        limit = timeout if timeout is not None else self.entry.timeout_ms / 1000
        try:
            async with asyncio.timeout(limit):
                result = await coro
        except TimeoutError:
            self.status.timeouts += 1
            self._failure(where, f"{where}: timeout after {limit:.3f}s")
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.status.errors += 1
            self._failure(where, f"{where}: {type(exc).__name__}: {exc}")
            log.exception("tool %s failed in %s", self.tool.id, where)
            return None
        # A success only resets its own call site: a healthy on_timer must not
        # mask a failing on_event.
        self._consecutive[where] = 0
        self.status.consecutive_failures = max(self._consecutive.values(), default=0)
        return result

    def _failure(self, where: str, msg: str) -> None:
        self.status.last_error = msg
        self._consecutive[where] = self._consecutive.get(where, 0) + 1
        self.status.consecutive_failures = max(self._consecutive.values())
        METRICS.inc("tool_failures_total", tool=self.tool.id)
        if self._consecutive[where] >= self.entry.max_consecutive_failures:
            self._breaker_until = time.monotonic() + self.entry.breaker_cooldown_sec
            self.status.state = "breaker_open"
            log.error("tool %s: circuit breaker open (%s)", self.tool.id, msg)

    def _breaker_open(self) -> bool:
        if self._breaker_until and time.monotonic() < self._breaker_until:
            return True
        if self._breaker_until:  # cooldown over: half-open, give the tool another chance
            self._breaker_until = 0.0
            self._consecutive.clear()
            self.status.consecutive_failures = 0
            self.status.state = "running"
        return False

    def _emit(self, signals: list[Signal] | None, ev: MarketEvent | None = None) -> None:
        if not signals:
            return
        for s in signals:
            self.signal_bus.publish(s)
        self.status.signals += len(signals)
        METRICS.inc("signals_total", len(signals), tool=self.tool.id)
        if ev is not None and ev.ingest_mono is not None:
            METRICS.observe(
                "tick_to_signal_seconds", time.monotonic() - ev.ingest_mono, tool=self.tool.id
            )

    async def process_event(self, ev: MarketEvent) -> None:
        self.status.events += 1
        if not self.status.enabled:
            return
        if self._breaker_open():
            self.status.skipped_breaker += 1
            return
        self._emit(await self._guard(self.tool.on_event(ev, self.ctx), "on_event"), ev)

    async def process_timer(self, now: datetime) -> None:
        if not self.status.enabled or self._breaker_open():
            return
        self._emit(await self._guard(self.tool.on_timer(now, self.ctx), "on_timer"))

    async def _event_loop(self) -> None:
        assert self._sub is not None
        while True:
            ev = await self._sub.get()
            await self.process_event(ev)
            self.status.queue_lag = self._sub.lag
            self.status.dropped = self._sub.dropped

    async def _timer_loop(self) -> None:
        last = self.ctx.clock.now()
        last_snap = time.monotonic()
        while True:
            await asyncio.sleep(0.2)
            now = self.ctx.clock.now()
            if (now - last).total_seconds() >= self.entry.timer_interval_sec:
                last = now
                await self.process_timer(now)
            if self.snapshot_dir is not None and time.monotonic() - last_snap > 30:
                last_snap = time.monotonic()
                self.save_snapshot()

    # ------------------------------------------------------------ snapshots

    def _snap_paths(self) -> tuple[Path, Path] | None:
        if self.snapshot_dir is None:
            return None
        return (
            self.snapshot_dir / f"{self.tool.id}.snap",
            self.snapshot_dir / f"{self.tool.id}.meta.json",
        )

    def save_snapshot(self) -> None:
        paths = self._snap_paths()
        if paths is None:
            return
        try:
            data = self.tool.snapshot()
        except Exception as exc:
            log.warning("tool %s snapshot failed: %s", self.tool.id, exc)
            return
        if not data:
            return
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[union-attr]
        snap, meta = paths
        tmp = snap.with_suffix(".tmp")
        tmp.write_bytes(data)
        tmp.replace(snap)
        today = self.ctx.calendar.trading_date(self.ctx.clock.now())
        meta.write_text(
            json.dumps({"version": self.tool.version, "trading_date": today.isoformat()})
        )

    def _restore(self) -> None:
        paths = self._snap_paths()
        if paths is None or not paths[0].exists() or not paths[1].exists():
            return
        snap, meta_path = paths
        meta = json.loads(meta_path.read_text())
        today = self.ctx.calendar.trading_date(self.ctx.clock.now()).isoformat()
        if meta.get("version") != self.tool.version or meta.get("trading_date") != today:
            log.info("tool %s: ignoring stale snapshot %s", self.tool.id, meta)
            return
        try:
            self.tool.restore(snap.read_bytes())
            self.status.restored_snapshot = True
            log.info("tool %s: restored snapshot from %s", self.tool.id, today)
        except Exception as exc:
            log.warning("tool %s: restore failed: %s", self.tool.id, exc)

    def status_dict(self) -> dict[str, object]:
        if self._sub is not None:
            self.status.queue_lag = self._sub.lag
            self.status.dropped = self._sub.dropped
        return asdict(self.status)
