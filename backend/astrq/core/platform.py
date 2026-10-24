"""Wires the core together: provider -> ingest -> market bus -> tool runners ->
signal bus -> store / recorder / alert router / dashboard listeners.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any

from pydantic import TypeAdapter

from astrq.core.alerts import AlertRouter, CallbackChannel, Channel, TelegramChannel
from astrq.core.bus import InMemoryBus
from astrq.core.clock import Clock, EventClock, SimClock, WallClock
from astrq.core.config import Settings
from astrq.core.context import ContextProvider, ContextService, NullContextProvider
from astrq.core.ingest import Ingest
from astrq.core.market_rules import MarketRules
from astrq.core.metrics import METRICS
from astrq.core.models import MarketEvent, Severity, Signal
from astrq.core.recorder import JsonlRecorder
from astrq.core.registry import load_tool
from astrq.core.remote_store import R2Client, RemoteSync
from astrq.core.runner import ToolRunner
from astrq.core.sessions import SessionCalendar
from astrq.core.store import SignalStore
from astrq.core.tool import ToolContext
from astrq.core.universe import Universe
from astrq.providers.base import MarketDataProvider
from astrq.providers.dnse import DnseProvider
from astrq.providers.recorded_baseline import RecordedBaselineProvider
from astrq.providers.replay import ReplayProvider
from astrq.providers.ssi import SsiProvider
from astrq.providers.synthetic import SyntheticContextProvider, SyntheticProvider

log = logging.getLogger(__name__)

Listener = Callable[[dict[str, Any]], Awaitable[None]]
SIGNAL_ADAPTER: TypeAdapter[Signal] = TypeAdapter(Signal)


class Platform:
    def __init__(self, settings: Settings, stop_at_close: bool = False) -> None:
        self.s = settings
        self.calendar = SessionCalendar(settings.sessions)
        self.rules = MarketRules(settings.market_rules)
        self.universe = Universe(settings.path(settings.universe.file))
        self.clock: Clock
        self._advance: Callable[[Any], None] | None = None
        data_dir = settings.path(settings.storage.data_dir)
        self.data_dir = data_dir
        self.raw_recorder: JsonlRecorder | None = None
        self.provider = self._make_provider(stop_at_close)
        self.context = ContextService(self._make_context_provider(), settings.context.cache_ttl_sec)
        self.market_bus: InMemoryBus[MarketEvent] = InMemoryBus("market", settings.bus.queue_size)
        self.signal_bus: InMemoryBus[Signal] = InMemoryBus("signals", settings.bus.queue_size)
        self.store = SignalStore(self.calendar)

        self.tick_recorder = (
            JsonlRecorder(data_dir / "ticks", self.calendar, "ticks")
            if settings.storage.record_ticks
            else None
        )
        self.signal_recorder = (
            JsonlRecorder(data_dir / "signals", self.calendar, "signals")
            if settings.storage.record_signals
            else None
        )
        self.ingest = Ingest(
            self.market_bus,
            self.calendar,
            self.universe,
            on_event=self._record_tick,
            advance_clock=self._advance,
        )

        self.runners: dict[str, ToolRunner] = {}
        for tool_id, entry in settings.tools.items():
            tool = load_tool(tool_id, entry)
            ctx = ToolContext(
                tool_id=tool_id,
                config=tool.config if hasattr(tool, "config") else entry,
                universe=self.universe,
                context=self.context,
                calendar=self.calendar,
                rules=self.rules,
                clock=self.clock,
                log=logging.getLogger(f"astrq.tool.{tool_id}"),
            )
            self.runners[tool_id] = ToolRunner(
                tool,
                ctx,
                entry,
                self.market_bus,
                self.signal_bus,
                snapshot_dir=data_dir / "snapshots",
                queue_size=settings.bus.queue_size,
            )

        self.listeners: list[Listener] = []
        channels: list[Channel] = [
            CallbackChannel(
                "dashboard", self._dashboard_alert, settings.alerts.dashboard_min_severity, 600
            )
        ]
        self.telegram: TelegramChannel | None = None
        tg = settings.alerts.telegram
        if tg.enabled and tg.bot_token is not None and tg.chat_id:
            self.telegram = TelegramChannel(
                tg.bot_token.get_secret_value(),
                tg.chat_id,
                tg.min_severity,
                tg.max_per_minute,
                enabled_tools={"large_order_radar"},
            )
            channels.append(self.telegram)
        self.router = AlertRouter(
            channels,
            settings.alerts.cooldown_sec,
            tool_enabled=lambda t: t in self.runners and self.runners[t].status.enabled,
        )
        self._alert_q: asyncio.Queue[Signal] = asyncio.Queue(maxsize=10_000)
        self._tasks: list[asyncio.Task[Any]] = []
        self._jobs_done: dict[str, date] = {}
        self.started = False
        self.feed_error: str | None = None
        self.tick_rate = 0.0
        self.last_summary: str | None = None
        self.remote: RemoteSync | None = None
        rc = settings.storage.remote
        if rc.kind == "r2":
            self.remote = RemoteSync(R2Client(rc), data_dir, rc.keep_summary_days)

    # ------------------------------------------------------------ factories

    def _make_provider(self, stop_at_close: bool) -> MarketDataProvider:
        p = self.s.provider
        if p.kind == "synthetic":
            start = SyntheticProvider.start_time(p.synthetic, self.calendar)
            clock = SimClock(start, p.synthetic.speed)
            self.clock = clock
            if p.synthetic.speed <= 0:
                self._advance = clock.advance_to
            return SyntheticProvider(
                p.synthetic, self.universe.symbols, self.calendar, self.rules, clock, stop_at_close
            )
        if p.kind == "replay":
            ec = EventClock()
            self.clock = ec
            self._advance = ec.advance_to
            return ReplayProvider(self.s.path(p.replay.path), p.replay.speed)
        self.clock = WallClock()
        vendor_cfg = p.ssi if p.kind == "ssi" else p.dnse
        if vendor_cfg.record_raw:
            # raw vendor messages, kept to verify units/session codes against reality
            self.raw_recorder = JsonlRecorder(
                self.data_dir / "raw" / p.kind, self.calendar, f"raw_{p.kind}"
            )
        sink = self.raw_recorder.write_line if self.raw_recorder else None
        if p.kind == "ssi":
            return SsiProvider(p.ssi, self.calendar, raw_sink=sink)
        return DnseProvider(p.dnse, self.calendar, raw_sink=sink)

    def _make_context_provider(self) -> ContextProvider:
        kind = self.s.context.kind
        if kind == "synthetic":
            return SyntheticContextProvider(self.s.provider.synthetic.seed, self.rules)
        if kind == "recorded":
            return RecordedBaselineProvider(
                self.data_dir / "ticks",
                self.data_dir / "baselines",
                self.s.context.lookback_days,
                self.s.context.min_days,
            )
        if kind == "vndirect":
            # TODO(verify): VNDirect REST/MCP baseline source; see docs/adr/0004-baseline-source.md
            log.warning("context.kind=vndirect is not implemented yet; running baseline_cold")
        return NullContextProvider()

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        today = self.calendar.trading_date(self.clock.now())
        if self.remote is not None:  # disk-less host: bring back summaries + today's signals
            await self.remote.restore(today)
        self._reload_today_signals(today)
        await self.context.warm(self.universe.symbols, today)
        self._jobs_done["warm"] = today
        if self.tick_recorder:
            self.tick_recorder.start()
        if self.signal_recorder:
            self.signal_recorder.start()
        if self.raw_recorder:
            self.raw_recorder.start()
        for r in self.runners.values():
            await r.start()
        self._tasks = [
            asyncio.create_task(self._run_feed(), name="feed"),
            asyncio.create_task(self._signal_loop(), name="signals"),
            asyncio.create_task(self._alert_loop(), name="alerts"),
            asyncio.create_task(self._scheduler_loop(), name="scheduler"),
            asyncio.create_task(self._status_loop(), name="status"),
            asyncio.create_task(self._remote_loop(), name="remote"),
        ]
        if self.telegram is not None:
            self._tasks.append(
                asyncio.create_task(self.telegram.poll_commands(self.handle_command), name="tg")
            )
        self.started = True
        log.info("platform started: provider=%s tools=%s", self.provider.name, list(self.runners))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        for r in self.runners.values():
            await r.stop()
        await self.provider.close()
        if self.tick_recorder:
            await self.tick_recorder.close()
        if self.signal_recorder:
            await self.signal_recorder.close()
        if self.raw_recorder:
            await self.raw_recorder.close()
        if self.remote is not None:  # host stopping (e.g. spin-down): persist what we have
            await self.remote.upload_signals(self.calendar.trading_date(self.clock.now()))
            await self.remote.client.close()
        if self.telegram is not None:
            await self.telegram.close()
        self.started = False

    async def _remote_loop(self) -> None:
        if self.remote is None:
            return
        interval = self.s.storage.remote.signals_upload_interval_sec
        while True:
            await asyncio.sleep(interval)
            await self.remote.upload_signals(self.calendar.trading_date(self.clock.now()))

    def _reload_today_signals(self, today: date) -> None:
        """After a restart, reload today's recorded signals so the radar is not empty."""
        if self.signal_recorder is None:
            return
        f = self.signal_recorder.file_for(today)
        if not f.exists():
            return
        n = 0
        with f.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    self.store.add(SIGNAL_ADAPTER.validate_json(line))
                    n += 1
        log.info("reloaded %d signals for %s", n, today)

    # ------------------------------------------------------------ loops

    def _record_tick(self, ev: MarketEvent) -> None:
        if self.tick_recorder is not None:
            self.tick_recorder.write(ev, ev.ts_exchange)

    async def _run_feed(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self.provider.connect()
                await self.provider.subscribe(
                    self.universe.symbols, ["tick", "quote", "expected_price"]
                )
                self.feed_error = None
                backoff = 1.0
                await self.ingest.run(self.provider)
                if self.provider.status.finished:
                    log.info("feed finished")
                    return
                raise ConnectionError("stream ended unexpectedly")
            except asyncio.CancelledError:
                raise
            except NotImplementedError as exc:
                self.feed_error = str(exc)
                log.error("feed not available: %s", exc)
                return
            except Exception as exc:
                self.feed_error = f"{type(exc).__name__}: {exc}"
                self.provider.status.reconnects += 1
                METRICS.inc("feed_reconnects_total")
                log.warning("feed error, reconnecting in %.0fs: %s", backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _signal_loop(self) -> None:
        sub = self.signal_bus.subscribe("platform", maxsize=self.s.bus.queue_size)
        while True:
            s = await sub.get()
            self.store.add(s)
            if self.signal_recorder is not None:
                self.signal_recorder.write(s, s.ts)
            await self._notify({"type": "signal", "signal": s.model_dump(mode="json")})
            try:
                self._alert_q.put_nowait(s)
            except asyncio.QueueFull:
                METRICS.inc("alerts_dropped_total")

    async def _alert_loop(self) -> None:
        while True:
            s = await self._alert_q.get()
            await self.router.handle(s)

    async def _dashboard_alert(self, text: str, signal: Signal | None) -> None:
        await self._notify(
            {
                "type": "alert",
                "text": text,
                "signal_id": signal.signal_id if signal else None,
                "symbol": signal.symbol if signal else None,
                "severity": signal.severity.value if signal else None,
            }
        )

    async def _notify(self, msg: dict[str, Any]) -> None:
        for listener in list(self.listeners):
            try:
                await listener(msg)
            except Exception:
                with contextlib.suppress(ValueError):
                    self.listeners.remove(listener)

    async def _status_loop(self) -> None:
        prev = METRICS.counter("ticks_total")
        prev_t = time.monotonic()
        while True:
            await asyncio.sleep(2.0)  # status push cadence (bandwidth-friendly)
            cur, now = METRICS.counter("ticks_total"), time.monotonic()
            self.tick_rate = (cur - prev) / max(now - prev_t, 1e-6)
            METRICS.set("tick_rate_per_sec", self.tick_rate)
            prev, prev_t = cur, now
            await self._notify({"type": "status", "status": self.status()})

    async def _scheduler_loop(self) -> None:
        last_universe_check = time.monotonic()
        while True:
            await asyncio.sleep(0.5)
            now = self.clock.now()
            d = self.calendar.trading_date(now)
            local_t = self.calendar.local(now).time()
            if self.calendar.is_trading_day(d):
                if local_t >= self.s.context.warmup_time and self._jobs_done.get("warm") != d:
                    self._jobs_done["warm"] = d
                    await self.context.warm(self.universe.symbols, d)
                if local_t >= self.s.context.eod_summary_time and self._jobs_done.get("eod") != d:
                    self._jobs_done["eod"] = d
                    await self._eod_summary()
                    if self.remote is not None:
                        await self.remote.upload_signals(d)
                        await self.remote.upload_day_summary(d)
            if self._jobs_done.get("cleanup") != d:
                self._jobs_done["cleanup"] = d
                for rec in (self.tick_recorder, self.signal_recorder, self.raw_recorder):
                    if rec is not None:
                        rec.cleanup(self.s.storage.retention_days)
            if time.monotonic() - last_universe_check > self.s.universe.reload_interval_sec:
                last_universe_check = time.monotonic()
                if self.universe.maybe_reload():
                    await self.provider.subscribe(
                        self.universe.symbols, ["tick", "quote", "expected_price"]
                    )
                    await self.context.warm(self.universe.symbols, d)

    async def _eod_summary(self) -> None:
        parts: list[str] = []
        for r in self.runners.values():
            fn = getattr(r.tool, "daily_summary", None)
            if callable(fn):
                parts.append(str(fn(r.ctx)))
        if not parts:
            return
        text = "\n\n".join(parts)
        self.last_summary = text
        await self.router.broadcast_text(text)
        await self._notify({"type": "summary", "text": text})

    # ------------------------------------------------------------ control & status

    def set_tool_enabled(self, tool_id: str, enabled: bool) -> None:
        self.runners[tool_id].set_enabled(enabled)

    async def handle_command(self, text: str) -> str:
        """Telegram commands: /status, /on <tool>, /off <tool>, /min <info|notice|alert>."""
        parts = text.split()
        cmd = parts[0].split("@")[0].lower()
        if cmd == "/status":
            st = self.status()
            tools = ", ".join(
                f"{t['tool_id']}={'bật' if t['enabled'] else 'tắt'}({t['state']})"
                for t in st["tools"]
            )
            return (
                f"Phiên: {st['session']} · Feed: {st['provider']['detail']}\n"
                f"Tick/giây: {st['tick_rate']:.1f} · Tín hiệu hôm nay: {st['signals_today']}\n"
                f"Tools: {tools}"
            )
        if cmd in ("/on", "/off") and len(parts) == 2 and parts[1] in self.runners:
            self.set_tool_enabled(parts[1], cmd == "/on")
            return f"Đã {'bật' if cmd == '/on' else 'tắt'} {parts[1]}"
        if cmd == "/min" and len(parts) == 2 and parts[1] in Severity.__members__.values():
            if self.telegram is not None:
                self.telegram.min_severity = Severity(parts[1])
            return f"Mức độ tối thiểu Telegram: {parts[1]}"
        return "Lệnh: /status, /on <tool>, /off <tool>, /min <info|notice|alert>"

    def status(self) -> dict[str, Any]:
        now = self.clock.now()
        d = self.calendar.trading_date(now)
        ps = self.provider.status
        return {
            "now": now.isoformat(),
            "trading_date": d.isoformat(),
            "session": self.calendar.session_at(now).value,
            "clock": type(self.clock).__name__,
            "clock_speed": getattr(self.clock, "speed", 1.0),
            "provider": {
                "name": self.provider.name,
                "connected": ps.connected,
                "finished": ps.finished,
                "last_event_at": ps.last_event_at.isoformat() if ps.last_event_at else None,
                "reconnects": ps.reconnects,
                "detail": ps.detail,
                "stats": ps.stats,
                "error": self.feed_error,
            },
            "ingest": dict(self.ingest.stats),
            "tick_rate": round(self.tick_rate, 1),
            "latency": {
                "feed": METRICS.summary("feed_latency_seconds"),
                "internal": METRICS.summary("tick_to_signal_seconds", tool="large_order_radar"),
            },
            "baseline_coverage": round(self.context.coverage(self.universe.symbols, d), 3),
            "universe": {
                "name": self.universe.meta.name,
                "size": len(self.universe.symbols),
                "verified": self.universe.meta.verified,
                "as_of": self.universe.meta.as_of,
            },
            "signals_today": len(self.store.signals),
            "remote_storage": None
            if self.remote is None
            else {
                "last_upload": self.remote.last_upload.isoformat()
                if self.remote.last_upload
                else None,
                "last_error": self.remote.last_error,
            },
            "tools": [r.status_dict() for r in self.runners.values()],
            "listeners": len(self.listeners),
        }
