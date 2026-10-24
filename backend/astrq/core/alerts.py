"""Alert router: dedupe, per (symbol, side) cooldown with coalescing, per-channel
rate limit, severity/tool filters. Channels: dashboard (WebSocket) and Telegram.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from astrq.core import fmt
from astrq.core.metrics import METRICS
from astrq.core.models import Alert, Severity, Signal

log = logging.getLogger(__name__)


class Channel(Protocol):
    name: str
    min_severity: Severity
    max_per_minute: int
    enabled_tools: set[str] | None  # None = all

    async def send(self, text: str, signal: Signal | None) -> None: ...


def render_signal(s: Signal, merged: int = 0) -> str:
    icon = {"buy": "🟢", "sell": "🔴"}.get(s.side.value, "🟡")
    head = (
        f"{icon} {fmt.SIDE_VI[s.side.value]} LỚN · {s.symbol} · "
        f"{fmt.SEVERITY_VI[s.severity.value]} {s.score:.0f}"
    )
    lines = [head, *s.features.get("summary_vi", [])]
    if s.quality_flags:
        lines.append("Cờ: " + ", ".join(fmt.FLAG_VI.get(f, f) for f in s.quality_flags))
    if merged:
        lines.append(f"(+{merged} cập nhật cùng mã đã gộp)")
    lines.append(f"Lúc {fmt.hms(s.ts)} (giờ VN)")
    lines.append(fmt.DISCLAIMER_VI)
    return "\n".join(lines)


@dataclass
class _ChannelState:
    sent_keys: set[str] = field(default_factory=set)
    cooldown: dict[tuple[str, str], tuple[datetime, int]] = field(default_factory=dict)
    merged: dict[tuple[str, str], int] = field(default_factory=dict)
    sent_wall: deque[float] = field(default_factory=deque)


class AlertRouter:
    def __init__(
        self,
        channels: list[Channel],
        cooldown_sec: float,
        tool_enabled: Callable[[str], bool] = lambda _t: True,
        wall: Callable[[], float] = time.monotonic,
    ) -> None:
        self.channels = channels
        self.cooldown_sec = cooldown_sec
        self.tool_enabled = tool_enabled
        self.wall = wall
        self._state: dict[str, _ChannelState] = {c.name: _ChannelState() for c in channels}
        self.history: deque[Alert] = deque(maxlen=500)

    def decide(self, s: Signal, ch: Channel) -> str:
        """Pure decision (plus state update when 'send'). Returns the reason."""
        st = self._state[ch.name]
        if not self.tool_enabled(s.tool_id):
            return "tool_disabled"
        if ch.enabled_tools is not None and s.tool_id not in ch.enabled_tools:
            return "tool_filtered"
        if s.severity.rank < ch.min_severity.rank:
            return "below_severity"
        if s.dedupe_key in st.sent_keys:
            return "duplicate"
        ck = (s.symbol, s.side.value)
        last = st.cooldown.get(ck)
        if last is not None:
            last_ts, last_rank = last
            within = (s.ts - last_ts).total_seconds() < self.cooldown_sec
            if within and s.severity.rank <= last_rank:
                st.merged[ck] = st.merged.get(ck, 0) + 1
                return "cooldown"
        now = self.wall()
        while st.sent_wall and now - st.sent_wall[0] > 60:
            st.sent_wall.popleft()
        if len(st.sent_wall) >= ch.max_per_minute:
            st.merged[ck] = st.merged.get(ck, 0) + 1
            return "rate_limited"
        st.sent_wall.append(now)
        st.sent_keys.add(s.dedupe_key)
        st.cooldown[ck] = (s.ts, s.severity.rank)
        return "send"

    async def handle(self, s: Signal) -> None:
        for ch in self.channels:
            reason = self.decide(s, ch)
            if reason != "send":
                METRICS.inc("alerts_suppressed_total", channel=ch.name, reason=reason)
                continue
            merged = self._state[ch.name].merged.pop((s.symbol, s.side.value), 0)
            text = render_signal(s, merged)
            status = "sent"
            try:
                await ch.send(text, s)
            except Exception as exc:
                status = "failed"
                log.warning("alert channel %s failed: %s", ch.name, type(exc).__name__)
            METRICS.inc("alerts_total", channel=ch.name, status=status)
            self.history.append(
                Alert(
                    signal_id=s.signal_id,
                    channel=ch.name,
                    rendered_text=text,
                    sent_at=datetime.now(UTC),
                    status=status,
                )
            )

    async def broadcast_text(self, text: str) -> None:
        for ch in self.channels:
            try:
                await ch.send(text, None)
            except Exception as exc:
                log.warning("broadcast to %s failed: %s", ch.name, type(exc).__name__)


class CallbackChannel:
    """Delivers rendered alerts to an in-process callback (dashboard WebSocket)."""

    def __init__(
        self,
        name: str,
        callback: Callable[[str, Signal | None], Awaitable[None]],
        min_severity: Severity = Severity.NOTICE,
        max_per_minute: int = 120,
    ) -> None:
        self.name = name
        self.callback = callback
        self.min_severity = min_severity
        self.max_per_minute = max_per_minute
        self.enabled_tools: set[str] | None = None

    async def send(self, text: str, signal: Signal | None) -> None:
        await self.callback(text, signal)


class TelegramChannel:
    """Telegram Bot API (sendMessage / getUpdates). Token is never logged."""

    API = "https://api.telegram.org"

    def __init__(
        self,
        token: str,
        chat_id: str,
        min_severity: Severity,
        max_per_minute: int,
        enabled_tools: set[str] | None = None,
    ) -> None:
        self.name = "telegram"
        self._token = token
        self.chat_id = chat_id
        self.min_severity = min_severity
        self.max_per_minute = max_per_minute
        self.enabled_tools = enabled_tools
        self._client = httpx.AsyncClient(timeout=15.0)

    def _url(self, method: str) -> str:
        return f"{self.API}/bot{self._token}/{method}"

    async def send(self, text: str, signal: Signal | None) -> None:
        r = await self._client.post(
            self._url("sendMessage"), json={"chat_id": self.chat_id, "text": text}
        )
        if r.status_code != 200:
            raise RuntimeError(f"telegram sendMessage HTTP {r.status_code}")

    async def poll_commands(self, handler: Callable[[str], Awaitable[str]]) -> None:
        """Long-poll getUpdates; only messages from the configured chat are honored."""
        offset = 0
        while True:
            try:
                r = await self._client.get(
                    self._url("getUpdates"),
                    params={"timeout": 30, "offset": offset},
                    timeout=40.0,
                )
                data: dict[str, Any] = r.json()
                for upd in data.get("result", []):
                    offset = max(offset, int(upd["update_id"]) + 1)
                    msg = upd.get("message") or {}
                    if str((msg.get("chat") or {}).get("id")) != str(self.chat_id):
                        continue
                    text = (msg.get("text") or "").strip()
                    if text.startswith("/"):
                        reply = await handler(text)
                        await self.send(reply, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("telegram polling error: %s", type(exc).__name__)
                await asyncio.sleep(5)

    async def close(self) -> None:
        await self._client.aclose()
