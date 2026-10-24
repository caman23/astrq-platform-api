"""DNSE LightSpeed API market data provider (WebSocket, JSON encoding).

Protocol (official docs, sdk/build_websocket):
  connect {ws_url}?encoding=json -> server "welcome"
  -> {"action":"auth","api_key","signature","timestamp","nonce"} within 30 s, where
     signature = hex(HMAC-SHA256(api_secret, f"{api_key}:{timestamp}:{nonce}")),
     timestamp = unix seconds (±5 min), nonce = unix microseconds (never reused)
  -> "auth_success" | {"action":"error","code":"AUTH_FAILED"|...} then close
  -> {"action":"subscribe","channels":[{"name":"tick_extra.G1.json","symbols":[...]}]}
  server {"action":"ping","timestamp"} -> reply {"action":"pong","timestamp"} within 60 s;
  connections last at most 8 h ("connection_expired") -> reconnect with a NEW nonce.
Reconnects/backoff are owned by the platform feed loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

from astrq.core.config import DnseConfig
from astrq.core.models import MarketEvent
from astrq.core.sessions import SessionCalendar
from astrq.providers.base import ProviderStatus
from astrq.providers.dnse.parse import DnseNormalizer

log = logging.getLogger(__name__)

RawSink = Callable[[str, datetime], None]


class DnseAuthError(Exception):
    """Rejected credentials/signature (message never contains the secret)."""


def build_auth_message(api_key: str, api_secret: str, now: float | None = None) -> dict[str, Any]:
    t = time.time() if now is None else now
    timestamp = int(t)
    nonce = str(int(t * 1_000_000))
    sig = hmac.new(
        api_secret.encode(), f"{api_key}:{timestamp}:{nonce}".encode(), hashlib.sha256
    ).hexdigest()
    return {
        "action": "auth",
        "api_key": api_key,
        "signature": sig,
        "timestamp": timestamp,
        "nonce": nonce,
    }


class DnseProvider:
    name = "dnse"

    def __init__(
        self, cfg: DnseConfig, calendar: SessionCalendar, raw_sink: RawSink | None = None
    ) -> None:
        self.cfg = cfg
        self.calendar = calendar
        self.raw_sink = raw_sink if cfg.record_raw else None
        self.status = ProviderStatus(detail="DNSE LightSpeed API")
        self.normalizer = DnseNormalizer(cfg, calendar)
        self._ws: ClientConnection | None = None
        self._symbols: list[str] = []
        self._streaming = False
        self._closed = False
        self._server_limits: dict[str, Any] = {}
        self._active: list[dict[str, Any]] = []
        self._coverage: dict[str, str] = {}

    async def _recv_json(self, timeout: float) -> dict[str, Any]:
        assert self._ws is not None
        raw = await asyncio.wait_for(self._ws.recv(), timeout)
        msg = json.loads(raw)
        return msg if isinstance(msg, dict) else {}

    async def connect(self) -> None:
        await self._drop()
        if not self.cfg.api_key or self.cfg.api_secret is None:
            raise DnseAuthError("missing DNSE api_key/api_secret")
        self._ws = await connect(
            f"{self.cfg.ws_url}?encoding=json", open_timeout=20, ping_interval=30, max_size=None
        )
        welcome = await self._recv_json(15)
        if welcome.get("action") != "welcome":
            log.warning("DNSE: unexpected first message %s", str(welcome)[:200])
        await self._ws.send(
            json.dumps(build_auth_message(self.cfg.api_key, self.cfg.api_secret.get_secret_value()))
        )
        resp = await self._recv_json(15)
        if resp.get("action") == "auth_success":
            self._server_limits = {
                "capabilities": resp.get("capabilities"),
                "rate_limit": resp.get("rate_limit"),
            }
        else:
            code = resp.get("code", "?")
            await self._drop()
            if code == "AUTH_FAILED":
                raise DnseAuthError(f"DNSE auth failed: {resp.get('message')}")
            raise ConnectionError(f"DNSE auth error {code}: {resp.get('message')}")
        self.status.connected = True
        self.status.detail = "DNSE LightSpeed API · " + ", ".join(self.cfg.channels)

    def _stream_budget(self) -> int:
        rl = self._server_limits.get("rate_limit")
        srv = rl.get("subscriptions_max") if isinstance(rl, dict) else None
        if isinstance(srv, int) and srv > 0:
            return min(self.cfg.max_streams, srv)
        return self.cfg.max_streams

    def plan_subscriptions(self) -> list[dict[str, Any]]:
        """One subscribe message per channel, in priority order, within the stream budget.

        DNSE counts streams as channel x symbol (observed live: "MAX_CHANNELS_EXCEEDED:
        max 200 channels for tier normalUser"), and a rejected message subscribes nothing,
        so channels are sent separately and lower-priority ones get what is left.
        """
        budget = self._stream_budget()
        plan: list[dict[str, Any]] = []
        self._coverage = {}
        for ch in self.cfg.channels:
            take = self._symbols[: max(0, budget)]
            budget -= len(take)
            self._coverage[ch] = f"{len(take)}/{len(self._symbols)}"
            if take:
                plan.append(
                    {"action": "subscribe", "channels": [{"name": f"{ch}.json", "symbols": take}]}
                )
            if len(take) < len(self._symbols):
                log.warning(
                    "DNSE stream budget: %s covers %d/%d symbols", ch, len(take), len(self._symbols)
                )
        return plan

    async def _send_plan(self) -> None:
        assert self._ws is not None
        for msg in self._active:  # replace the previous subscription set
            await self._ws.send(json.dumps({**msg, "action": "unsubscribe"}))
        self._active = self.plan_subscriptions()
        for msg in self._active:
            await self._ws.send(json.dumps(msg))

    async def subscribe(self, symbols: Sequence[str], channels: Sequence[str]) -> None:
        self._symbols = sorted({s.upper() for s in symbols})
        if self._ws is not None and self._streaming:  # universe changed mid-stream
            await self._send_plan()

    def _idle_limit(self) -> float | None:
        s = self.calendar.session_at(datetime.now(UTC))
        return self.cfg.idle_reconnect_sec if (s.is_continuous or s.is_auction) else None

    async def _control(self, msg: dict[str, Any]) -> None:
        action = msg.get("action")
        assert self._ws is not None
        if action == "ping":
            pong: dict[str, Any] = {"action": "pong"}
            if "timestamp" in msg:
                pong["timestamp"] = msg["timestamp"]
            await self._ws.send(json.dumps(pong))
        elif action == "subscribed":
            self.normalizer.stats["subscribe_acks"] += 1
        elif action == "connection_expired":
            raise ConnectionError("DNSE connection expired (8h limit)")
        elif action == "error":
            self.normalizer.stats[f"server_error_{msg.get('code', '?')}"] += 1
            log.warning("DNSE error %s: %s", msg.get("code"), msg.get("message"))

    async def events(self) -> AsyncIterator[MarketEvent]:
        if self._ws is None:
            raise ConnectionError("DNSE stream not connected")
        self._active = []  # fresh connection: nothing subscribed yet
        await self._send_plan()
        self._streaming = True
        self.status.stats = {**self.normalizer.summary(), "subscriptions": self._coverage}
        while not self._closed:
            limit = self._idle_limit()
            try:
                raw = await (asyncio.wait_for(self._ws.recv(), limit) if limit else self._ws.recv())
            except TimeoutError:
                raise ConnectionError("DNSE stream silent during a session") from None
            except Exception as exc:  # websockets ConnectionClosed*
                self.status.connected = False
                raise ConnectionError(f"DNSE stream closed: {exc}") from None
            text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                self.normalizer.stats["undecodable"] += 1
                continue
            if not isinstance(msg, dict):
                continue
            if "action" in msg:
                await self._control(msg)
                continue
            received = datetime.now(UTC)
            if self.raw_sink is not None:
                self.raw_sink(text, received)
            for ev in self.normalizer.events(msg, received):
                self.status.last_event_at = received
                yield ev
            self.status.stats = {
                **self.normalizer.summary(),
                "subscriptions": self._coverage,
                "server": self._server_limits,
            }

    async def _drop(self) -> None:
        self._streaming = False
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        self.status.connected = False

    async def close(self) -> None:
        self._closed = True
        await self._drop()
