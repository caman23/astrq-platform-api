"""SSI FastConnect Data market data provider.

Lifecycle per (re)connect, driven by the platform feed loop (which owns backoff):
connect() -> token (cached, refreshed 1h before expiry) -> SignalR negotiate + WS
-> subscribe() -> events() invokes SwitchChannels and yields normalized events until
the socket closes or stays silent too long during a session, then raises so the
platform reconnects.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import UTC, datetime

import httpx

from astrq.core.config import SsiConfig
from astrq.core.models import MarketEvent
from astrq.core.sessions import SessionCalendar
from astrq.providers.base import ProviderStatus
from astrq.providers.ssi.parse import SsiNormalizer, decode_broadcast
from astrq.providers.ssi.rest import SsiRestClient
from astrq.providers.ssi.signalr import SignalRAuthError, SignalRClient

log = logging.getLogger(__name__)

RawSink = Callable[[str, datetime], None]


class SsiProvider:
    name = "ssi"

    def __init__(
        self,
        cfg: SsiConfig,
        calendar: SessionCalendar,
        raw_sink: RawSink | None = None,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.cfg = cfg
        self.calendar = calendar
        self.raw_sink = raw_sink if cfg.record_raw else None
        self.status = ProviderStatus(detail="SSI FastConnect Data")
        self._http = http or httpx.AsyncClient(timeout=20.0)  # TLS verification on
        self._own_http = http is None
        self.rest = SsiRestClient(cfg, self._http)
        self.normalizer = SsiNormalizer(cfg, calendar)
        self._sr: SignalRClient | None = None
        self._symbols: list[str] = []
        self._closed = False
        self._streaming = False

    def channel_string(self) -> str:
        if self.cfg.subscribe_all or not self._symbols:
            return f"{self.cfg.channel}:ALL"
        return f"{self.cfg.channel}:{'-'.join(self._symbols)}"

    async def connect(self) -> None:
        await self._drop_stream()
        token = await self.rest.token()
        sr = SignalRClient(self.cfg.stream_url, token, self._http)
        try:
            await sr.connect()
        except SignalRAuthError:
            self.rest.invalidate()  # next attempt fetches a fresh token
            raise
        self._sr = sr
        self.status.connected = True
        self.status.detail = f"SSI FastConnect Data · {self.cfg.channel}"

    async def subscribe(self, symbols: Sequence[str], channels: Sequence[str]) -> None:
        self._symbols = sorted(set(symbols))
        if self._sr is not None and self._streaming:  # universe changed mid-stream
            await self._sr.invoke("SwitchChannels", self.channel_string())

    def _idle_limit(self) -> float | None:
        s = self.calendar.session_at(datetime.now(UTC))
        return self.cfg.idle_reconnect_sec if (s.is_continuous or s.is_auction) else None

    async def events(self) -> AsyncIterator[MarketEvent]:
        if self._sr is None:
            raise ConnectionError("SSI stream not connected")
        await self._sr.invoke("SwitchChannels", self.channel_string())
        self._streaming = True
        it = self._sr.messages().__aiter__()
        while not self._closed:
            try:
                limit = self._idle_limit()
                method, args = await (asyncio.wait_for(anext(it), limit) if limit else anext(it))
            except StopAsyncIteration:
                break
            except TimeoutError:
                self.status.stats = self.normalizer.summary()
                raise ConnectionError("SSI stream silent during a session") from None
            if method == "Error":
                log.warning("SSI hub error: %s", str(args)[:300])
                self.normalizer.stats["hub_errors"] += 1
                continue
            if method != "Broadcast":
                continue
            received = datetime.now(UTC)
            for arg in args:
                if self.raw_sink is not None:
                    self.raw_sink(arg if isinstance(arg, str) else str(arg), received)
                decoded = decode_broadcast(arg)
                if decoded is None:
                    self.normalizer.stats["undecodable"] += 1
                    continue
                for ev in self.normalizer.events(decoded[0], decoded[1], received):
                    self.status.last_event_at = received
                    yield ev
            self.status.stats = self.normalizer.summary()
        self.status.connected = False
        if not self._closed:
            raise ConnectionError("SSI stream closed by server")

    async def _drop_stream(self) -> None:
        self._streaming = False
        if self._sr is not None:
            with contextlib.suppress(Exception):
                await self._sr.close()
            self._sr = None
        self.status.connected = False

    async def close(self) -> None:
        self._closed = True
        await self._drop_stream()
        if self._own_http:
            await self._http.aclose()
