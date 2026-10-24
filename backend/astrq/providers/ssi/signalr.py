"""Minimal asyncio client for classic ASP.NET SignalR (protocol 1.5), as used by
SSI FastConnect Data streaming.

Protocol steps mirror the official `ssi-fc-data` 2.2.2 client exactly:
  1. POST {base}/v2.0/signalr/negotiate?connectionData=[{"name": HUB}]&clientProtocol=1.5
     (Authorization: Bearer <token>) -> {"ConnectionToken", "ProtocolVersion", ...}
  2. WebSocket {wss base}/v2.0/signalr/connect?transport=webSockets&connectionToken=...
     &connectionData=...&clientProtocol=...
  3. Invoke: {"H": HUB, "M": "SwitchChannels", "A": ["X-TRADE:FPT-VCB"], "I": n}
  4. Server push: {"M": [{"H": HUB, "M": "Broadcast", "A": [<json string>]}]}; "{}" = keep-alive.
Unlike the official client, TLS certificates ARE verified and headers are never printed.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

import httpx
from websockets.asyncio.client import ClientConnection, connect

HUB = "FcMarketDataV2Hub"
SIGNALR_PATH = "v2.0/signalr"
PROTOCOL = "1.5"


class SignalRError(Exception):
    pass


class SignalRAuthError(SignalRError):
    pass


class SignalRClient:
    def __init__(self, base_url: str, token: str, http: httpx.AsyncClient, hub: str = HUB) -> None:
        self.url = base_url.rstrip("/") + "/" + SIGNALR_PATH
        self.hub = hub
        self.http = http
        self._auth = {"Authorization": f"Bearer {token}"}
        self._conn_data = json.dumps([{"name": hub}])
        self._ws: ClientConnection | None = None
        self._counter = -1

    async def connect(self) -> None:
        r = await self.http.post(
            f"{self.url}/negotiate",
            params={"connectionData": self._conn_data, "clientProtocol": PROTOCOL},
            headers=self._auth,
        )
        if r.status_code == 401:
            raise SignalRAuthError("negotiate unauthorized (token rejected or expired)")
        if r.status_code != 200:
            raise SignalRError(f"negotiate HTTP {r.status_code}")
        data = r.json()
        query = urlencode(
            {
                "transport": "webSockets",
                "connectionToken": data["ConnectionToken"],
                "connectionData": self._conn_data,
                "clientProtocol": data.get("ProtocolVersion", PROTOCOL),
            }
        )
        ws_base = self.url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        self._ws = await connect(
            f"{ws_base}/connect?{query}",
            additional_headers=self._auth,
            open_timeout=20,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        )

    async def invoke(self, method: str, *args: Any) -> None:
        if self._ws is None:
            raise SignalRError("not connected")
        self._counter += 1
        await self._ws.send(
            json.dumps({"H": self.hub, "M": method, "A": list(args), "I": self._counter})
        )

    async def messages(self) -> AsyncIterator[tuple[str, list[Any]]]:
        """Yield (method, args) for hub calls addressed to our hub. Ends when the socket closes."""
        if self._ws is None:
            raise SignalRError("not connected")
        async for raw in self._ws:
            if not raw:
                continue
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                continue
            if msg.get("E"):
                raise SignalRError(f"server error: {msg['E']}")
            for call in msg.get("M") or []:
                if str(call.get("H", "")).lower() == self.hub.lower():
                    yield str(call.get("M", "")), list(call.get("A") or [])

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
