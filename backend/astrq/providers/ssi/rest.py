"""SSI FastConnect Data REST: access token + index components.

Verified against the official guide (api-specs) and the official `ssi-fc-data`
2.2.2 client: POST api/v2/Market/AccessToken with {"consumerID","consumerSecret"};
response {"status": 200, "message": ..., "data": {"accessToken": <JWT>}}.
The token is never logged.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import httpx

from astrq.core.config import SsiConfig

TOKEN_PATH = "api/v2/Market/AccessToken"
INDEX_COMPONENTS_PATH = "api/v2/Market/IndexComponents"
REFRESH_MARGIN_SEC = 3600  # same margin as the official client


class SsiAuthError(Exception):
    """Credentials rejected or token request failed (message never contains secrets)."""


def _jwt_exp(token: str) -> float | None:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        return float(exp) if exp is not None else None
    except (IndexError, ValueError, json.JSONDecodeError):
        return None


class SsiRestClient:
    def __init__(self, cfg: SsiConfig, http: httpx.AsyncClient) -> None:
        self.cfg = cfg
        self.http = http
        self._token: str | None = None
        self._expires_at = 0.0

    def _url(self, path: str) -> str:
        return self.cfg.api_url.rstrip("/") + "/" + path

    async def token(self) -> str:
        if self._token and time.time() < self._expires_at - REFRESH_MARGIN_SEC:
            return self._token
        if not self.cfg.consumer_id or self.cfg.consumer_secret is None:
            raise SsiAuthError("missing SSI ConsumerID/ConsumerSecret")
        r = await self.http.post(
            self._url(TOKEN_PATH),
            json={
                "consumerID": self.cfg.consumer_id,
                "consumerSecret": self.cfg.consumer_secret.get_secret_value(),
            },
        )
        try:
            body: dict[str, Any] = r.json()
        except ValueError as exc:
            raise SsiAuthError(f"token endpoint HTTP {r.status_code}: non-JSON body") from exc
        data = body.get("data") or {}
        token = data.get("accessToken") if isinstance(data, dict) else None
        if r.status_code != 200 or str(body.get("status")) != "200" or not token:
            raise SsiAuthError(
                f"token request failed (HTTP {r.status_code}, status={body.get('status')}): "
                f"{body.get('message')}"
            )
        self._token = str(token)
        # unknown expiry -> assume 8h so we refresh periodically anyway
        self._expires_at = _jwt_exp(self._token) or (time.time() + 8 * 3600)
        return self._token

    def invalidate(self) -> None:
        self._token = None

    async def index_components(self, index_code: str) -> tuple[str | None, list[str]]:
        """Return (exchange, symbols) of an index, e.g. VN100."""
        token = await self.token()
        r = await self.http.get(
            self._url(INDEX_COMPONENTS_PATH),
            params={"indexCode": index_code, "pageIndex": 1, "pageSize": 1000},
            headers={"Authorization": f"Bearer {token}"},
        )
        r.raise_for_status()
        body = r.json()
        exchange: str | None = None
        symbols: list[str] = []
        for item in body.get("data") or []:
            exchange = item.get("Exchange") or exchange
            for comp in item.get("IndexComponent") or []:
                sym = comp.get("StockSymbol")
                if sym:
                    symbols.append(str(sym).upper())
        return exchange, symbols
