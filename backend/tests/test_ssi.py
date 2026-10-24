"""SSI FastConnect Data adapter.

Fixtures are CONSTRUCTED from the field list in SSI's official "Streaming data"
guide (X-TRADE / X) — not captured traffic. Replace/extend them with real
captures from data/raw/ssi/ after the first live session.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from websockets.asyncio.server import ServerConnection, serve

from astrq.core.config import SessionsConfig, SsiConfig
from astrq.core.models import ExpectedPrice, Quote, Session, Side, SideSource, Tick
from astrq.core.sessions import SessionCalendar
from astrq.providers.ssi import SsiProvider
from astrq.providers.ssi.parse import SsiNormalizer, decode_broadcast
from astrq.providers.ssi.rest import SsiAuthError, SsiRestClient

CAL = SessionCalendar(SessionsConfig())
NOW = datetime(2026, 10, 21, 3, 0, tzinfo=UTC)


def x_trade(**over: Any) -> str:
    content = {
        "RType": "X-TRADE",
        "TradingDate": "21/10/2026",
        "Time": "10:15:30",
        "Isin": "VN000000FPT1",
        "Symbol": "FPT",
        "Ceiling": 132000.0,
        "Floor": 115000.0,
        "RefPrice": 123500.0,
        "LastPrice": 124000.0,
        "LastVol": 5000.0,
        "TotalVal": 9.9e10,
        "TotalVol": 800000.0,
        "EstMatchedPrice": 0.0,
        "MarketId": "HOSE",
        "Exchange": "HOSE",
        "TradingSession": "LO",
        "TradingStatus": "N",
        "Side": "BU",
    }
    content.update(over)
    # documented shape: Content is a JSON *string*; docs samples even carry trailing commas
    return json.dumps({"DataType": "X-TRADE", "Content": json.dumps(content)[:-1] + ",}"})


def norm(cfg: SsiConfig | None = None) -> SsiNormalizer:
    return SsiNormalizer(cfg or SsiConfig(), CAL)


def ticks(n: SsiNormalizer, raw: str) -> list[Any]:
    decoded = decode_broadcast(raw)
    assert decoded is not None
    return n.events(decoded[0], decoded[1], NOW)


def test_x_trade_to_tick() -> None:
    [t] = ticks(norm(), x_trade())
    assert isinstance(t, Tick)
    assert (t.symbol, t.price, t.volume, t.value) == ("FPT", 124000.0, 5000, 124000.0 * 5000)
    assert (t.side, t.side_source) == (Side.BUY, SideSource.PROVIDER)
    # 10:15:30 Vietnam time == 03:15:30 UTC
    assert t.ts_exchange == datetime(2026, 10, 21, 3, 15, 30, tzinfo=UTC)
    assert t.session is Session.CLOSED  # unmapped code -> calendar decides in ingest
    assert t.raw_ref == "tv:800000"


def test_side_codes_and_unknown() -> None:
    n = norm()
    [s] = ticks(n, x_trade(Side="SD"))
    [u] = ticks(n, x_trade(Side="Unknow", TotalVol=810000.0))
    assert s.side is Side.SELL
    assert (u.side, u.side_source) == (Side.UNKNOWN, SideSource.UNKNOWN)


def test_total_volume_drops_repeats_and_counts_misses() -> None:
    n = norm()
    assert len(ticks(n, x_trade(TotalVol=800000.0))) == 1
    assert ticks(n, x_trade(TotalVol=800000.0)) == []  # snapshot repeat
    assert len(ticks(n, x_trade(TotalVol=815000.0))) == 1  # 10k missing (5k LastVol)
    assert n.stats["repeat_dropped"] == 1
    assert n.stats["missed_volume"] == 10000


def test_session_map_and_expected_price_in_auction() -> None:
    cfg = SsiConfig(session_map={"LO": Session.CONTINUOUS_AM})
    [t] = ticks(norm(cfg), x_trade())
    assert t.session is Session.CONTINUOUS_AM
    n = norm()
    out = ticks(
        n, x_trade(Time="09:10:00", LastVol=0.0, EstMatchedPrice=124500.0, TradingSession="ATO")
    )
    assert len(out) == 1 and isinstance(out[0], ExpectedPrice) and out[0].session is Session.ATO
    assert n.sessions_seen["ATO"] == 1


def test_units_sanity_counter() -> None:
    n = norm()
    ticks(n, x_trade(LastPrice=124.0))
    assert n.stats["price_looks_like_thousands"] == 1
    [t] = ticks(norm(SsiConfig(price_multiplier=1000)), x_trade(LastPrice=124.0))
    assert t.price == 124000.0


def test_x_snapshot_gives_quote_and_trade() -> None:
    content = json.loads(json.loads(x_trade())["Content"].replace(",}", "}"))
    content.update({"BidPrice1": 123900.0, "AskPrice1": 124000.0, "BidVol1": 1000, "AskVol1": 2000})
    out = norm().events("X", content, NOW)
    assert [type(e) for e in out] == [Quote, Tick]
    assert out[0].best_bid == 123900.0 and out[0].ask_size == 2000


# ------------------------------------------------------------------ REST


def _jwt(exp: float) -> str:
    def b64(d: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip("=")

    return f"{b64({'alg': 'none'})}.{b64({'exp': exp})}.sig"


def _rest_transport(calls: list[str], token: str, ok: bool = True) -> httpx.MockTransport:
    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        if req.url.path.endswith("/AccessToken"):
            body = json.loads(req.content)
            assert set(body) == {"consumerID", "consumerSecret"}
            if not ok:
                return httpx.Response(200, json={"status": 400, "message": "Invalid", "data": None})
            return httpx.Response(
                200, json={"status": 200, "message": "Success", "data": {"accessToken": token}}
            )
        if req.url.path.endswith("/IndexComponents"):
            assert req.headers["Authorization"] == f"Bearer {token}"
            assert req.url.params["indexCode"] == "VN100"
            return httpx.Response(
                200,
                json={
                    "status": 200,
                    "data": [
                        {
                            "IndexCode": "VN100",
                            "Exchange": "HOSE",
                            "TotalSymbolNo": "2",
                            "IndexComponent": [
                                {"Isin": "x", "StockSymbol": "FPT"},
                                {"Isin": "y", "StockSymbol": "vcb"},
                            ],
                        }
                    ],
                },
            )
        if req.url.path.endswith("/negotiate"):
            assert req.headers["Authorization"] == f"Bearer {token}"
            return httpx.Response(200, json={"ConnectionToken": "ct-1", "ProtocolVersion": "1.5"})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def _cfg(**over: Any) -> SsiConfig:
    base: dict[str, Any] = {"consumer_id": "id", "consumer_secret": "s3cret"}
    base.update(over)
    return SsiConfig(**base)


async def test_token_cached_and_index_components() -> None:
    calls: list[str] = []
    token = _jwt(time.time() + 8 * 3600)
    async with httpx.AsyncClient(transport=_rest_transport(calls, token)) as http:
        rest = SsiRestClient(_cfg(), http)
        assert await rest.token() == token
        assert await rest.token() == token
        exch, syms = await rest.index_components("VN100")
    assert calls.count("/api/v2/Market/AccessToken") == 1  # cached
    assert (exch, syms) == ("HOSE", ["FPT", "VCB"])


async def test_token_refreshed_near_expiry_and_errors_hide_secret() -> None:
    calls: list[str] = []
    async with httpx.AsyncClient(transport=_rest_transport(calls, _jwt(time.time() + 600))) as http:
        rest = SsiRestClient(_cfg(), http)
        await rest.token()
        await rest.token()  # < 1h left -> refresh
    assert calls.count("/api/v2/Market/AccessToken") == 2
    async with httpx.AsyncClient(transport=_rest_transport([], "t", ok=False)) as http:
        with pytest.raises(SsiAuthError) as e:
            await SsiRestClient(_cfg(), http).token()
    assert "s3cret" not in str(e.value) and "Invalid" in str(e.value)


# ------------------------------------------------------------------ streaming end-to-end


async def test_provider_end_to_end_against_fake_signalr_hub() -> None:
    token = _jwt(time.time() + 8 * 3600)
    seen: dict[str, Any] = {}

    async def hub(ws: ServerConnection) -> None:
        req = ws.request
        assert req is not None
        seen["path"] = req.path
        seen["auth"] = req.headers.get("Authorization")
        await ws.send(json.dumps({"C": "c1", "S": 1, "M": []}))  # init frame
        invoke = json.loads(await ws.recv())
        seen["invoke"] = invoke
        await ws.send("{}")  # keep-alive
        frames = [
            x_trade(TotalVol=800000.0),
            x_trade(TotalVol=800000.0),
            x_trade(TotalVol=806000.0, Side="SD", LastVol=6000.0),
        ]
        await ws.send(
            json.dumps(
                {
                    "C": "c2",
                    "M": [{"H": "FcMarketDataV2Hub", "M": "Broadcast", "A": [f]} for f in frames],
                }
            )
        )
        await ws.send(
            json.dumps({"M": [{"H": "OtherHub", "M": "Broadcast", "A": [x_trade(TotalVol=9e6)]}]})
        )
        await ws.close()

    async with serve(hub, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        raw: list[str] = []
        async with httpx.AsyncClient(transport=_rest_transport([], token)) as http:
            prov = SsiProvider(
                _cfg(stream_url=f"http://127.0.0.1:{port}/", api_url="http://api.test/"),
                CAL,
                raw_sink=lambda line, _ts: raw.append(line),
                http=http,
            )
            await prov.connect()
            await prov.subscribe(["VCB", "FPT"], [])
            got = []
            with pytest.raises(ConnectionError):  # server closed -> platform reconnects
                async for ev in prov.events():
                    got.append(ev)
            await prov.close()

    assert seen["path"].startswith(
        "/v2.0/signalr/connect?transport=webSockets&connectionToken=ct-1"
    )
    assert seen["auth"] == f"Bearer {token}"
    assert seen["invoke"] == {
        "H": "FcMarketDataV2Hub",
        "M": "SwitchChannels",
        "A": ["X-TRADE:FPT-VCB"],
        "I": 0,
    }
    assert [(t.side.value, t.volume) for t in got] == [("buy", 5000), ("sell", 6000)]
    assert len(raw) == 3  # raw messages of our hub recorded before normalization
    assert prov.status.stats["counters"]["repeat_dropped"] == 1


async def test_negotiate_401_invalidates_token() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/AccessToken"):
            return httpx.Response(
                200, json={"status": 200, "data": {"accessToken": _jwt(time.time() + 9e3)}}
            )
        return httpx.Response(401)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        prov = SsiProvider(_cfg(), CAL, http=http)
        with pytest.raises(Exception, match="unauthorized"):
            await prov.connect()
        assert prov.rest._token is None
        await prov.close()


def test_settings_require_credentials_and_real_baseline() -> None:
    from astrq.core.config import Settings

    creds = {"consumer_id": "a", "consumer_secret": "topsecret-123"}
    with pytest.raises(ValueError, match="CONSUMER_ID"):
        Settings(provider={"kind": "ssi"})
    with pytest.raises(ValueError, match="recorded"):
        Settings(provider={"kind": "ssi", "ssi": creds})
    s = Settings(provider={"kind": "ssi", "ssi": creds}, context={"kind": "recorded"})
    assert "topsecret-123" not in repr(s)
