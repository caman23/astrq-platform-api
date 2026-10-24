"""DNSE LightSpeed market-data adapter.

Doc-shaped fixtures follow the official `tick_extra` sample (developers.dnse.com.vn,
market-data/connect) with a stock symbol; SDK-shaped ones follow the `dnse` 0.5.0
PyPI models. Neither is captured traffic — replace with data/raw/dnse/ samples later.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from astrq.core.config import DnseConfig, SessionsConfig, Settings
from astrq.core.models import ExpectedPrice, Session, Side, SideSource, Tick
from astrq.core.sessions import SessionCalendar
from astrq.providers.dnse import DnseProvider
from astrq.providers.dnse.parse import DnseNormalizer
from astrq.providers.dnse.provider import DnseAuthError, build_auth_message

CAL = SessionCalendar(SessionsConfig())
NOW = datetime(2026, 10, 21, 3, 0, tzinfo=UTC)
T_1015 = int(datetime(2026, 10, 21, 3, 15, 30, tzinfo=UTC).timestamp())  # 10:15:30 VN


def doc_tick(**over: Any) -> dict[str, Any]:
    m: dict[str, Any] = {
        "marketId": "STO",
        "boardId": "G1",
        "isin": "VN000000FPT1",
        "symbol": "FPT",
        "matchPrice": 124.0,
        "matchQtty": 5000.0,
        "side": "BUY",
        "avgPrice": 123.8,
        "totalVolumeTraded": 800000,
        "grossTradeAmount": 99.0,
        "highestPrice": 125.0,
        "lowestPrice": 122.5,
        "openPrice": 123.0,
        "tradingSessionId": "40",
        "time": {"Seconds": T_1015, "Nanos": 72000000},
    }
    m.update(over)
    return m


def norm(**cfg: Any) -> DnseNormalizer:
    return DnseNormalizer(DnseConfig(**cfg), CAL)


def test_doc_shaped_tick_extra() -> None:
    n = norm()
    [t] = n.events(doc_tick(), NOW)
    assert isinstance(t, Tick)
    assert (t.symbol, t.price, t.volume) == ("FPT", 124_000.0, 5000)  # auto: thousand -> VND
    assert (t.side, t.side_source, t.is_negotiated) == (Side.BUY, SideSource.PROVIDER, False)
    assert t.ts_exchange == datetime(2026, 10, 21, 3, 15, 30, 72000, tzinfo=UTC)
    assert t.raw_ref == "G1:tv:800000"
    assert n.sessions_seen == {"40": 1} and n.stats["price_scaled_x1000"] == 1


def test_sdk_shaped_trade_and_units() -> None:
    n = norm(price_unit="vnd")
    msg = {
        "T": "te",
        "symbol": "vcb",
        "price": 61500,
        "volume": 300,
        "side": "S",
        "timestamp": T_1015 * 1000,
        "total_volume": 1000,
    }
    [t] = n.events(msg, NOW)
    assert (t.symbol, t.price, t.side) == ("VCB", 61500, Side.SELL)
    assert t.ts_exchange == datetime(2026, 10, 21, 3, 15, 30, tzinfo=UTC)
    assert n.shapes_seen == {"trade:sdk": 1}


def test_putthrough_board_and_repeat_drop() -> None:
    n = norm()
    [pt] = n.events(doc_tick(boardId="T1", side="", matchQtty=500000.0, totalVolumeTraded=5e5), NOW)
    assert pt.is_negotiated and pt.side is Side.UNKNOWN
    assert len(n.events(doc_tick(), NOW)) == 1
    assert n.events(doc_tick(), NOW) == []  # same cumulative volume on G1 -> repeat
    assert n.stats["repeat_dropped"] == 1


def test_session_map_and_expected_price() -> None:
    [t] = norm(session_map={"40": Session.CONTINUOUS_AM}).events(doc_tick(), NOW)
    assert t.session is Session.CONTINUOUS_AM
    ato = int(datetime(2026, 10, 21, 2, 10, tzinfo=UTC).timestamp())  # 09:10 VN
    exp = {
        "symbol": "FPT",
        "boardId": "G1",
        "expectedTradePrice": 124.5,
        "expectedTradeQuantity": 20000,
        "time": {"Seconds": ato, "Nanos": 0},
    }
    [e] = norm().events(exp, NOW)
    assert isinstance(e, ExpectedPrice) and e.session is Session.ATO and e.price == 124_500
    exp["time"] = {"Seconds": T_1015, "Nanos": 0}
    assert norm().events(exp, NOW) == []  # outside auction: ignored


def test_auth_signature_matches_docs() -> None:
    msg = build_auth_message("key1", "secret1", now=1_700_000_000.123456)
    assert msg["timestamp"] == 1_700_000_000 and msg["nonce"] == "1700000000123456"
    expected = hmac.new(b"secret1", b"key1:1700000000:1700000000123456", hashlib.sha256)
    assert msg["signature"] == expected.hexdigest()
    assert "secret1" not in json.dumps(msg)


async def _run_fake(server_script: Any, cfg: DnseConfig) -> tuple[list[Any], DnseProvider]:
    async with serve(server_script, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        prov = DnseProvider(
            cfg.model_copy(update={"ws_url": f"ws://127.0.0.1:{port}/v1/stream"}), CAL
        )
        got: list[Any] = []
        try:
            await prov.connect()
            await prov.subscribe(["fpt", "VCB"], [])
            async for ev in prov.events():
                got.append(ev)
        finally:
            await prov.close()
    return got, prov


async def test_end_to_end_against_fake_dnse_server() -> None:
    seen: dict[str, Any] = {}

    async def server(ws: ServerConnection) -> None:
        seen["path"] = ws.request.path if ws.request else None
        await ws.send(json.dumps({"action": "welcome", "session_id": "s1"}))
        auth = json.loads(await ws.recv())
        payload = f"{auth['api_key']}:{auth['timestamp']}:{auth['nonce']}".encode()
        seen["sig_ok"] = auth["signature"] == hmac.new(b"sek", payload, hashlib.sha256).hexdigest()
        await ws.send(
            json.dumps({"action": "auth_success", "rate_limit": {"subscriptions_max": 3}})
        )
        seen["sub"] = [json.loads(await ws.recv()), json.loads(await ws.recv())]
        await ws.send(json.dumps({"action": "subscribed"}))
        await ws.send(json.dumps({"action": "ping", "timestamp": 123}))
        seen["pong"] = json.loads(await ws.recv())
        await ws.send(json.dumps(doc_tick()))
        await ws.send(json.dumps(doc_tick(side="SELL", totalVolumeTraded=806000, matchQtty=6000.0)))
        await ws.send(json.dumps({"action": "connection_expired", "code": "MAX_DURATION_EXCEEDED"}))
        await ws.wait_closed()

    cfg = DnseConfig(api_key="k", api_secret="sek")
    with pytest.raises(ConnectionError, match="expired"):
        await _run_fake(server, cfg)
    assert seen["path"] == "/v1/stream?encoding=json"
    assert seen["sig_ok"]
    # server budget 3 streams (channel x symbol): tick_extra first, expected_price gets the rest
    assert seen["sub"] == [
        {
            "action": "subscribe",
            "channels": [{"name": "tick_extra.G1.json", "symbols": ["FPT", "VCB"]}],
        },
        {
            "action": "subscribe",
            "channels": [{"name": "expected_price.G1.json", "symbols": ["FPT"]}],
        },
    ]
    assert seen["pong"] == {"action": "pong", "timestamp": 123}


async def test_events_yielded_and_stats() -> None:
    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"action": "welcome"}))
        await ws.recv()
        await ws.send(json.dumps({"action": "auth_success"}))
        await ws.recv()
        await ws.send(json.dumps(doc_tick()))
        await ws.send(json.dumps(doc_tick()))  # repeat
        await ws.send(json.dumps(doc_tick(side="SELL", totalVolumeTraded=806000, matchQtty=6000.0)))
        await ws.close()

    got: list[Any] = []
    async with serve(server, "127.0.0.1", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        prov = DnseProvider(
            DnseConfig(api_key="k", api_secret="s", ws_url=f"ws://127.0.0.1:{port}"), CAL
        )
        await prov.connect()
        await prov.subscribe(["FPT"], [])
        with pytest.raises(ConnectionError):
            async for ev in prov.events():
                got.append(ev)
        await prov.close()
    assert [(t.side.value, t.volume) for t in got] == [("buy", 5000), ("sell", 6000)]
    assert prov.status.stats["counters"]["repeat_dropped"] == 1
    assert prov.status.stats["sides_seen"] == {"BUY": 1, "SELL": 1}  # repeats not counted


async def test_auth_failed_raises_auth_error() -> None:
    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"action": "welcome"}))
        await ws.recv()
        await ws.send(json.dumps({"action": "error", "code": "AUTH_FAILED", "message": "bad sig"}))
        await ws.close()

    async with serve(server, "127.0.0.1", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        prov = DnseProvider(
            DnseConfig(api_key="k", api_secret="topsecret", ws_url=f"ws://127.0.0.1:{port}"), CAL
        )
        with pytest.raises(DnseAuthError) as e:
            await prov.connect()
        await prov.close()
    assert "bad sig" in str(e.value) and "topsecret" not in str(e.value)


def test_settings_require_dnse_credentials() -> None:
    with pytest.raises(ValueError, match="DNSE__API_KEY"):
        Settings(provider={"kind": "dnse"}, context={"kind": "recorded"})
    s = Settings(
        provider={"kind": "dnse", "dnse": {"api_key": "k", "api_secret": "hidden-123"}},
        context={"kind": "recorded"},
    )
    assert "hidden-123" not in repr(s)
