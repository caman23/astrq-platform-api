"""Pure normalization of DNSE market-data WebSocket messages into canonical events.

Field names come from the official docs (developers.dnse.com.vn, market-data/connect):
tick / tick_extra: symbol, boardId, matchPrice, matchQtty, side (tick_extra only),
totalVolumeTraded, tradingSessionId, time{Seconds,Nanos};
expected_price: symbol, boardId, expectedTradePrice, expectedTradeQuantity, time.

The `dnse` 0.5.0 PyPI SDK uses different names (T="t"/"te"/"e", price, volume,
timestamp, total_volume). Until real traffic is captured, both shapes are accepted.

Still to verify on live data (docs/provider-notes.md): price unit (doc samples look like
thousand VND), the full set of `side` values, tradingSessionId codes, the data envelope.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any

from astrq.core.config import DnseConfig
from astrq.core.models import ExpectedPrice, MarketEvent, Session, Side, SideSource, Tick
from astrq.core.sessions import SessionCalendar
from astrq.providers.cumvol import CumulativeVolumeTracker

PROVIDER = "dnse"
_SIDE = {"BUY": Side.BUY, "B": Side.BUY, "SELL": Side.SELL, "S": Side.SELL}
_TRADE_TYPES = {"t", "te"}
_EXPECTED_TYPES = {"e"}


def _first(p: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in p and p[k] is not None:
            return p[k]
    return None


def _num(v: Any) -> float | None:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


class DnseNormalizer:
    def __init__(self, cfg: DnseConfig, calendar: SessionCalendar) -> None:
        self.cfg = cfg
        self.calendar = calendar
        self.stats: Counter[str] = Counter()
        self.sessions_seen: Counter[str] = Counter()
        self.sides_seen: Counter[str] = Counter()
        self.shapes_seen: Counter[str] = Counter()
        self._cumvol = CumulativeVolumeTracker(self.stats)

    # ------------------------------------------------------------ helpers

    def _ts(self, p: dict[str, Any], received: datetime) -> datetime:
        t = _first(p, "time", "timestamp")
        if isinstance(t, dict):
            secs = _num(_first(t, "Seconds", "seconds"))
            nanos = _num(_first(t, "Nanos", "nanos")) or 0.0
            if secs is not None:
                return datetime.fromtimestamp(secs + nanos / 1e9, UTC)
        n = _num(t)
        if n is not None and n > 0:
            return datetime.fromtimestamp(n / 1000 if n > 1e12 else n, UTC)
        self.stats["no_timestamp"] += 1
        return received

    def _price(self, raw: float) -> float:
        unit = self.cfg.price_unit
        if unit == "thousand" or (unit == "auto" and raw < 1000):
            if unit == "auto":
                self.stats["price_scaled_x1000"] += 1
            return raw * 1000
        return raw

    def _session(self, p: dict[str, Any]) -> Session | None:
        code = _first(p, "tradingSessionId", "trading_session_id")
        if code is None:
            return None
        self.sessions_seen[str(code)] += 1
        return self.cfg.session_map.get(str(code))

    # ------------------------------------------------------------ entry point

    def events(self, msg: dict[str, Any], received: datetime) -> list[MarketEvent]:
        p = msg["data"] if isinstance(msg.get("data"), dict) else msg
        t_code = str(_first(msg, "T") or _first(p, "T") or "")
        symbol = str(_first(p, "symbol", "s") or "").upper()
        if not symbol:
            self.stats["no_symbol"] += 1
            return []
        if "expectedTradePrice" in p or t_code in _EXPECTED_TYPES:
            self.shapes_seen["expected:" + ("doc" if "expectedTradePrice" in p else "sdk")] += 1
            return self._expected(symbol, p, received)
        if "matchPrice" in p or t_code in _TRADE_TYPES:
            self.shapes_seen["trade:" + ("doc" if "matchPrice" in p else "sdk")] += 1
            return self._trade(symbol, p, received)
        self.stats["unhandled_message"] += 1
        return []

    def _expected(self, symbol: str, p: dict[str, Any], received: datetime) -> list[MarketEvent]:
        price = _num(_first(p, "expectedTradePrice", "price"))
        if not price or price <= 0:
            return []
        ts = self._ts(p, received)
        session = self._session(p) or self.calendar.session_at(ts)
        if not session.is_auction:
            self.stats["expected_outside_auction"] += 1
            return []
        qty = _num(_first(p, "expectedTradeQuantity", "volume"))
        return [
            ExpectedPrice(
                provider=PROVIDER,
                symbol=symbol,
                ts_exchange=ts,
                ts_received=received,
                session=session,
                price=self._price(price),
                volume=int(qty * self.cfg.volume_multiplier) if qty else None,
            )
        ]

    def _trade(self, symbol: str, p: dict[str, Any], received: datetime) -> list[MarketEvent]:
        price = _num(_first(p, "matchPrice", "price"))
        vol = _num(_first(p, "matchQtty", "volume"))
        if not price or not vol or price <= 0 or vol <= 0:
            self.stats["no_trade_fields"] += 1
            return []
        board = str(_first(p, "boardId", "board_id") or "G1")
        total = _num(_first(p, "totalVolumeTraded", "total_volume"))
        if not self._cumvol.accept((symbol, board), total, vol):
            return []
        negotiated = board in self.cfg.putthrough_boards
        raw_side = str(_first(p, "side") or "").upper()
        if raw_side:
            self.sides_seen[raw_side] += 1
        side = Side.UNKNOWN if negotiated else _SIDE.get(raw_side, Side.UNKNOWN)
        px = self._price(price)
        qty = int(vol * self.cfg.volume_multiplier)
        session = self._session(p)
        self.stats["ticks"] += 1
        self.stats[f"side_{side.value}"] += 1
        return [
            Tick(
                provider=PROVIDER,
                symbol=symbol,
                ts_exchange=self._ts(p, received),
                ts_received=received,
                price=px,
                volume=qty,
                value=px * qty,
                side=side,
                side_source=SideSource.PROVIDER if side is not Side.UNKNOWN else SideSource.UNKNOWN,
                session=session or Session.CLOSED,  # CLOSED -> ingest uses the calendar
                is_negotiated=negotiated,
                raw_ref=f"{board}:tv:{int(total)}" if total is not None else None,
            )
        ]

    def summary(self) -> dict[str, Any]:
        return {
            "counters": dict(self.stats),
            "sessions_seen": dict(self.sessions_seen),
            "sides_seen": dict(self.sides_seen),
            "message_shapes": dict(self.shapes_seen),
        }
