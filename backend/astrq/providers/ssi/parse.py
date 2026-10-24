"""Pure normalization of SSI FastConnect Data messages into canonical events.

Message format (official guide, "Streaming data"): {"DataType": "X-TRADE", "Content": "<json>"}.
X-TRADE fields used: Symbol, TradingDate (DD/MM/YYYY), Time (HH:MM:SS), LastPrice,
LastVol, TotalVol, Side ("BU" buy-up | "SD" sell-down | other = unknown),
TradingSession, EstMatchedPrice. X adds BidPrice1/AskPrice1/BidVol1/AskVol1.
Field-name casing is inconsistent in the docs, so keys are matched case-insensitively.

Assumptions still to verify on real data (docs/provider-notes.md):
  * Time is Vietnam local time (the documented sample is during VN trading hours);
  * prices are VND (documented sample: Ceiling 24500) — a sanity counter flags
    prices that look like thousands;
  * TotalVol is cumulative matched volume, used to drop repeats and count misses;
  * put-through deals are not flagged in X-TRADE (is_negotiated stays False).
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from astrq.core.config import SsiConfig
from astrq.core.models import ExpectedPrice, MarketEvent, Quote, Session, Side, SideSource, Tick
from astrq.core.sessions import SessionCalendar
from astrq.providers.cumvol import CumulativeVolumeTracker

PROVIDER = "ssi"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
_SIDE = {"BU": Side.BUY, "SD": Side.SELL}
_TRAILING_COMMA = re.compile(r",\s*([}\]])")


def _loads(s: str) -> Any:
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return json.loads(_TRAILING_COMMA.sub(r"\1", s))  # documented samples have these


def decode_broadcast(arg: Any) -> tuple[str, dict[str, Any]] | None:
    """Broadcast argument -> (DataType, content dict)."""
    obj = _loads(arg) if isinstance(arg, str) else arg
    if not isinstance(obj, dict):
        return None
    dtype = obj.get("DataType") or obj.get("dataType")
    content = obj.get("Content", obj.get("content"))
    if isinstance(content, str):
        content = _loads(content)
    if not dtype or not isinstance(content, dict):
        return None
    return str(dtype), content


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class SsiNormalizer:
    def __init__(self, cfg: SsiConfig, calendar: SessionCalendar) -> None:
        self.cfg = cfg
        self.calendar = calendar
        self.stats: Counter[str] = Counter()
        self._cumvol = CumulativeVolumeTracker(self.stats)
        self.sessions_seen: Counter[str] = Counter()
        self.data_types_seen: Counter[str] = Counter()

    def _ts(self, c: dict[str, Any], received: datetime) -> datetime:
        d, t = c.get("tradingdate"), c.get("time")
        if d and t:
            try:
                local = datetime.strptime(f"{d} {t}", "%d/%m/%Y %H:%M:%S")
                return local.replace(tzinfo=VN_TZ).astimezone(UTC)
            except ValueError:
                self.stats["bad_timestamp"] += 1
        return received

    def events(self, dtype: str, content: dict[str, Any], received: datetime) -> list[MarketEvent]:
        self.data_types_seen[dtype] += 1
        c = {str(k).lower(): v for k, v in content.items()}
        symbol = str(c.get("symbol") or "").upper()
        if not symbol:
            self.stats["no_symbol"] += 1
            return []
        ts = self._ts(c, received)
        out: list[MarketEvent] = []
        if dtype in ("X-QUOTE", "X"):
            q = self._quote(symbol, c, ts, received)
            if q is not None:
                out.append(q)
        if dtype in ("X-TRADE", "X"):
            out.extend(self._trade(symbol, c, ts, received))
        return out

    def _quote(
        self, symbol: str, c: dict[str, Any], ts: datetime, received: datetime
    ) -> Quote | None:
        bid, ask = _num(c.get("bidprice1")), _num(c.get("askprice1"))
        if not bid and not ask:
            return None
        m = self.cfg.price_multiplier
        bv, av = _num(c.get("bidvol1")), _num(c.get("askvol1"))
        return Quote(
            provider=PROVIDER,
            symbol=symbol,
            ts_exchange=ts,
            ts_received=received,
            best_bid=bid * m if bid else None,
            best_ask=ask * m if ask else None,
            bid_size=int(bv * self.cfg.volume_multiplier) if bv else None,
            ask_size=int(av * self.cfg.volume_multiplier) if av else None,
        )

    def _trade(
        self, symbol: str, c: dict[str, Any], ts: datetime, received: datetime
    ) -> list[MarketEvent]:
        out: list[MarketEvent] = []
        code = str(c.get("tradingsession") or "")
        if code:
            self.sessions_seen[code] += 1
        session = self.cfg.session_map.get(code)
        cal_session = session or self.calendar.session_at(ts)

        est = _num(c.get("estmatchedprice"))
        if est and est > 0 and cal_session.is_auction:
            out.append(
                ExpectedPrice(
                    provider=PROVIDER,
                    symbol=symbol,
                    ts_exchange=ts,
                    ts_received=received,
                    session=cal_session,
                    price=est * self.cfg.price_multiplier,
                )
            )

        price, vol, total = (
            _num(c.get("lastprice")),
            _num(c.get("lastvol")),
            _num(c.get("totalvol")),
        )
        if not price or not vol or price <= 0 or vol <= 0:
            self.stats["no_trade_fields"] += 1
            return out
        if not self._cumvol.accept(symbol, total, vol):
            return out  # snapshot repeat / out-of-order

        px = price * self.cfg.price_multiplier
        if px < 500:
            self.stats["price_looks_like_thousands"] += 1
        qty = int(vol * self.cfg.volume_multiplier)
        side = _SIDE.get(str(c.get("side") or "").upper(), Side.UNKNOWN)
        self.stats["ticks"] += 1
        self.stats[f"side_{side.value}"] += 1
        out.append(
            Tick(
                provider=PROVIDER,
                symbol=symbol,
                ts_exchange=ts,
                ts_received=received,
                price=px,
                volume=qty,
                value=px * qty,
                side=side,
                side_source=SideSource.PROVIDER if side is not Side.UNKNOWN else SideSource.UNKNOWN,
                session=session or Session.CLOSED,  # CLOSED -> ingest uses the calendar
                is_negotiated=False,
                raw_ref=f"tv:{int(total)}" if total is not None else None,
            )
        )
        return out

    def summary(self) -> dict[str, Any]:
        return {
            "counters": dict(self.stats),
            "sessions_seen": dict(self.sessions_seen),
            "data_types_seen": dict(self.data_types_seen),
        }
