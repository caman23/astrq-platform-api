"""Test helpers: tiny universe, fixed baselines, tick builders, offline runner."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from astrq.core.config import Settings, ToolEntry
from astrq.core.context import Baseline, ContextService
from astrq.core.models import Instrument, MarketEvent, Quote, Session, Side, SideSource, Tick
from astrq.core.offline import OfflineResult, run_offline
from astrq.core.universe import Universe
from astrq.tools.large_order_radar import LargeOrderRadar
from astrq.tools.large_order_radar.config import RadarConfig

VN = ZoneInfo("Asia/Ho_Chi_Minh")
DAY = date(2026, 10, 21)
SYMBOLS = ["AAA", "BBB"]


def ts(hms: str, ms: int = 0) -> datetime:
    h, m, s = (int(x) for x in hms.split(":"))
    return datetime(DAY.year, DAY.month, DAY.day, h, m, s, ms * 1000, tzinfo=VN)


class FixedContext:
    """median trade value 20M VND, mid tier => group open threshold 500M."""

    name = "fixed"

    def __init__(self, median: float = 20e6) -> None:
        self.median = median

    async def baseline(self, symbol: str, trading_date: date) -> Baseline | None:
        return Baseline(
            symbol=symbol,
            trading_date=trading_date,
            median_trade_value=self.median,
            p95_trade_value=self.median * 7,
            avg_daily_value=50e9,
            liquidity_tier="mid",
            source="test",
        )


_seq: dict[str, int] = {}


def tick(
    sym: str,
    hms: str,
    price: float,
    volume: int,
    side: Side = Side.BUY,
    *,
    ms: int = 0,
    session: Session | None = None,
    negotiated: bool = False,
    seq: int | None = None,
    provider_side: bool = True,
) -> Tick:
    if seq is None:
        _seq[sym] = _seq.get(sym, 0) + 1
        seq = _seq[sym]
    t = ts(hms, ms)
    return Tick(
        provider="test",
        symbol=sym,
        ts_exchange=t,
        ts_received=t,
        price=price,
        volume=volume,
        value=price * volume,
        side=side if provider_side else Side.UNKNOWN,
        side_source=SideSource.PROVIDER if provider_side else SideSource.UNKNOWN,
        session=session or Session.CLOSED,  # CLOSED => ingest fills from the calendar
        is_negotiated=negotiated,
        seq=seq,
    )


def quote(sym: str, hms: str, bid: float, ask: float) -> Quote:
    t = ts(hms)
    return Quote(
        provider="test", symbol=sym, ts_exchange=t, ts_received=t, best_bid=bid, best_ask=ask
    )


def reset_seq() -> None:
    _seq.clear()


def settings() -> Settings:
    return Settings()


def universe() -> Universe:
    return Universe.from_instruments("test", [Instrument(symbol=s, sector="Test") for s in SYMBOLS])


def radar(cfg: RadarConfig | None = None) -> tuple[LargeOrderRadar, ToolEntry]:
    return LargeOrderRadar(cfg or RadarConfig()), ToolEntry(
        module="astrq.tools.large_order_radar:LargeOrderRadar"
    )


async def run(
    events: list[MarketEvent],
    cfg: RadarConfig | None = None,
    context: ContextService | None = None,
) -> OfflineResult:
    return await run_offline(
        events,
        settings(),
        universe(),
        context or ContextService(FixedContext()),
        tools={"large_order_radar": radar(cfg)},
    )


def project(result: OfflineResult) -> list[dict[str, object]]:
    """Compact, stable projection of signals for golden comparison."""
    out = []
    for s in result.signals:
        g = s.features.get("group", {})
        out.append(
            {
                "t": s.ts.astimezone(VN).strftime("%H:%M:%S"),
                "symbol": s.symbol,
                "kind": s.kind,
                "side": s.side.value,
                "n": g.get("n_trades"),
                "value": g.get("total_value"),
                "score": s.score,
                "severity": s.severity.value,
                "flags": s.quality_flags,
            }
        )
    return out
