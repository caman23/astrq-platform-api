"""Replay scenarios (section 11 of the spec). Each returns a list of events.

Fixed baseline: median trade value 20M (mid tier) => single large >= 500M
(absolute) or >= 200M (relative), child candidates >= 80M, group opens at 500M.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from astrq.core.models import MarketEvent, Session, Side
from tests.helpers import reset_seq, tick, ts


def _noise(sym: str, start: str, n: int, every: int = 7) -> list[MarketEvent]:
    t0 = ts(start)
    out: list[MarketEvent] = []
    for i in range(n):
        t = t0 + timedelta(seconds=i * every)
        side = Side.BUY if i % 2 else Side.SELL
        out.append(
            tick(sym, t.strftime("%H:%M:%S"), 20_000 + (50 if side is Side.BUY else 0), 1000, side)
        )
    return out


def _children(
    sym: str, start: str, n: int, every: int, value: float, side: Side, step_every: int = 3
) -> list[MarketEvent]:
    t0 = ts(start)
    out: list[MarketEvent] = []
    price = 20_000.0
    for i in range(n):
        if i and i % step_every == 0:
            price += 50 if side is Side.BUY else -50
        t = t0 + timedelta(seconds=i * every)
        out.append(tick(sym, t.strftime("%H:%M:%S"), price, int(value / price) // 100 * 100, side))
    return out


def _sorted(events: list[MarketEvent]) -> list[MarketEvent]:
    return sorted(events, key=lambda e: e.ts_exchange)


def single_block() -> list[MarketEvent]:
    reset_seq()
    block = tick("AAA", "10:01:00", 20_050, 100_000, Side.BUY)  # 2.005B single print
    return _sorted([*_noise("AAA", "10:00:00", 20), block])


def split_parent() -> list[MarketEvent]:
    reset_seq()
    return _sorted(
        _noise("AAA", "10:00:00", 30) + _children("AAA", "10:00:10", 12, 5, 150e6, Side.BUY)
    )


def reversal() -> list[MarketEvent]:
    reset_seq()
    return _sorted(
        _children("AAA", "10:00:00", 6, 5, 150e6, Side.BUY)
        + _children("AAA", "10:00:32", 6, 5, 150e6, Side.SELL)
    )


def lunch_break() -> list[MarketEvent]:
    reset_seq()
    return _sorted(
        _children("AAA", "11:29:20", 6, 5, 150e6, Side.BUY)
        + _children("AAA", "13:00:05", 6, 5, 150e6, Side.BUY)
    )


def auctions() -> list[MarketEvent]:
    reset_seq()
    return [
        tick(
            "AAA",
            "09:14:59",
            20_000,
            150_000,
            Side.UNKNOWN,
            ms=999,
            session=Session.ATO,
            provider_side=False,
        ),  # 3B ATO match
        tick(
            "BBB",
            "09:14:59",
            20_000,
            5_000,
            Side.UNKNOWN,
            ms=999,
            session=Session.ATO,
            provider_side=False,
        ),  # 100M: not large
        tick(
            "AAA",
            "14:44:59",
            20_100,
            200_000,
            Side.UNKNOWN,
            ms=999,
            session=Session.ATC,
            provider_side=False,
        ),  # 4.02B ATC match
    ]


def putthrough_interleaved() -> list[MarketEvent]:
    reset_seq()
    kids = _children("AAA", "10:00:00", 8, 5, 150e6, Side.BUY)
    pt = tick(
        "AAA", "10:00:12", 19_500, 1_000_000, Side.UNKNOWN, negotiated=True, provider_side=False
    )
    return _sorted([*kids, pt])


def duplicates_and_late() -> list[MarketEvent]:
    reset_seq()
    kids = _children("AAA", "10:00:00", 8, 5, 150e6, Side.BUY)
    dup = kids[3]  # exact duplicate (same seq) -> dropped by ingest
    late = tick("AAA", "10:00:21", 20_050, 7_400, Side.BUY)  # arrives after 10:00:25 child
    events = [*kids[:6], dup, late, *kids[6:]]
    return events


def two_symbols_inferred_side() -> list[MarketEvent]:
    """No provider side flag: classification falls back to the tick rule."""
    reset_seq()
    out: list[MarketEvent] = []
    price = 20_000.0
    for i in range(8):
        price += 50
        out.append(
            tick("BBB", f"10:05:{i * 5:02d}", price, 7_500, Side.UNKNOWN, provider_side=False)
        )
    return out


SCENARIOS: dict[str, Callable[[], list[MarketEvent]]] = {
    "single_block": single_block,
    "split_parent": split_parent,
    "reversal": reversal,
    "lunch_break": lunch_break,
    "auctions": auctions,
    "putthrough_interleaved": putthrough_interleaved,
    "duplicates_and_late": duplicates_and_late,
    "inferred_side": two_symbols_inferred_side,
}
