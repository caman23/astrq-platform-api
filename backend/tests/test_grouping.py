from __future__ import annotations

import random
from datetime import timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from astrq.core.models import Session, Side, SideSource
from astrq.tools.large_order_radar.grouping import GroupBook, ParentGroup, TradeRec
from tests.helpers import ts

T0 = ts("10:00:00")


def rec(sec: float, value: float, side: Side = Side.BUY, price: float = 20_000) -> TradeRec:
    return TradeRec(
        ts=T0 + timedelta(seconds=sec),
        price=price,
        volume=int(value // price),
        value=value,
        side=side,
        side_source=SideSource.PROVIDER,
        confidence=1.0,
    )


def book() -> GroupBook:
    return GroupBook(gap_sec=30, band_ticks=3, update_step=0.5, keep_trades=40)


def test_lifecycle_open_update_close() -> None:
    b = book()
    actions: list[str] = []
    for i in range(10):  # 10 x 150M every 5s
        for a, _g in b.add("AAA", rec(i * 5, 150e6), Session.CONTINUOUS_AM, 50, 500e6, False):
            actions.append(a)
    # open at 600M (4th), update at >=900M (6th), >=1.35B (9th)
    assert actions == ["open", "update", "update"]
    closed = b.close_idle(T0 + timedelta(seconds=45 + 91), idle_sec=90)
    assert [a for a, _ in closed] == ["close"]
    assert closed[0][1].total_value == 1.5e9 and closed[0][1].n_trades == 10


def test_gap_and_price_band_break_groups() -> None:
    b = book()
    b.add("AAA", rec(0, 600e6), Session.CONTINUOUS_AM, 50, 500e6, False)
    ev = b.add("AAA", rec(31, 600e6), Session.CONTINUOUS_AM, 50, 500e6, False)  # gap > 30s
    assert [a for a, _ in ev] == ["close", "open"]
    ev = b.add("AAA", rec(35, 600e6, price=20_200), Session.CONTINUOUS_AM, 50, 500e6, False)
    assert [a for a, _ in ev] == ["close", "open"]  # 4 ticks away > 3-tick band


def test_session_change_breaks_group() -> None:
    b = book()
    b.add("AAA", rec(0, 600e6), Session.CONTINUOUS_AM, 50, 500e6, False)
    ev = b.add("AAA", rec(5, 600e6), Session.CONTINUOUS_PM, 50, 500e6, False)
    assert [a for a, _ in ev] == ["close", "open"]


def test_opposite_sides_are_independent() -> None:
    b = book()
    b.add("AAA", rec(0, 600e6, Side.BUY), Session.CONTINUOUS_AM, 50, 500e6, False)
    ev = b.add("AAA", rec(1, 600e6, Side.SELL), Session.CONTINUOUS_AM, 50, 500e6, False)
    assert [a for a, _ in ev] == ["open"]
    assert len(b.active) == 2


def test_snapshot_roundtrip() -> None:
    b = book()
    for i in range(5):
        b.add("AAA", rec(i * 3, 200e6), Session.CONTINUOUS_AM, 50, 500e6, False)
    data = b.dump()
    b2 = book()
    b2.load(data)
    g1, g2 = next(iter(b.active.values())), next(iter(b2.active.values()))
    assert g1.to_dict() == g2.to_dict()
    assert isinstance(g2, ParentGroup)


@settings(max_examples=150, deadline=None)
@given(
    values=st.lists(st.floats(50e6, 2e9), min_size=1, max_size=60),
    gaps=st.lists(st.floats(0, 40), min_size=60, max_size=60),
    sides=st.lists(st.sampled_from([Side.BUY, Side.SELL]), min_size=60, max_size=60),
    seed=st.integers(0, 10_000),
)
def test_value_is_conserved_under_reordering(
    values: list[float], gaps: list[float], sides: list[Side], seed: int
) -> None:
    """Every trade lands in exactly one group: sum(groups) == sum(trades), whatever
    the arrival order (late ticks included)."""
    t = 0.0
    trades = []
    for i, v in enumerate(values):
        t += gaps[i]
        trades.append(rec(t, v, sides[i]))
    # local shuffles: swap neighbours to simulate slightly out-of-order delivery
    rng = random.Random(seed)
    for i in range(len(trades) - 1):
        if rng.random() < 0.3:
            trades[i], trades[i + 1] = trades[i + 1], trades[i]

    b = GroupBook(gap_sec=30, band_ticks=3, update_step=0.5, keep_trades=10_000)
    finished: list[ParentGroup] = []  # closed (opened) + discarded (never opened)
    for tr in trades:
        before = b.active.get(("AAA", tr.side))
        b.add("AAA", tr, Session.CONTINUOUS_AM, 50, 500e6, False)
        if before is not None and b.active.get(("AAA", tr.side)) is not before:
            finished.append(before)
    groups = finished + list(b.active.values())
    assert abs(sum(g.total_value for g in groups) - sum(values)) < 1e-2
    assert sum(g.n_trades for g in groups) == len(values)
    for g in groups:
        assert abs(g.total_value - sum(x.value for x in g.trades)) < 1e-3
        assert (g.state != "pending") == (g.total_value >= 500e6)


@settings(max_examples=100, deadline=None)
@given(values=st.lists(st.floats(50e6, 2e9), min_size=1, max_size=40))
def test_group_total_equals_sum_of_members(values: list[float]) -> None:
    g = ParentGroup.start("AAA", rec(0, values[0]), Session.CONTINUOUS_AM, 500e6, False)
    for i, v in enumerate(values):
        g.add(rec(i, v), keep_trades=1000)
    assert abs(g.total_value - sum(values)) < 1e-3
    assert g.n_trades == len(values)
    assert abs(g.total_value - sum(t.value for t in g.trades)) < 1e-3
