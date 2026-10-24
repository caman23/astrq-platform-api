from __future__ import annotations

import math
from datetime import date

from astrq.core.config import MarketRulesConfig, SessionsConfig
from astrq.core.context import Baseline
from astrq.core.market_rules import MarketRules
from astrq.core.models import Session, Side, SideSource
from astrq.core.sessions import SessionCalendar
from astrq.tools.large_order_radar.config import RadarConfig
from astrq.tools.large_order_radar.scoring import ScoreInput, score
from astrq.tools.large_order_radar.side import SideClassifier
from astrq.tools.large_order_radar.thresholds import Thresholds
from tests.helpers import quote, tick, ts


def _baseline(median: float, tier: str = "mid") -> Baseline:
    return Baseline(
        symbol="AAA",
        trading_date=date(2026, 10, 21),
        median_trade_value=median,
        p95_trade_value=median * 7,
        avg_daily_value=1e10,
        liquidity_tier=tier,
        source="t",
    )


# ------------------------------------------------------------------ sessions


def test_session_detection() -> None:
    cal = SessionCalendar(SessionsConfig())
    assert cal.session_at(ts("08:59:59")) is Session.PRE_OPEN
    assert cal.session_at(ts("09:00:00")) is Session.ATO
    assert cal.session_at(ts("09:15:00")) is Session.CONTINUOUS_AM
    assert cal.session_at(ts("11:30:00")) is Session.LUNCH
    assert cal.session_at(ts("13:00:00")) is Session.CONTINUOUS_PM
    assert cal.session_at(ts("14:30:00")) is Session.ATC
    assert cal.session_at(ts("14:50:00")) is Session.PUT_THROUGH
    assert cal.session_at(ts("15:00:00")) is Session.CLOSED


def test_weekend_and_holiday_closed() -> None:
    cal = SessionCalendar(SessionsConfig(holidays=[date(2026, 10, 21)]))
    assert cal.session_at(ts("10:00:00")) is Session.CLOSED  # holiday
    assert not cal.is_trading_day(date(2026, 10, 24))  # Saturday
    relaxed = SessionCalendar(SessionsConfig(enforce_trading_days=False))
    assert relaxed.session_at(ts("10:00:00")) is Session.CONTINUOUS_AM


def test_tick_size_ladder() -> None:
    r = MarketRules(MarketRulesConfig())
    assert r.tick_size(9_990) == 10
    assert r.tick_size(10_000) == 50
    assert r.tick_size(49_950) == 50
    assert r.tick_size(50_000) == 100


# ------------------------------------------------------------------ side classification


def test_side_provider_flag_wins() -> None:
    c = SideClassifier()
    r = c.classify(tick("AAA", "10:00:00", 20_000, 100, Side.SELL, session=Session.CONTINUOUS_AM))
    assert (r.side, r.source, r.confidence) == (Side.SELL, SideSource.PROVIDER, 1.0)


def test_side_quote_rule_and_inside_spread() -> None:
    c = SideClassifier()
    c.on_quote(quote("AAA", "10:00:00", 20_000, 20_050))
    mk = lambda p: tick(  # noqa: E731
        "AAA", "10:00:01", p, 100, session=Session.CONTINUOUS_AM, provider_side=False
    )
    assert c.classify(mk(20_050)).side is Side.BUY
    assert c.classify(mk(20_000)).side is Side.SELL
    inside = c.classify(mk(20_030))
    assert inside.side is Side.BUY and inside.inside_spread and inside.confidence == 0.5


def test_side_tick_rule_with_zero_tick() -> None:
    c = SideClassifier()
    mk = lambda p: tick(  # noqa: E731
        "AAA", "10:00:01", p, 100, session=Session.CONTINUOUS_AM, provider_side=False
    )
    assert c.classify(mk(20_000)).side is Side.UNKNOWN
    assert c.classify(mk(20_050)).side is Side.BUY
    zero = c.classify(mk(20_050))
    assert zero.side is Side.BUY and zero.source is SideSource.TICK_RULE
    assert c.classify(mk(20_000)).side is Side.SELL


def test_auction_has_no_aggressor() -> None:
    c = SideClassifier()
    r = c.classify(tick("AAA", "09:14:59", 20_000, 100, session=Session.ATO, provider_side=False))
    assert r.side is Side.UNKNOWN


# ------------------------------------------------------------------ thresholds


def test_absolute_and_relative_triggers() -> None:
    th = Thresholds(RadarConfig())
    assert th.large_triggers(600e6, None) == ["absolute"]
    assert th.large_triggers(300e6, None) == []
    # relative: mid tier k=10, median 20M => bar = max(150M, 200M) = 200M
    assert th.large_triggers(250e6, _baseline(20e6)) == ["relative"]
    assert th.large_triggers(600e6, _baseline(20e6)) == ["absolute", "relative"]


def test_group_open_threshold_is_clamped() -> None:
    th = Thresholds(RadarConfig())
    assert th.group_open_value(None) == 1.5e9
    assert th.group_open_value(_baseline(5e6)) == 500e6  # floor = absolute threshold
    assert th.group_open_value(_baseline(40e6)) == 1e9  # 25 x median
    assert th.group_open_value(_baseline(200e6)) == 1.5e9  # cap


# ------------------------------------------------------------------ scoring


def _inp(value: float, **kw: object) -> ScoreInput:
    base = dict(
        total_value=value,
        duration_sec=120,
        side=Side.BUY,
        first_price=20_000,
        last_price=20_100,
        side_confidence=1.0,
        concentration=0.5,
        baseline=_baseline(20e6),
    )
    base.update(kw)
    return ScoreInput(**base)  # type: ignore[arg-type]


def test_score_explanation_sums_to_score() -> None:
    r = score(_inp(3e9), RadarConfig())
    assert math.isclose(sum(c.contribution for c in r.explanation), r.score, abs_tol=0.2)
    assert 0 <= r.score <= 100


def test_score_monotonic_in_value() -> None:
    cfg = RadarConfig()
    scores = [score(_inp(v), cfg).score for v in (0.6e9, 1e9, 3e9, 9e9)]
    assert scores == sorted(scores)


def test_sell_impact_counts_downward_moves_only() -> None:
    cfg = RadarConfig()
    up = score(_inp(2e9, side=Side.SELL, last_price=20_200), cfg)
    down = score(_inp(2e9, side=Side.SELL, last_price=19_800), cfg)
    assert down.score > up.score


def test_cold_baseline_gets_no_relative_credit() -> None:
    cfg = RadarConfig()
    warm = score(_inp(2e9), cfg)
    cold = score(_inp(2e9, baseline=None), cfg)
    assert cold.score < warm.score
    rel = next(c for c in cold.explanation if c.factor == "relative")
    assert rel.contribution == 0
