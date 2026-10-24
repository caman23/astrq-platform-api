from __future__ import annotations

from datetime import timedelta

from astrq.core.alerts import AlertRouter, CallbackChannel, render_signal
from astrq.core.models import Severity, Side, Signal
from tests.helpers import ts


def sig(
    key: str, sec: int, sev: Severity = Severity.NOTICE, symbol: str = "AAA", side: Side = Side.BUY
) -> Signal:
    return Signal(
        signal_id=key,
        tool_id="large_order_radar",
        tool_version="1",
        symbol=symbol,
        ts=ts("10:00:00") + timedelta(seconds=sec),
        kind="block_order_open",
        side=side,
        score=50,
        severity=sev,
        features={"summary_vi": ["Nhóm lệnh MỞ"]},
        dedupe_key=key,
        correlation_id="g",
    )


class Clock:
    t = 0.0

    def __call__(self) -> float:
        return self.t


async def _noop(text: str, s: Signal | None) -> None:
    return None


def make(
    max_per_minute: int = 100, min_sev: Severity = Severity.INFO
) -> tuple[AlertRouter, CallbackChannel, Clock]:
    ch = CallbackChannel("c", _noop, min_sev, max_per_minute)
    clock = Clock()
    return AlertRouter([ch], cooldown_sec=120, wall=clock), ch, clock


def test_dedupe() -> None:
    r, ch, _ = make()
    assert r.decide(sig("k1", 0), ch) == "send"
    assert r.decide(sig("k1", 500), ch) == "duplicate"


def test_cooldown_and_escalation() -> None:
    r, ch, _ = make()
    assert r.decide(sig("a", 0), ch) == "send"
    assert r.decide(sig("b", 30), ch) == "cooldown"
    assert r.decide(sig("c", 40, Severity.ALERT), ch) == "send"  # escalation passes
    assert r.decide(sig("d", 50, Severity.ALERT), ch) == "cooldown"
    assert r.decide(sig("e", 200), ch) == "send"  # cooldown expired
    assert r.decide(sig("f", 201, side=Side.SELL), ch) == "send"  # other side independent


def test_rate_limit_per_minute() -> None:
    r, ch, clock = make(max_per_minute=2)
    assert r.decide(sig("a", 0, symbol="AAA"), ch) == "send"
    assert r.decide(sig("b", 0, symbol="BBB"), ch) == "send"
    assert r.decide(sig("c", 0, symbol="CCC"), ch) == "rate_limited"
    clock.t = 61
    assert r.decide(sig("d", 0, symbol="DDD"), ch) == "send"


def test_severity_and_tool_filters() -> None:
    r, ch, _ = make(min_sev=Severity.ALERT)
    assert r.decide(sig("a", 0), ch) == "below_severity"
    r2 = AlertRouter([ch], 120, tool_enabled=lambda _t: False)
    assert r2.decide(sig("b", 0, Severity.ALERT), ch) == "tool_disabled"


async def test_coalesced_count_is_rendered() -> None:
    sent: list[str] = []

    async def cb(text: str, s: Signal | None) -> None:
        sent.append(text)

    ch = CallbackChannel("c", cb, Severity.INFO, 100)
    r = AlertRouter([ch], 120)
    await r.handle(sig("a", 0))
    await r.handle(sig("b", 10))
    await r.handle(sig("c", 20))
    await r.handle(sig("d", 30, Severity.ALERT))
    assert len(sent) == 2
    assert "(+2 cập nhật cùng mã đã gộp)" in sent[1]
    assert "không phải khuyến nghị đầu tư" in sent[0]


def test_render_contains_essentials() -> None:
    text = render_signal(sig("a", 0))
    assert "MUA LỚN · AAA" in text and "10:00:00" in text
