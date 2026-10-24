from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from astrq.core.models import Session, Side, Tick
from astrq.providers.recorded_baseline import RecordedBaselineProvider


def _write_day(d: date, values: list[float], tmp: Path, extra: list[Tick] | None = None) -> None:
    lines = []
    for i, v in enumerate(values):
        t = datetime(d.year, d.month, d.day, 3, 0, i % 60, tzinfo=UTC)
        lines.append(
            Tick(
                provider="ssi",
                symbol="FPT",
                ts_exchange=t,
                ts_received=t,
                price=100_000,
                volume=int(v / 100_000),
                value=v,
                side=Side.BUY,
                session=Session.CONTINUOUS_AM,
            ).model_dump_json()
        )
    for e in extra or []:
        lines.append(e.model_dump_json())
    lines.append('{"event_type":"quote","symbol":"FPT"}')  # non-tick lines are skipped
    (tmp / "ticks").mkdir(exist_ok=True)
    (tmp / "ticks" / f"{d.isoformat()}.jsonl").write_text("\n".join(lines) + "\n")


async def test_baseline_from_recorded_days(tmp_path: Path) -> None:
    t = datetime(2026, 10, 19, 7, 50, tzinfo=UTC)  # ATC -> excluded
    auction = Tick(
        provider="ssi",
        symbol="FPT",
        ts_exchange=t,
        ts_received=t,
        price=1e5,
        volume=10**6,
        value=1e11,
        session=Session.ATC,
    )
    pt = Tick(
        provider="ssi",
        symbol="FPT",
        ts_exchange=t,
        ts_received=t,
        price=1e5,
        volume=10**6,
        value=1e11,
        session=Session.CONTINUOUS_PM,
        is_negotiated=True,
    )
    _write_day(date(2026, 10, 19), [10e6, 20e6, 30e6], tmp_path, [auction, pt])
    _write_day(date(2026, 10, 20), [20e6, 40e6, 60e6], tmp_path)

    prov = RecordedBaselineProvider(tmp_path / "ticks", tmp_path / "baselines", 10, min_days=2)
    b = await prov.baseline("FPT", date(2026, 10, 21))
    assert b is not None
    assert b.median_trade_value == 30e6  # median of daily medians (20M, 40M)
    assert b.avg_daily_value == (60e6 + 120e6) / 2  # auction & put-through excluded
    assert b.liquidity_tier == "low" and b.source == "recorded:2d"
    assert (tmp_path / "baselines" / "2026-10-19.json").exists()  # cached per day

    # the current day's own file is never used; too few days -> cold
    cold = RecordedBaselineProvider(tmp_path / "ticks", tmp_path / "b2", 10, min_days=3)
    assert await cold.baseline("FPT", date(2026, 10, 21)) is None
    assert await prov.baseline("FPT", date(2026, 10, 20)) is None  # only 1 earlier day
