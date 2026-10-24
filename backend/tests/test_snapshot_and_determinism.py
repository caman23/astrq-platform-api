from __future__ import annotations

from datetime import date, time

from astrq.core.clock import SimClock
from astrq.core.config import Settings, SyntheticConfig
from astrq.core.context import ContextService
from astrq.core.market_rules import MarketRules
from astrq.core.models import Instrument, MarketEvent
from astrq.core.offline import run_offline
from astrq.core.sessions import SessionCalendar
from astrq.core.universe import Universe
from astrq.providers.synthetic import SyntheticContextProvider, SyntheticProvider
from astrq.tools.large_order_radar import LargeOrderRadar
from astrq.tools.large_order_radar.config import RadarConfig
from tests.helpers import FixedContext, project, radar, run, settings, universe
from tests.scenarios import split_parent

SYMS = ["VCB", "FPT", "HPG", "SSI", "MWG", "VHM", "GAS", "TCB"]


async def _synthetic_events(seed: int, minutes: int) -> list[MarketEvent]:
    s = Settings()
    cfg = SyntheticConfig(
        seed=seed,
        speed=0,
        trading_date=date(2026, 10, 21),
        start_time=time(9, 14, 0),
        parent_orders_per_hour=400,
        block_trades_per_hour=100,
    )
    cal = SessionCalendar(s.sessions)
    rules = MarketRules(s.market_rules)
    start = SyntheticProvider.start_time(cfg, cal)
    prov = SyntheticProvider(cfg, SYMS, cal, rules, SimClock(start, 0))
    await prov.connect()
    out: list[MarketEvent] = []
    end = start.timestamp() + minutes * 60
    async for ev in prov.events():
        if ev.ts_exchange.timestamp() > end:
            break
        out.append(ev)
    return out


async def test_synthetic_is_deterministic_and_pipeline_too() -> None:
    a = await _synthetic_events(7, 40)
    b = await _synthetic_events(7, 40)
    assert [e.model_dump_json() for e in a] == [e.model_dump_json() for e in b]
    c = await _synthetic_events(8, 40)
    assert [e.model_dump_json() for e in a] != [e.model_dump_json() for e in c]

    u = Universe.from_instruments("t", [Instrument(symbol=x) for x in SYMS])
    rules = MarketRules(Settings().market_rules)

    async def go() -> list[str]:
        ctx = ContextService(SyntheticContextProvider(7, rules))
        res = await run_offline(a, settings(), u, ctx, tools={"large_order_radar": radar()})
        return [s.signal_id for s in res.signals]

    ids1, ids2 = await go(), await go()
    assert ids1 == ids2 and len(ids1) > 5


async def test_restart_mid_session_keeps_open_groups() -> None:
    """Snapshot halfway, restore into a fresh tool, finish: identical to one run."""
    events = split_parent()
    full = project(await run(events))

    cut = len(events) // 2
    first_tool = LargeOrderRadar(RadarConfig())
    first = await run_offline(
        events[:cut],
        settings(),
        universe(),
        ContextService(FixedContext()),
        tools={"large_order_radar": (first_tool, radar()[1])},
        flush=False,
    )
    blob = first_tool.snapshot()
    assert first_tool.book.active, "a group must be open at the cut point"

    restored = LargeOrderRadar(RadarConfig())
    restored.restore(blob)
    assert restored.snapshot() == blob
    rest = await run_offline(
        events[cut:],
        settings(),
        universe(),
        ContextService(FixedContext()),
        tools={"large_order_radar": (restored, radar()[1])},
    )
    assert project(first) + project(rest) == full
