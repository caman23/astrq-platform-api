"""Deterministic replay scenarios checked against golden files.

Regenerate after an intentional behaviour change:
    UPDATE_GOLDEN=1 pytest tests/test_scenarios.py
and review the diff of tests/golden/*.json before committing.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.helpers import project, run
from tests.scenarios import SCENARIOS

GOLDEN = Path(__file__).parent / "golden"


@pytest.mark.parametrize("name", sorted(SCENARIOS))
async def test_scenario_matches_golden(name: str) -> None:
    result = await run(SCENARIOS[name]())
    got = project(result)
    path = GOLDEN / f"{name}.json"
    if os.environ.get("UPDATE_GOLDEN") or not path.exists():
        GOLDEN.mkdir(exist_ok=True)
        path.write_text(json.dumps(got, indent=1, ensure_ascii=False) + "\n")
    assert got == json.loads(path.read_text())


@pytest.mark.parametrize("name", sorted(SCENARIOS))
async def test_scenario_is_deterministic(name: str) -> None:
    a = await run(SCENARIOS[name]())
    b = await run(SCENARIOS[name]())
    assert [s.model_dump(mode="json") for s in a.signals] == [
        s.model_dump(mode="json") for s in b.signals
    ]


# Semantic assertions (independent of golden files) ----------------------------


async def test_single_block_opens_and_closes() -> None:
    sig = (await run(SCENARIOS["single_block"]())).signals
    kinds = [s.kind for s in sig]
    assert kinds == ["block_order_open", "block_order_close"]
    assert set(sig[0].features["group"]["triggers"]) == {"absolute", "relative"}


async def test_split_parent_is_one_group() -> None:
    sig = (await run(SCENARIOS["split_parent"]())).signals
    assert [s.kind for s in sig] == [
        "block_order_open",
        "block_order_update",
        "block_order_update",
        "block_order_close",
    ]
    close = sig[-1].features["group"]
    assert close["n_trades"] == 12 and close["price_change_pct"] > 0
    assert len({s.correlation_id for s in sig}) == 1


async def test_reversal_produces_two_opposite_groups() -> None:
    sig = (await run(SCENARIOS["reversal"]())).signals
    opens = [s for s in sig if s.kind == "block_order_open"]
    assert [s.side.value for s in opens] == ["buy", "sell"]


async def test_lunch_break_splits_group() -> None:
    sig = (await run(SCENARIOS["lunch_break"]())).signals
    opens = [s for s in sig if s.kind == "block_order_open"]
    assert [s.features["group"]["session"] for s in opens] == ["continuous_am", "continuous_pm"]


async def test_auctions_flag_only_large_prints() -> None:
    sig = (await run(SCENARIOS["auctions"]())).signals
    assert [(s.symbol, s.kind) for s in sig] == [("AAA", "block_auction"), ("AAA", "block_auction")]
    assert all("auction" in s.quality_flags for s in sig)


async def test_putthrough_is_separate_and_does_not_break_group() -> None:
    sig = (await run(SCENARIOS["putthrough_interleaved"]())).signals
    pt = [s for s in sig if s.kind == "block_putthrough"]
    assert len(pt) == 1 and pt[0].severity.value in ("info", "notice")
    groups = {s.correlation_id for s in sig if s.kind.startswith("block_order")}
    assert len(groups) == 1
    close = next(s for s in sig if s.kind == "block_order_close")
    assert close.features["group"]["n_trades"] == 8
    m = close.features["symbol_metrics"]
    assert m["day_putthrough_value"] == 19_500_000_000
    assert m["large_buy"] == close.features["group"]["total_value"]  # put-through excluded


async def test_duplicates_dropped_and_late_flagged() -> None:
    res = await run(SCENARIOS["duplicates_and_late"]())
    assert res.ingest_stats["duplicates"] == 1
    assert res.ingest_stats["late"] == 1
    close = next(s for s in res.signals if s.kind == "block_order_close")
    assert close.features["group"]["n_trades"] == 9
    assert "late_ticks" in close.quality_flags


async def test_inferred_side_is_flagged() -> None:
    sig = (await run(SCENARIOS["inferred_side"]())).signals
    assert sig and all(s.side.value == "buy" for s in sig)
    assert all("side_inferred" in s.quality_flags for s in sig)
