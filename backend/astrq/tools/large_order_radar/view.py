"""Aggregated radar view for the dashboard: one entry per symbol that had at
least one large-order signal today, with its orders (latest state of each
correlation id) and child trades for hover details.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from astrq.core.models import Severity, Signal

if TYPE_CHECKING:
    from astrq.core.tool import ToolContext
    from astrq.tools.large_order_radar.window import DayTotals


def _order_from(s: Signal, prev: dict[str, Any] | None) -> dict[str, Any]:
    g = dict(s.features.get("group") or {})
    trades = g.pop("trades", [])
    peak = max(s.score, prev["peak_score"]) if prev else s.score
    peak_sev = s.severity
    if prev and Severity(prev["peak_severity"]).rank > peak_sev.rank:
        peak_sev = Severity(prev["peak_severity"])
    return {
        "id": s.correlation_id,
        "kind": s.kind,
        "state": s.state,
        "side": s.side.value,
        "score": s.score,
        "severity": s.severity.value,
        "peak_score": peak,
        "peak_severity": peak_sev.value,
        "quality_flags": s.quality_flags,
        "updated_at": s.ts.isoformat(),
        "updates": (prev["updates"] + 1) if prev else 1,
        "summary_vi": s.features.get("summary_vi", []),
        "explanation": [c.model_dump() for c in s.explanation],
        **g,
        "trades": trades,
    }


def build_view(
    signals: Sequence[Signal],
    ctx: ToolContext,
    params: Mapping[str, str],
    days: Mapping[str, DayTotals],
) -> dict[str, Any]:
    only = params.get("symbol")
    with_trades = int(params.get("orders_with_trades", "3"))
    by_sym: dict[str, dict[str, Any]] = {}
    for s in signals:
        if only and s.symbol != only:
            continue
        e = by_sym.setdefault(
            s.symbol,
            {"symbol": s.symbol, "orders": {}, "sector": None, "baseline": None, "metrics": {}},
        )
        e["orders"][s.correlation_id] = _order_from(s, e["orders"].get(s.correlation_id))
        e["metrics"] = s.features.get("symbol_metrics", e["metrics"])
        e["sector"] = s.features.get("sector") or e["sector"]
        e["baseline"] = s.features.get("baseline") or e["baseline"]
        e["last_signal_at"] = s.ts.isoformat()

    out: list[dict[str, Any]] = []
    for sym, e in by_sym.items():
        orders = sorted(e["orders"].values(), key=lambda o: o.get("last_at") or "", reverse=True)
        if not only:
            for o in orders[with_trades:]:
                o["trades"] = []
            orders = orders[:30]
        # Put-through deals are listed but do not position the symbol on the radar.
        ranked = [o for o in orders if o["kind"] != "block_putthrough"] or orders
        peak = max(ranked, key=lambda o: o["peak_score"])
        sev = max((Severity(o["peak_severity"]) for o in ranked), key=lambda x: x.rank)
        m = e["metrics"]
        day = days.get(sym)
        inst = ctx.universe.get(sym)
        out.append(
            {
                "symbol": sym,
                "sector": e["sector"] or (inst.sector if inst else None),
                "max_score": peak["peak_score"],
                "max_severity": sev.value,
                "dominant_side": _dominant(day, m),
                "last_signal_at": e["last_signal_at"],
                "n_orders": len(e["orders"]),
                "day_large_buy": round(day.large_buy) if day else m.get("day_large_buy", 0),
                "day_large_sell": round(day.large_sell) if day else m.get("day_large_sell", 0),
                "day_net_large": round(day.net_large) if day else m.get("day_net_large", 0),
                "day_putthrough_value": round(day.putthrough_value)
                if day
                else m.get("day_putthrough_value", 0),
                "metrics": m,
                "baseline": e["baseline"],
                "orders": orders,
            }
        )
    out.sort(key=lambda x: (-x["max_score"], x["symbol"]))
    trading_date = ctx.calendar.trading_date(ctx.clock.now()).isoformat()
    return {"trading_date": trading_date, "symbols": out}


def _dominant(day: DayTotals | None, m: Mapping[str, Any]) -> str:
    buy = day.large_buy if day else m.get("day_large_buy", 0)
    sell = day.large_sell if day else m.get("day_large_sell", 0)
    total = buy + sell
    if total <= 0:
        return "unknown"
    if buy / total >= 0.6:
        return "buy"
    if sell / total >= 0.6:
        return "sell"
    return "mixed"
