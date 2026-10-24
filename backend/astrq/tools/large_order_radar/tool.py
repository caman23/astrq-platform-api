"""Tool #1 — Large Order Radar (Radar Lệnh Lớn)."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any, ClassVar

from astrq.core import fmt
from astrq.core.context import Baseline
from astrq.core.models import (
    ExpectedPrice,
    MarketEvent,
    Quote,
    Severity,
    Side,
    Signal,
    Tick,
)
from astrq.core.tool import BaseTool, Subscription, ToolContext
from astrq.tools.large_order_radar.config import RadarConfig
from astrq.tools.large_order_radar.grouping import GroupBook, ParentGroup, TradeRec
from astrq.tools.large_order_radar.scoring import ScoreInput, score
from astrq.tools.large_order_radar.side import SideClassifier
from astrq.tools.large_order_radar.thresholds import Thresholds
from astrq.tools.large_order_radar.view import build_view
from astrq.tools.large_order_radar.window import DayTotals, SymbolWindow

_NS = uuid.UUID("6d1f3c4e-9a51-4f1e-8d7e-1c2b3a4d5e6f")

STATE_VI = {"open": "MỞ", "update": "CẬP NHẬT", "close": "ĐÓNG", "single": "ĐƠN"}


class LargeOrderRadar(BaseTool):
    id: ClassVar[str] = "large_order_radar"
    version: ClassVar[str] = "1.0.0"
    name: ClassVar[str] = "Radar Lệnh Lớn"
    description: ClassVar[str] = (
        "Phát hiện giao dịch lớn và chuỗi lệnh cùng chiều bị chia nhỏ trên rổ VN100."
    )
    config_model: ClassVar[type[RadarConfig]] = RadarConfig
    signal_kinds: ClassVar[list[str]] = [
        "block_order_open",
        "block_order_update",
        "block_order_close",
        "block_auction",
        "block_putthrough",
    ]

    def __init__(self, config: RadarConfig) -> None:
        super().__init__(config)
        self.cfg = config
        self.th = Thresholds(config)
        self.classifier = SideClassifier(config.quote_stale_sec)
        self.book = GroupBook(
            config.group_gap_sec,
            config.price_band_ticks,
            config.update_step_ratio,
            config.max_trades_in_features,
        )
        self.windows: dict[str, SymbolWindow] = {}
        self.days: dict[str, DayTotals] = {}
        self.expected: dict[str, dict[str, Any]] = {}
        self.trading_date: date | None = None

    def subscriptions(self) -> list[Subscription]:
        return [Subscription(frozenset({"tick", "quote", "expected_price"}))]

    # ------------------------------------------------------------ helpers

    def _window(self, symbol: str) -> SymbolWindow:
        w = self.windows.get(symbol)
        if w is None:
            w = self.windows[symbol] = SymbolWindow(self.cfg.metrics_window_sec)
        return w

    def _day(self, symbol: str) -> DayTotals:
        d = self.days.get(symbol)
        if d is None:
            d = self.days[symbol] = DayTotals()
        return d

    def _roll_day(self, ts: datetime, ctx: ToolContext) -> None:
        d = ctx.calendar.trading_date(ts)
        if self.trading_date != d:
            if self.trading_date is not None:
                self.book.active.clear()
                self.windows.clear()
                self.days.clear()
                self.expected.clear()
            self.trading_date = d

    def _baseline(self, symbol: str, ctx: ToolContext) -> Baseline | None:
        if self.trading_date is None:
            return None
        return ctx.context.baseline(symbol, self.trading_date)

    def _symbol_features(self, symbol: str) -> dict[str, Any]:
        day = self._day(symbol)
        return {
            **self._window(symbol).snapshot(),
            "day_large_buy": round(day.large_buy),
            "day_large_sell": round(day.large_sell),
            "day_net_large": round(day.net_large),
            "day_putthrough_value": round(day.putthrough_value),
            "day_putthrough_count": day.putthrough_count,
            "day_groups_opened": day.groups_opened,
        }

    @staticmethod
    def _baseline_features(b: Baseline | None) -> dict[str, Any] | None:
        if b is None:
            return None
        return {
            "median_trade_value": round(b.median_trade_value),
            "p95_trade_value": round(b.p95_trade_value),
            "avg_daily_value": round(b.avg_daily_value),
            "liquidity_tier": b.liquidity_tier,
            "source": b.source,
        }

    def _make_signal(
        self,
        ctx: ToolContext,
        *,
        symbol: str,
        ts: datetime,
        kind: str,
        state: str,
        side: Side,
        correlation_id: str,
        dedupe_suffix: str,
        result_score: float,
        severity: Severity,
        explanation: Any,
        flags: list[str],
        features: dict[str, Any],
    ) -> Signal:
        dedupe_key = f"{self.id}:{symbol}:{correlation_id}:{state}:{dedupe_suffix}"
        inst = ctx.universe.get(symbol)
        features = {
            **features,
            "sector": inst.sector if inst else None,
            "symbol_metrics": self._symbol_features(symbol),
        }
        day = self._day(symbol)
        day.max_score = max(day.max_score, result_score)
        return Signal(
            signal_id=str(uuid.uuid5(_NS, dedupe_key)),
            tool_id=self.id,
            tool_version=self.version,
            symbol=symbol,
            ts=ts,
            kind=kind,
            side=side,
            score=result_score,
            severity=severity,
            state=state,
            features=features,
            explanation=explanation,
            quality_flags=flags,
            dedupe_key=dedupe_key,
            correlation_id=correlation_id,
        )

    # ------------------------------------------------------------ groups

    def _group_signal(self, action: str, g: ParentGroup, ctx: ToolContext, ts: datetime) -> Signal:
        baseline = self._baseline(g.symbol, ctx)
        res = score(
            ScoreInput(
                total_value=g.total_value,
                duration_sec=g.duration_sec,
                side=g.side,
                first_price=g.first_price,
                last_price=g.last_price,
                side_confidence=g.side_confidence,
                concentration=self._window(g.symbol).side_concentration(g.side),
                baseline=baseline,
            ),
            self.cfg,
        )
        flags: list[str] = []
        if g.baseline_cold or baseline is None:
            flags.append("baseline_cold")
        if g.side is Side.UNKNOWN:
            flags.append("side_unknown")
        elif g.n_inferred:
            flags.append("side_inferred")
        if g.n_inside_spread:
            flags.append("inside_spread")
        if g.n_late:
            flags.append("late_ticks")
        if action == "open":
            self._day(g.symbol).groups_opened += 1
        ratio_note = ""
        if baseline is not None and baseline.median_trade_value > 0:
            ratio_note = f" · ×{g.total_value / baseline.median_trade_value:.0f} trung vị lệnh"
        move = (g.last_price - g.first_price) / g.first_price * 100 if g.first_price else 0.0
        summary = [
            f"Nhóm lệnh {STATE_VI[action]} · {g.n_trades} lệnh · "
            f"{fmt.vnd(g.total_value)}{ratio_note}",
            f"Giá {fmt.price(g.first_price)} → {fmt.price(g.last_price)} ({move:+.2f}%) · "
            f"{fmt.hms(g.first_ts)}–{fmt.hms(g.last_ts)}",
        ]
        features = {
            "group": {
                "group_id": g.group_id,
                "action": action,
                "side": g.side.value,
                "session": g.session.value,
                "total_value": round(g.total_value),
                "total_volume": g.total_volume,
                "n_trades": g.n_trades,
                "vwap": round(g.vwap, 2),
                "first_price": g.first_price,
                "last_price": g.last_price,
                "min_price": g.min_price,
                "max_price": g.max_price,
                "price_change_pct": round(move, 3),
                "started_at": g.first_ts.isoformat(),
                "last_at": g.last_ts.isoformat(),
                "duration_sec": round(g.duration_sec, 1),
                "max_single_value": round(g.max_single_value),
                "open_threshold": round(g.open_threshold),
                "triggers": list(g.triggers),
                "side_confidence": round(g.side_confidence, 3),
                "trades": [t.to_dict() for t in g.trades],
            },
            "baseline": self._baseline_features(baseline),
            "summary_vi": summary,
        }
        return self._make_signal(
            ctx,
            symbol=g.symbol,
            ts=ts,
            kind=f"block_order_{action}",
            state=action,
            side=g.side,
            correlation_id=g.group_id,
            dedupe_suffix=str(g.update_count),
            result_score=res.score,
            severity=res.severity,
            explanation=res.explanation,
            flags=flags,
            features=features,
        )

    # ------------------------------------------------------------ special sessions

    def _single_signal(
        self, t: Tick, ctx: ToolContext, kind: str, flags: list[str], factors: set[str]
    ) -> Signal:
        baseline = self._baseline(t.symbol, ctx)
        res = score(
            ScoreInput(
                total_value=t.value,
                duration_sec=0,
                side=Side.UNKNOWN,
                first_price=t.price,
                last_price=t.price,
                side_confidence=0.0,
                concentration=0.0,
                baseline=baseline,
            ),
            self.cfg,
            factors=factors,
        )
        if baseline is None:
            flags = [*flags, "baseline_cold"]
        severity = res.severity
        cap = self.cfg.putthrough_max_severity
        if kind == "block_putthrough" and severity.rank > cap.rank:
            severity = cap
        cid = f"{t.symbol}-{kind}-{int(t.ts_exchange.timestamp() * 1000)}-{t.seq or 0}"
        label = "Thỏa thuận" if kind == "block_putthrough" else f"Khớp {t.session.value.upper()}"
        trade = {
            "ts": t.ts_exchange.isoformat(),
            "price": t.price,
            "volume": t.volume,
            "value": t.value,
            "side": Side.UNKNOWN.value,
            "side_source": "unknown",
            "confidence": 0.0,
            "inside_spread": False,
            "triggers": self.th.large_triggers(t.value, baseline),
            "seq": t.seq,
        }
        features = {
            "group": {
                "group_id": cid,
                "action": "single",
                "side": "unknown",
                "session": t.session.value,
                "total_value": round(t.value),
                "total_volume": t.volume,
                "n_trades": 1,
                "vwap": t.price,
                "first_price": t.price,
                "last_price": t.price,
                "min_price": t.price,
                "max_price": t.price,
                "price_change_pct": 0.0,
                "started_at": t.ts_exchange.isoformat(),
                "last_at": t.ts_exchange.isoformat(),
                "duration_sec": 0.0,
                "max_single_value": round(t.value),
                "triggers": trade["triggers"],
                "side_confidence": 0.0,
                "trades": [trade],
            },
            "expected_price": self.expected.get(t.symbol),
            "baseline": self._baseline_features(baseline),
            "summary_vi": [
                f"{label} · {fmt.vnd(t.value)} · giá {fmt.price(t.price)} · "
                f"{fmt.hms(t.ts_exchange)}"
            ],
        }
        return self._make_signal(
            ctx,
            symbol=t.symbol,
            ts=t.ts_exchange,
            kind=kind,
            state="single",
            side=Side.UNKNOWN,
            correlation_id=cid,
            dedupe_suffix="0",
            result_score=res.score,
            severity=severity,
            explanation=res.explanation,
            flags=flags,
            features=features,
        )

    # ------------------------------------------------------------ contract

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        self._roll_day(event.ts_exchange, ctx)
        if isinstance(event, Quote):
            self.classifier.on_quote(event)
            return []
        if isinstance(event, ExpectedPrice):
            self.expected[event.symbol] = {
                "session": event.session.value,
                "price": event.price,
                "volume": event.volume,
                "ts": event.ts_exchange.isoformat(),
            }
            return []
        if not isinstance(event, Tick):
            return []
        t = event
        baseline = self._baseline(t.symbol, ctx)
        day = self._day(t.symbol)

        if t.is_negotiated:
            # Put-through is shown separately and never mixed into matched-order metrics.
            day.putthrough_value += t.value
            day.putthrough_count += 1
            if t.value >= self.cfg.putthrough_min_value:
                return [
                    self._single_signal(
                        t,
                        ctx,
                        "block_putthrough",
                        ["negotiated", "side_unknown"],
                        {"value", "relative"},
                    )
                ]
            return []

        c = self.classifier.classify(t)
        triggers = self.th.large_triggers(t.value, baseline)
        flagged = bool(triggers) or self.th.is_child_candidate(t.value, baseline)
        ts_epoch = t.ts_exchange.timestamp()
        self._window(t.symbol).add(ts_epoch, c.side, t.value, flagged, t.price)
        if flagged:
            if c.side is Side.BUY:
                day.large_buy += t.value
            elif c.side is Side.SELL:
                day.large_sell += t.value
            else:
                day.large_unknown += t.value

        if t.session.is_auction:
            if t.value >= self.th.auction_value(baseline):
                day.auction_blocks += 1
                return [
                    self._single_signal(
                        t, ctx, "block_auction", ["auction", "side_unknown"], {"value", "relative"}
                    )
                ]
            return []
        if not t.session.is_continuous or not flagged:
            return []

        rec = TradeRec(
            ts=t.ts_exchange,
            price=t.price,
            volume=t.volume,
            value=t.value,
            side=c.side,
            side_source=c.source,
            confidence=c.confidence,
            inside_spread=c.inside_spread,
            triggers=triggers,
            seq=t.seq,
        )
        inst = ctx.universe.get(t.symbol)
        tick_size = ctx.rules.tick_size(t.price, inst.exchange if inst else "HOSE")
        events = self.book.add(
            t.symbol,
            rec,
            t.session,
            tick_size,
            self.th.group_open_value(baseline),
            baseline is None,
        )
        return [
            self._group_signal(action, g, ctx, t.ts_exchange)
            for action, g in events
            if action != "close" or self.cfg.emit_close
        ]

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]:
        session = ctx.calendar.session_at(now)
        if session.is_continuous:
            events = self.book.close_idle(now, self.cfg.idle_close_sec)
        else:
            events = self.book.close_all()  # lunch break / auction / close ends every group
        # build CLOSE signals before evicting, so their metrics describe the group's period
        out = [self._group_signal(a, g, ctx, now) for a, g in events] if self.cfg.emit_close else []
        for w in self.windows.values():
            w.evict(now.timestamp())
        return out

    def snapshot(self) -> bytes:
        return json.dumps(
            {
                "trading_date": self.trading_date.isoformat() if self.trading_date else None,
                "groups": self.book.dump(),
                "windows": {s: w.dump() for s, w in self.windows.items()},
                "days": {s: vars(d) for s, d in self.days.items()},
                "classifier": self.classifier.dump(),
                "expected": self.expected,
            }
        ).encode()

    def restore(self, data: bytes) -> None:
        d = json.loads(data)
        self.trading_date = date.fromisoformat(d["trading_date"]) if d["trading_date"] else None
        self.book.load(d["groups"])
        self.windows = {}
        for s, entries in d["windows"].items():
            self._window(s).load(entries)
        self.days = {s: DayTotals(**v) for s, v in d["days"].items()}
        self.classifier.load(d["classifier"])
        self.expected = d["expected"]

    def daily_summary(self, ctx: ToolContext) -> str:
        """End-of-session report (Vietnamese)."""
        rows = sorted(self.days.items(), key=lambda kv: kv[1].net_large, reverse=True)
        buys = [(s, d) for s, d in rows if d.net_large > 0][:5]
        sells = [(s, d) for s, d in reversed(rows) if d.net_large < 0][:5]
        groups = sum(d.groups_opened for d in self.days.values())
        pt = sum(d.putthrough_value for d in self.days.values())
        lines = [
            f"📊 Tổng kết Radar Lệnh Lớn · {self.trading_date or ''}",
            f"Số nhóm lệnh lớn: {groups} · Thỏa thuận: {fmt.vnd(pt)}",
            "Ròng lớn MUA nhiều nhất: "
            + (", ".join(f"{s} {fmt.vnd(d.net_large)}" for s, d in buys) or "—"),
            "Ròng lớn BÁN nhiều nhất: "
            + (", ".join(f"{s} {fmt.vnd(d.net_large)}" for s, d in sells) or "—"),
            fmt.DISCLAIMER_VI,
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------ dashboard view

    def view(
        self, signals: Sequence[Signal], ctx: ToolContext, params: dict[str, str] | None = None
    ) -> dict[str, Any]:
        return build_view(signals, ctx, params or {}, self.days)
