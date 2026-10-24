"""Parent-order grouping: same symbol + same side + price band + max gap.

Lifecycle: pending -> OPEN (total >= open threshold) -> UPDATE (+step) -> CLOSE
(idle timeout, session change, or a trade that cannot join).
Invariant: group.total_value == sum(member values) (property-tested).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from astrq.core.models import Session, Side, SideSource


@dataclass
class TradeRec:
    ts: datetime
    price: float
    volume: int
    value: float
    side: Side
    side_source: SideSource
    confidence: float
    inside_spread: bool = False
    triggers: list[str] = field(default_factory=list)
    seq: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        d["side"] = self.side.value
        d["side_source"] = self.side_source.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TradeRec:
        return cls(
            ts=datetime.fromisoformat(d["ts"]),
            price=d["price"],
            volume=d["volume"],
            value=d["value"],
            side=Side(d["side"]),
            side_source=SideSource(d["side_source"]),
            confidence=d["confidence"],
            inside_spread=d["inside_spread"],
            triggers=list(d["triggers"]),
            seq=d["seq"],
        )


@dataclass
class ParentGroup:
    group_id: str
    symbol: str
    side: Side
    session: Session
    open_threshold: float
    baseline_cold: bool
    first_ts: datetime
    last_ts: datetime
    first_price: float
    last_price: float
    min_price: float
    max_price: float
    total_value: float = 0.0
    total_volume: int = 0
    n_trades: int = 0
    conf_value_sum: float = 0.0
    max_single_value: float = 0.0
    n_inferred: int = 0
    n_inside_spread: int = 0
    n_late: int = 0
    triggers: list[str] = field(default_factory=list)
    trades: list[TradeRec] = field(default_factory=list)
    state: str = "pending"  # pending | open | closed
    last_emitted_value: float = 0.0
    update_count: int = 0

    @classmethod
    def start(
        cls, symbol: str, tr: TradeRec, session: Session, open_threshold: float, cold: bool
    ) -> ParentGroup:
        gid = f"{symbol}-{tr.side.value[0]}-{int(tr.ts.timestamp() * 1000)}"
        return cls(
            group_id=gid,
            symbol=symbol,
            side=tr.side,
            session=session,
            open_threshold=open_threshold,
            baseline_cold=cold,
            first_ts=tr.ts,
            last_ts=tr.ts,
            first_price=tr.price,
            last_price=tr.price,
            min_price=tr.price,
            max_price=tr.price,
        )

    @property
    def vwap(self) -> float:
        return self.total_value / self.total_volume if self.total_volume else 0.0

    @property
    def duration_sec(self) -> float:
        return max(0.0, (self.last_ts - self.first_ts).total_seconds())

    @property
    def side_confidence(self) -> float:
        return self.conf_value_sum / self.total_value if self.total_value else 0.0

    def accepts(self, tr: TradeRec, session: Session, gap_sec: float, band: float) -> bool:
        if session is not self.session or tr.side is not self.side:
            return False
        if abs((tr.ts - self.last_ts).total_seconds()) > gap_sec:
            return False
        return abs(tr.price - self.last_price) <= band + 1e-9

    def add(self, tr: TradeRec, keep_trades: int) -> None:
        self.total_value += tr.value
        self.total_volume += tr.volume
        self.n_trades += 1
        self.conf_value_sum += tr.confidence * tr.value
        self.max_single_value = max(self.max_single_value, tr.value)
        self.min_price = min(self.min_price, tr.price)
        self.max_price = max(self.max_price, tr.price)
        if tr.side_source is not SideSource.PROVIDER:
            self.n_inferred += 1
        if tr.inside_spread:
            self.n_inside_spread += 1
        for t in tr.triggers:
            if t not in self.triggers:
                self.triggers.append(t)
        if tr.ts >= self.last_ts:
            self.last_ts = tr.ts
            self.last_price = tr.price
        else:
            self.n_late += 1
        if tr.ts < self.first_ts:
            self.first_ts = tr.ts
            self.first_price = tr.price
        self.trades.append(tr)
        if len(self.trades) > keep_trades:
            # keep the first few and the most recent ones
            head = max(1, keep_trades // 4)
            del self.trades[head]

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["side"] = self.side.value
        d["session"] = self.session.value
        d["first_ts"] = self.first_ts.isoformat()
        d["last_ts"] = self.last_ts.isoformat()
        d["trades"] = [t.to_dict() for t in self.trades]
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ParentGroup:
        d = dict(d)
        d["side"] = Side(d["side"])
        d["session"] = Session(d["session"])
        d["first_ts"] = datetime.fromisoformat(d["first_ts"])
        d["last_ts"] = datetime.fromisoformat(d["last_ts"])
        d["trades"] = [TradeRec.from_dict(t) for t in d["trades"]]
        return cls(**d)


GroupEvent = tuple[str, ParentGroup]  # ("open"|"update"|"close", group)


class GroupBook:
    def __init__(
        self, gap_sec: float, band_ticks: int, update_step: float, keep_trades: int
    ) -> None:
        self.gap_sec = gap_sec
        self.band_ticks = band_ticks
        self.update_step = update_step
        self.keep_trades = keep_trades
        self.active: dict[tuple[str, Side], ParentGroup] = {}

    def add(
        self,
        symbol: str,
        tr: TradeRec,
        session: Session,
        tick_size: float,
        open_threshold: float,
        baseline_cold: bool,
    ) -> list[GroupEvent]:
        out: list[GroupEvent] = []
        key = (symbol, tr.side)
        g = self.active.get(key)
        if g is not None and not g.accepts(tr, session, self.gap_sec, self.band_ticks * tick_size):
            out.extend(self._close(key))
            g = None
        if g is None:
            g = ParentGroup.start(symbol, tr, session, open_threshold, baseline_cold)
            self.active[key] = g
        g.add(tr, self.keep_trades)
        if g.state == "pending" and g.total_value >= g.open_threshold:
            g.state = "open"
            g.last_emitted_value = g.total_value
            out.append(("open", g))
        elif g.state == "open" and g.total_value >= g.last_emitted_value * (1 + self.update_step):
            g.update_count += 1
            g.last_emitted_value = g.total_value
            out.append(("update", g))
        return out

    def _close(self, key: tuple[str, Side]) -> list[GroupEvent]:
        g = self.active.pop(key)
        if g.state == "open":
            g.state = "closed"
            return [("close", g)]
        return []

    def close_idle(self, now: datetime, idle_sec: float) -> list[GroupEvent]:
        out: list[GroupEvent] = []
        for key in [
            k for k, g in self.active.items() if (now - g.last_ts).total_seconds() > idle_sec
        ]:
            out.extend(self._close(key))
        return out

    def close_all(self, symbol: str | None = None) -> list[GroupEvent]:
        out: list[GroupEvent] = []
        for key in [k for k in self.active if symbol is None or k[0] == symbol]:
            out.extend(self._close(key))
        return out

    def dump(self) -> list[dict[str, Any]]:
        return [g.to_dict() for g in self.active.values()]

    def load(self, data: list[dict[str, Any]]) -> None:
        self.active = {}
        for d in data:
            g = ParentGroup.from_dict(d)
            self.active[(g.symbol, g.side)] = g
