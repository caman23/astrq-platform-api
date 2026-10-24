"""Deterministic synthetic market (seeded) for development and tests.

It produces a full trading day for the universe: background trades with a
one-tick spread, split parent orders (runs of same-side child trades), single
block trades, put-through deals, and single-price ATO/ATC matches. Same seed +
same universe => byte-identical event stream.
"""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from astrq.core.clock import SimClock
from astrq.core.config import SyntheticConfig
from astrq.core.context import Baseline
from astrq.core.market_rules import MarketRules
from astrq.core.models import ExpectedPrice, MarketEvent, Quote, Session, Side, SideSource, Tick
from astrq.core.sessions import SessionCalendar
from astrq.providers.base import ProviderStatus

_TIERS = {
    # tier: (weight, trades per minute, median VND value per trade)
    "high": (30, 36.0, 45e6),
    "mid": (45, 14.0, 22e6),
    "low": (25, 5.0, 10e6),
}


@dataclass(frozen=True)
class SymbolProfile:
    symbol: str
    ref_price: float
    tier: str
    trades_per_min: float
    median_shares: int

    @property
    def median_trade_value(self) -> float:
        return self.ref_price * self.median_shares


def synthetic_profile(symbol: str, seed: int, rules: MarketRules) -> SymbolProfile:
    rng = random.Random(f"profile:{seed}:{symbol}")
    tier = rng.choices(list(_TIERS), weights=[w for w, _, _ in _TIERS.values()])[0]
    _, tpm, median_value = _TIERS[tier]
    price = rules.round_to_tick(math.exp(rng.uniform(math.log(9_000), math.log(140_000))))
    shares = median_value * rng.uniform(0.7, 1.4) / price
    return SymbolProfile(
        symbol=symbol,
        ref_price=price,
        tier=tier,
        trades_per_min=tpm * rng.uniform(0.7, 1.3),
        median_shares=max(100, int(shares) // 100 * 100),
    )


class SyntheticContextProvider:
    """Baselines consistent with the synthetic market (same seed => same numbers)."""

    name = "synthetic"

    def __init__(self, seed: int, rules: MarketRules) -> None:
        self.seed = seed
        self.rules = rules

    async def baseline(self, symbol: str, trading_date: date) -> Baseline | None:
        p = synthetic_profile(symbol, self.seed, self.rules)
        per_day = p.trades_per_min * 270 * p.median_trade_value * 1.6
        return Baseline(
            symbol=symbol,
            trading_date=trading_date,
            median_trade_value=p.median_trade_value,
            p95_trade_value=p.median_trade_value * 7,
            avg_daily_value=per_day,
            liquidity_tier=p.tier,
            source="synthetic",
        )


@dataclass
class _SymState:
    profile: SymbolProfile
    tick: float
    bid: float
    seq: int = 0
    auction_volume: int = 0


@dataclass
class _Parent:
    symbol: str
    side: Side
    remaining_value: float
    child_value: float
    interval_s: int
    next_at: datetime
    children_done: int = 0
    impact_every: int = 3


class SyntheticProvider:
    name = "synthetic"

    def __init__(
        self,
        cfg: SyntheticConfig,
        symbols: Sequence[str],
        calendar: SessionCalendar,
        rules: MarketRules,
        clock: SimClock,
        stop_at_close: bool = False,
    ) -> None:
        self.cfg = cfg
        self.calendar = calendar
        self.rules = rules
        self.clock = clock
        self.stop_at_close = stop_at_close
        self.status = ProviderStatus()
        self._rng = random.Random(f"market:{cfg.seed}")
        self._symbols = sorted(symbols)
        self._state: dict[str, _SymState] = {}
        self._parents: list[_Parent] = []
        self._closed = False
        self._ms = 0
        for s in self._symbols:
            p = synthetic_profile(s, cfg.seed, rules)
            tick = rules.tick_size(p.ref_price)
            self._state[s] = _SymState(profile=p, tick=tick, bid=p.ref_price)

    @staticmethod
    def start_time(cfg: SyntheticConfig, calendar: SessionCalendar) -> datetime:
        d = cfg.trading_date or datetime.now(calendar.tz).date()
        return calendar.at_local(d, cfg.start_time)

    async def connect(self) -> None:
        self.status.connected = True
        self.status.detail = f"synthetic seed={self.cfg.seed} speed={self.cfg.speed}x"

    async def subscribe(self, symbols: Sequence[str], channels: Sequence[str]) -> None:
        wanted = set(symbols)
        for s in wanted - set(self._state):
            p = synthetic_profile(s, self.cfg.seed, self.rules)
            self._state[s] = _SymState(
                profile=p, tick=self.rules.tick_size(p.ref_price), bid=p.ref_price
            )
        self._symbols = sorted(wanted)

    async def close(self) -> None:
        self._closed = True
        self.status.connected = False

    async def events(self) -> AsyncIterator[MarketEvent]:
        t = self.start_time(self.cfg, self.calendar)
        last_session = self.calendar.session_at(t)
        while not self._closed:
            if self.clock.speed > 0:
                lag = (t - self.clock.now()).total_seconds()
                if lag > 0:
                    await asyncio.sleep(min(0.25, lag / self.clock.speed))
                    continue
            else:
                self.clock.advance_to(t)
            session = self.calendar.session_at(t)
            batch = list(self._step(t, session, last_session))
            last_session = session
            batch.sort(key=lambda e: e.ts_exchange)
            for ev in batch:
                self.status.last_event_at = ev.ts_received
                yield ev
            if self.clock.speed <= 0 and len(batch) > 0:
                await asyncio.sleep(0)  # let consumers run in offline mode
            t += timedelta(seconds=1)
            if session is Session.CLOSED and last_session is Session.CLOSED and self._past_close(t):
                self.status.finished = True
                if self.stop_at_close:
                    return
                await asyncio.sleep(1.0)

    def _past_close(self, t: datetime) -> bool:
        last = max(w.end for w in self.calendar.cfg.schedule)
        return self.calendar.local(t).time() >= last

    # ---------------------------------------------------------------- generation

    def _step(self, t: datetime, session: Session, prev: Session) -> Iterator[MarketEvent]:
        rng = self._rng
        self._ms = 0
        if prev.is_auction and session != prev:
            yield from self._auction_match(t - timedelta(milliseconds=1), prev)
        if session.is_auction:
            yield from self._auction_build(t, session)
            return
        if session.is_continuous:
            yield from self._background(t, session)
            yield from self._spawn_and_run_parents(t, session)
            if rng.random() < self.cfg.block_trades_per_hour / 3600:
                yield self._block_trade(t, session)
        if session.is_continuous or session is Session.PUT_THROUGH:
            rate = self.cfg.put_through_per_hour / 3600
            if session is Session.PUT_THROUGH:
                rate *= 6
            if rng.random() < rate:
                yield self._put_through(t, session)

    def _ts(self, t: datetime) -> tuple[datetime, datetime]:
        # Monotonic within the second so per-symbol seq order matches time order.
        self._ms = min(999, self._ms + self._rng.randint(1, 6))
        ex = t + timedelta(milliseconds=self._ms)
        rx = ex + timedelta(milliseconds=self._rng.randint(40, 600))
        return ex, rx

    def _clamp(self, st: _SymState, price: float) -> float:
        lo, hi = st.profile.ref_price * 0.931, st.profile.ref_price * 1.069
        return self.rules.round_to_tick(min(hi, max(lo, price)))

    def _quote(self, st: _SymState, t: datetime) -> Quote:
        ex, rx = self._ts(t)
        return Quote(
            provider=self.name,
            symbol=st.profile.symbol,
            ts_exchange=ex,
            ts_received=rx,
            best_bid=st.bid,
            best_ask=st.bid + st.tick,
            bid_size=self._rng.randint(1, 50) * 100,
            ask_size=self._rng.randint(1, 50) * 100,
        )

    def _trade(
        self,
        st: _SymState,
        t: datetime,
        session: Session,
        side: Side,
        volume: int,
        price: float | None = None,
        negotiated: bool = False,
    ) -> Tick:
        st.seq += 1
        if price is None:
            price = st.bid + st.tick if side is Side.BUY else st.bid
        ex, rx = self._ts(t)
        emit_side = self.cfg.emit_side_flag and not negotiated and not session.is_auction
        return Tick(
            provider=self.name,
            symbol=st.profile.symbol,
            ts_exchange=ex,
            ts_received=rx,
            price=price,
            volume=volume,
            value=price * volume,
            side=side if emit_side else Side.UNKNOWN,
            side_source=SideSource.PROVIDER if emit_side else SideSource.UNKNOWN,
            session=session,
            is_negotiated=negotiated,
            seq=st.seq,
            raw_ref=f"syn:{st.profile.symbol}:{st.seq}",
        )

    def _small_volume(self, st: _SymState) -> int:
        shares = st.profile.median_shares * self._rng.lognormvariate(0, 0.8)
        return max(100, int(shares) // 100 * 100)

    def _background(self, t: datetime, session: Session) -> Iterator[MarketEvent]:
        rng = self._rng
        for sym in self._symbols:
            st = self._state[sym]
            lam = st.profile.trades_per_min / 60
            if rng.random() >= lam:
                continue
            if rng.random() < 0.12:
                st.bid = self._clamp(st, st.bid + st.tick * rng.choice((-1, 1)))
                if self.cfg.emit_quotes:
                    yield self._quote(st, t)
            side = Side.BUY if rng.random() < 0.5 else Side.SELL
            yield self._trade(st, t, session, side, self._small_volume(st))

    def _pick_symbol(self) -> _SymState:
        weights = [_TIERS[self._state[s].profile.tier][0] for s in self._symbols]
        return self._state[self._rng.choices(self._symbols, weights=weights)[0]]

    def _spawn_and_run_parents(self, t: datetime, session: Session) -> Iterator[MarketEvent]:
        rng = self._rng
        if rng.random() < self.cfg.parent_orders_per_hour / 3600:
            st = self._pick_symbol()
            child = max(100e6, st.profile.median_trade_value * rng.uniform(4.5, 12))
            self._parents.append(
                _Parent(
                    symbol=st.profile.symbol,
                    side=Side.BUY if rng.random() < 0.55 else Side.SELL,
                    remaining_value=child * rng.randint(6, 30),
                    child_value=child,
                    interval_s=rng.randint(2, 12),
                    next_at=t,
                    impact_every=rng.randint(2, 5),
                )
            )
        still: list[_Parent] = []
        for p in self._parents:
            if p.next_at <= t:
                st = self._state[p.symbol]
                value = min(p.child_value * rng.uniform(0.7, 1.3), p.remaining_value)
                price = st.bid + st.tick if p.side is Side.BUY else st.bid
                vol = max(100, int(value / price) // 100 * 100)
                yield self._trade(st, t, session, p.side, vol)
                p.remaining_value -= price * vol
                p.children_done += 1
                if p.children_done % p.impact_every == 0:
                    step = st.tick if p.side is Side.BUY else -st.tick
                    st.bid = self._clamp(st, st.bid + step)
                    if self.cfg.emit_quotes:
                        yield self._quote(st, t)
                p.next_at = t + timedelta(seconds=max(1, p.interval_s + rng.randint(-1, 2)))
            if p.remaining_value > 0:
                still.append(p)
        self._parents = still

    def _block_trade(self, t: datetime, session: Session) -> Tick:
        st = self._pick_symbol()
        side = Side.BUY if self._rng.random() < 0.5 else Side.SELL
        value = max(0.6e9, st.profile.median_trade_value * self._rng.uniform(12, 50))
        price = st.bid + st.tick if side is Side.BUY else st.bid
        return self._trade(st, t, session, side, max(100, int(value / price) // 100 * 100))

    def _put_through(self, t: datetime, session: Session) -> Tick:
        st = self._pick_symbol()
        value = self._rng.uniform(5e9, 60e9)
        price = self._clamp(st, st.bid * self._rng.uniform(0.97, 1.03))
        vol = max(100, int(value / price) // 100 * 100)
        return self._trade(
            st,
            t,
            Session.PUT_THROUGH if not session.is_continuous else session,
            Side.UNKNOWN,
            vol,
            price=price,
            negotiated=True,
        )

    def _auction_build(self, t: datetime, session: Session) -> Iterator[MarketEvent]:
        rng = self._rng
        if self.calendar.local(t).second % 30 != 0:
            return
        for sym in self._symbols:
            st = self._state[sym]
            if rng.random() < 0.6:
                st.auction_volume += self._small_volume(st)
            if rng.random() < 0.5:
                ex, rx = self._ts(t)
                yield ExpectedPrice(
                    provider=self.name,
                    symbol=sym,
                    ts_exchange=ex,
                    ts_received=rx,
                    session=session,
                    price=self._clamp(st, st.bid + st.tick * rng.randint(-2, 2)),
                    volume=st.auction_volume,
                )

    def _auction_match(self, t: datetime, session: Session) -> Iterator[MarketEvent]:
        rng = self._rng
        for sym in self._symbols:
            st = self._state[sym]
            vol = st.auction_volume
            st.auction_volume = 0
            if rng.random() < 0.06:  # occasional big auction interest
                vol += int(rng.uniform(1e9, 8e9) / st.bid) // 100 * 100
            if vol <= 0:
                continue
            price = self._clamp(st, st.bid + st.tick * rng.randint(-2, 2))
            st.bid = price
            st.seq += 1
            rx = t + timedelta(milliseconds=rng.randint(40, 600))
            yield Tick(
                provider=self.name,
                symbol=sym,
                ts_exchange=t,
                ts_received=rx,
                price=price,
                volume=vol,
                value=price * vol,
                side=Side.UNKNOWN,
                side_source=SideSource.UNKNOWN,
                session=session,
                seq=st.seq,
                raw_ref=f"syn:{sym}:{st.seq}",
            )


def profiles_for(symbols: Sequence[str], seed: int, rules: MarketRules) -> dict[str, SymbolProfile]:
    return {s: synthetic_profile(s, seed, rules) for s in symbols}
