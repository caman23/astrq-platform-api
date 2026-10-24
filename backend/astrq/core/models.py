"""Canonical data model shared by providers, the bus, tools and the API.

All timestamps are timezone-aware UTC. Display conversion to Asia/Ho_Chi_Minh
happens at the edges (API rendering, Telegram text, dashboard).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class SideSource(StrEnum):
    PROVIDER = "provider"
    QUOTE_RULE = "quote_rule"
    TICK_RULE = "tick_rule"
    UNKNOWN = "unknown"


class Session(StrEnum):
    PRE_OPEN = "pre_open"
    ATO = "ato"
    CONTINUOUS_AM = "continuous_am"
    LUNCH = "lunch"
    CONTINUOUS_PM = "continuous_pm"
    ATC = "atc"
    PUT_THROUGH = "put_through"
    CLOSED = "closed"

    @property
    def is_continuous(self) -> bool:
        return self in (Session.CONTINUOUS_AM, Session.CONTINUOUS_PM)

    @property
    def is_auction(self) -> bool:
        return self in (Session.ATO, Session.ATC)


class Severity(StrEnum):
    INFO = "info"
    NOTICE = "notice"
    ALERT = "alert"

    @property
    def rank(self) -> int:
        return {"info": 0, "notice": 1, "alert": 2}[self.value]


def _require_utc(v: datetime) -> datetime:
    if v.tzinfo is None or v.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return v


class Instrument(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    exchange: str = "HOSE"
    lot_size: int = 100
    sector: str | None = None


class _EventBase(BaseModel):
    model_config = ConfigDict(frozen=True)

    provider: str
    symbol: str
    ts_exchange: datetime
    ts_received: datetime
    # Monotonic wall-clock reading taken at ingest; used only to measure internal
    # latency (tick received -> signal emitted). Never serialized.
    ingest_mono: float | None = Field(default=None, exclude=True)

    @field_validator("ts_exchange", "ts_received")
    @classmethod
    def check_tz(cls, v: datetime) -> datetime:
        return _require_utc(v)


class Tick(_EventBase):
    event_type: Literal["tick"] = "tick"
    price: float = Field(gt=0)
    volume: int = Field(gt=0)
    value: float = Field(gt=0)
    side: Side = Side.UNKNOWN
    side_source: SideSource = SideSource.UNKNOWN
    session: Session = Session.CLOSED
    is_negotiated: bool = False
    seq: int | None = None
    raw_ref: str | None = None


class Quote(_EventBase):
    event_type: Literal["quote"] = "quote"
    best_bid: float | None = None
    best_ask: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None


class ExpectedPrice(_EventBase):
    event_type: Literal["expected_price"] = "expected_price"
    session: Session
    price: float
    volume: int | None = None


class ForeignFlow(_EventBase):
    event_type: Literal["foreign_flow"] = "foreign_flow"
    buy_value: float
    sell_value: float
    net_value: float


MarketEvent = Annotated[
    Tick | Quote | ExpectedPrice | ForeignFlow, Field(discriminator="event_type")
]


class Contribution(BaseModel):
    """One factor's share of a signal's score; makes every score auditable."""

    factor: str
    label: str
    raw: float
    normalized: float
    weight: float
    contribution: float
    note: str = ""


class Signal(BaseModel):
    signal_id: str
    tool_id: str
    tool_version: str
    symbol: str
    ts: datetime
    kind: str
    side: Side = Side.UNKNOWN
    score: float = 0.0
    severity: Severity = Severity.INFO
    state: str = "single"
    features: dict[str, Any] = Field(default_factory=dict)
    explanation: list[Contribution] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
    dedupe_key: str
    correlation_id: str

    @field_validator("ts")
    @classmethod
    def check_tz(cls, v: datetime) -> datetime:
        return _require_utc(v)


class Alert(BaseModel):
    signal_id: str
    channel: str
    rendered_text: str
    sent_at: datetime
    status: Literal["sent", "suppressed", "failed"]
