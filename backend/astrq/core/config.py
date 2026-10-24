"""Settings: YAML file + environment overrides (prefix ASTRQ_, nested with __).

Example: ASTRQ_PROVIDER__SYNTHETIC__SPEED=60 overrides provider.synthetic.speed.
Secrets (tokens) must come from the environment, never from the YAML file.
"""

from __future__ import annotations

import os
from datetime import date, time
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from astrq.core.models import Session, Severity


class SessionWindow(BaseModel):
    session: Session
    start: time
    end: time


def _default_schedule() -> list[SessionWindow]:
    # TODO(verify): HOSE session hours must be confirmed against the current
    # exchange regulation before trading on real data (docs/open-questions.md #3).
    rows = [
        (Session.ATO, time(9, 0), time(9, 15)),
        (Session.CONTINUOUS_AM, time(9, 15), time(11, 30)),
        (Session.LUNCH, time(11, 30), time(13, 0)),
        (Session.CONTINUOUS_PM, time(13, 0), time(14, 30)),
        (Session.ATC, time(14, 30), time(14, 45)),
        (Session.PUT_THROUGH, time(14, 45), time(15, 0)),
    ]
    return [SessionWindow(session=s, start=a, end=b) for s, a, b in rows]


class SessionsConfig(BaseModel):
    timezone: str = "Asia/Ho_Chi_Minh"
    enforce_trading_days: bool = True
    schedule: list[SessionWindow] = Field(default_factory=_default_schedule)
    holidays: list[date] = Field(default_factory=list)


class TickSizeRule(BaseModel):
    exchange: str
    below: float | None = None  # price strictly below this uses `size`; None = no upper bound
    size: float


def _default_tick_rules() -> list[TickSizeRule]:
    # TODO(verify): HOSE tick-size ladder; confirm against current regulation.
    return [
        TickSizeRule(exchange="HOSE", below=10_000, size=10),
        TickSizeRule(exchange="HOSE", below=50_000, size=50),
        TickSizeRule(exchange="HOSE", below=None, size=100),
    ]


class MarketRulesConfig(BaseModel):
    tick_sizes: list[TickSizeRule] = Field(default_factory=_default_tick_rules)


class SyntheticConfig(BaseModel):
    seed: int = 42
    # Simulated seconds per wall second. 0 = as fast as possible (offline runs).
    speed: float = 30.0
    trading_date: date | None = None  # None = today (Vietnam time)
    start_time: time = time(8, 59, 30)
    emit_side_flag: bool = True
    emit_quotes: bool = True
    parent_orders_per_hour: float = 70.0
    block_trades_per_hour: float = 25.0
    put_through_per_hour: float = 4.0


class ReplayConfig(BaseModel):
    path: str = "data/ticks/replay.jsonl"
    speed: float = 1.0  # 0 = as fast as possible


class SsiConfig(BaseModel):
    """SSI FastConnect Data. Endpoints/protocol verified from the official guide and
    the official `ssi-fc-data` 2.2.2 client source (see docs/provider-notes.md)."""

    api_url: str = "https://fc-data.ssi.com.vn/"
    stream_url: str = "https://fc-datahub.ssi.com.vn/"
    consumer_id: str | None = None
    consumer_secret: SecretStr | None = None
    # One SwitchChannels call per connection. "X-TRADE" = matched trades with the
    # BU/SD side flag; "X" = trade + best bid/ask snapshot.
    channel: Literal["X-TRADE", "X"] = "X-TRADE"
    subscribe_all: bool = False  # True -> "<channel>:ALL" (whole market, filtered locally)
    # TODO(verify): units. The documented sample shows VND prices (Ceiling 24500).
    price_multiplier: float = 1.0
    volume_multiplier: int = 1
    # TODO(verify): TradingSession codes are not published in a readable table yet.
    # Fill from codes observed in /api/status (provider.stats.sessions_seen).
    # Unmapped codes fall back to the configured session calendar.
    session_map: dict[str, Session] = Field(default_factory=dict)
    record_raw: bool = True
    idle_reconnect_sec: float = 180.0  # reconnect if silent this long during a session


class DnseConfig(BaseModel):
    """DNSE LightSpeed API market data WebSocket. Protocol verified from the official
    developer docs (developers.dnse.com.vn: market-data/connect, sdk/build_websocket)."""

    ws_url: str = "wss://ws-openapi.dnse.com.vn/v1/stream"
    api_key: str | None = None
    api_secret: SecretStr | None = None
    # Channel names without the encoding suffix ("{name}.json" is sent). tick_extra adds
    # the active buy/sell side. Boards: G1 = round lot; T1/T3 = negotiated (put-through).
    # Max 200 streams per connection (docs) — keep the list short for ~100 symbols.
    channels: list[str] = Field(default_factory=lambda: ["tick_extra.G1", "expected_price.G1"])
    putthrough_boards: list[str] = Field(default_factory=lambda: ["T1", "T3"])
    # Verified live: streams are counted per channel x symbol; normalUser tier = 200.
    # Channels earlier in `channels` get the budget first.
    max_streams: int = 200
    # TODO(verify): price unit. Doc samples show decimals like 24.35 (likely thousand VND).
    # auto = multiply by 1000 when a stock price is < 1000.
    price_unit: Literal["auto", "vnd", "thousand"] = "auto"
    volume_multiplier: int = 1
    # TODO(verify): tradingSessionId codes (docs only show "40"); unmapped -> calendar.
    session_map: dict[str, Session] = Field(default_factory=dict)
    record_raw: bool = True
    idle_reconnect_sec: float = 240.0  # server pings every 3 min; silence longer = dead


class ProviderConfig(BaseModel):
    kind: Literal["synthetic", "replay", "dnse", "ssi"] = "synthetic"
    synthetic: SyntheticConfig = Field(default_factory=SyntheticConfig)
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    ssi: SsiConfig = Field(default_factory=SsiConfig)
    dnse: DnseConfig = Field(default_factory=DnseConfig)


class UniverseConfig(BaseModel):
    file: str = "config/universe_vn100.yaml"
    reload_interval_sec: float = 300.0


class BusConfig(BaseModel):
    queue_size: int = 20_000


class ToolEntry(BaseModel):
    enabled: bool = True
    module: str
    config: dict[str, Any] = Field(default_factory=dict)
    timeout_ms: int = 250
    max_consecutive_failures: int = 5
    breaker_cooldown_sec: float = 30.0
    timer_interval_sec: float = 5.0  # in market-clock seconds


class TelegramConfig(BaseModel):
    enabled: bool = False
    bot_token: SecretStr | None = None
    chat_id: str | None = None
    min_severity: Severity = Severity.ALERT
    max_per_minute: int = 20


class AlertsConfig(BaseModel):
    cooldown_sec: float = 120.0  # per (symbol, side, channel), market clock
    dashboard_min_severity: Severity = Severity.INFO
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)


class RemoteStorageConfig(BaseModel):
    """Optional S3-compatible object storage (Cloudflare R2) for hosts without a persistent
    disk (e.g. Render free): keeps daily baseline summaries and today's signals."""

    kind: Literal["none", "r2"] = "none"
    endpoint: str | None = None  # https://<account_id>.r2.cloudflarestorage.com
    bucket: str | None = None
    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    region: str = "auto"
    prefix: str = "astrq/"
    signals_upload_interval_sec: float = 900.0  # gzip upload of today's signals
    keep_summary_days: int = 30


class StorageConfig(BaseModel):
    data_dir: str = "data"
    record_ticks: bool = True
    record_signals: bool = True
    snapshot_interval_sec: float = 30.0
    retention_days: int = 14
    remote: RemoteStorageConfig = Field(default_factory=RemoteStorageConfig)


class ContextConfig(BaseModel):
    # recorded = baselines computed from our own recorded tick files (ADR-0004)
    kind: Literal["synthetic", "recorded", "none", "vndirect"] = "synthetic"
    lookback_days: int = 10
    min_days: int = 3  # fewer recorded sessions -> baseline_cold
    cache_ttl_sec: float = 6 * 3600
    warmup_time: time = time(8, 30)
    eod_summary_time: time = time(15, 5)


class ApiConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:5173"])
    frontend_dist: str = "frontend/dist"
    # Which sites may embed the dashboard in an <iframe> (CSP frame-ancestors).
    # "'self'" = same origin only. Add e.g. "https://*.astrq.io.vn" or "https://blog.example.com".
    frame_ancestors: list[str] = Field(
        default_factory=lambda: ["'self'", "https://astrq.io.vn", "https://*.astrq.io.vn"]
    )
    # When set, every request except /health must carry header X-Astrq-Origin: <secret>.
    # Add it with a Cloudflare Transform Rule on your domain so the host's default URL
    # (e.g. *.onrender.com) cannot bypass Cloudflare Access.
    origin_secret: SecretStr | None = None
    # Allow enabling/disabling tools from the web UI/API. Turn off on public deployments
    # (no login) so visitors cannot switch the radar off; Telegram /on /off still works.
    allow_tool_control: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ASTRQ_", env_nested_delimiter="__", extra="ignore"
    )

    root_dir: Path = Path(".")
    log_level: str = "INFO"
    provider: ProviderConfig = Field(default_factory=ProviderConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    sessions: SessionsConfig = Field(default_factory=SessionsConfig)
    market_rules: MarketRulesConfig = Field(default_factory=MarketRulesConfig)
    bus: BusConfig = Field(default_factory=BusConfig)
    tools: dict[str, ToolEntry] = Field(default_factory=dict)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    context: ContextConfig = Field(default_factory=ContextConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)

    @model_validator(mode="after")
    def _check(self) -> Settings:
        tg = self.alerts.telegram
        if tg.enabled and (tg.bot_token is None or not tg.chat_id):
            raise ValueError(
                "alerts.telegram.enabled requires ASTRQ_ALERTS__TELEGRAM__BOT_TOKEN "
                "and ASTRQ_ALERTS__TELEGRAM__CHAT_ID"
            )
        if self.provider.kind == "ssi":
            ssi = self.provider.ssi
            if not ssi.consumer_id or ssi.consumer_secret is None:
                raise ValueError(
                    "provider.kind=ssi requires ASTRQ_PROVIDER__SSI__CONSUMER_ID and "
                    "ASTRQ_PROVIDER__SSI__CONSUMER_SECRET (create them on SSI iBoard)"
                )
        r = self.storage.remote
        if r.kind == "r2" and not (
            r.endpoint and r.bucket and r.access_key_id and r.secret_access_key
        ):
            raise ValueError(
                "storage.remote.kind=r2 requires ASTRQ_STORAGE__REMOTE__ENDPOINT, __BUCKET, "
                "__ACCESS_KEY_ID and __SECRET_ACCESS_KEY"
            )
        if self.provider.kind == "dnse":
            d = self.provider.dnse
            if not d.api_key or d.api_secret is None:
                raise ValueError(
                    "provider.kind=dnse requires ASTRQ_PROVIDER__DNSE__API_KEY and "
                    "ASTRQ_PROVIDER__DNSE__API_SECRET (EntradeX → Thông tin tài khoản → "
                    "LightSpeed API)"
                )
        if self.provider.kind in ("ssi", "dnse") and self.context.kind == "synthetic":
            raise ValueError(
                "context.kind=synthetic only matches the synthetic market; with a real "
                "feed set ASTRQ_CONTEXT__KIND=recorded (or none)"
            )
        return self

    def path(self, rel: str) -> Path:
        p = Path(rel)
        return p if p.is_absolute() else (self.root_dir / p).resolve()


def load_settings(
    config_path: str | os.PathLike[str] | None = None, use_dotenv: bool = True, **overrides: Any
) -> Settings:
    """Load settings. Priority: overrides > process env > <root>/.env > YAML file.

    YAML defaults to ./config/settings.yaml (or $ASTRQ_CONFIG); <root> is the
    directory containing config/.
    """
    raw = config_path or os.environ.get("ASTRQ_CONFIG") or "config/settings.yaml"
    path = Path(raw).resolve()
    root = path.parent.parent if path.parent.name == "config" else path.parent
    env_file = root / ".env"

    class _FileSettings(Settings):
        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings]
            if use_dotenv and env_file.exists():
                sources.append(DotEnvSettingsSource(settings_cls, env_file=env_file))
            if path.exists():
                sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=path))
            return tuple(sources)

    overrides.setdefault("root_dir", root)
    return _FileSettings(**overrides)
