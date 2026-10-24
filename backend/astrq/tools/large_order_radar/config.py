"""Radar configuration. Every threshold is configurable; defaults are starting points."""

from __future__ import annotations

from pydantic import BaseModel, Field

from astrq.core.models import Severity


class ScoreWeights(BaseModel):
    value: float = 0.28
    relative: float = 0.24
    velocity: float = 0.14
    concentration: float = 0.12
    impact: float = 0.12
    side_confidence: float = 0.10


class RadarConfig(BaseModel):
    # --- what counts as a "large" single trade
    abs_threshold_value: float = Field(500e6, description="Ngưỡng tuyệt đối (VND)")
    relative_k: float = Field(10.0, description="Lớn nếu ≥ k × trung vị giá trị lệnh của mã")
    tier_relative_k: dict[str, float] = Field(
        default_factory=lambda: {"high": 12.0, "mid": 10.0, "low": 8.0},
        description="k theo nhóm thanh khoản",
    )
    relative_min_value: float = Field(150e6, description="Sàn giá trị cho ngưỡng tương đối")

    # --- which trades may be children of a parent order
    child_min_value: float = Field(100e6, description="Lệnh con tối thiểu khi chưa có baseline")
    child_relative_k: float = Field(4.0, description="Lệnh con ≥ k × trung vị (khi có baseline)")
    child_floor_value: float = Field(50e6, description="Sàn giá trị lệnh con")

    # --- parent order grouping
    group_gap_sec: float = Field(30.0, description="Khoảng cách tối đa giữa hai lệnh con")
    price_band_ticks: int = Field(3, description="Dải giá cho phép (số bước giá)")
    group_open_value: float = Field(1.5e9, description="Ngưỡng mở nhóm tối đa / khi thiếu baseline")
    group_relative_k: float = Field(25.0, description="Ngưỡng mở nhóm = k × trung vị (kẹp)")
    update_step_ratio: float = Field(0.5, description="Phát UPDATE khi tổng tăng thêm tỷ lệ này")
    idle_close_sec: float = Field(90.0, description="Đóng nhóm sau khoảng nhàn rỗi")
    emit_close: bool = True
    max_trades_in_features: int = 40

    # --- auction / put-through
    auction_min_value: float = Field(2e9, description="Ngưỡng lệnh lớn trong ATO/ATC")
    auction_relative_k: float = 60.0
    putthrough_min_value: float = Field(1e9, description="Ngưỡng hiển thị thỏa thuận")
    putthrough_max_severity: Severity = Field(
        Severity.NOTICE, description="Mức độ tối đa của tín hiệu thỏa thuận"
    )

    # --- per-symbol metrics & classification
    metrics_window_sec: float = 900.0
    quote_stale_sec: float = 15.0

    # --- scoring
    weights: ScoreWeights = Field(default_factory=ScoreWeights)
    notice_score: float = 40.0
    alert_score: float = 65.0

    # --- later versions
    absorption_enabled: bool = Field(False, description="Tín hiệu hấp thụ (chưa bật)")
