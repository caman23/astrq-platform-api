"""Radar score: weighted, normalized factors with a full explanation."""

from __future__ import annotations

import math
from dataclasses import dataclass

from astrq.core.context import Baseline
from astrq.core.models import Contribution, Severity, Side
from astrq.tools.large_order_radar.config import RadarConfig

LABELS = {
    "value": "Giá trị",
    "relative": "Độ lớn so với bình thường",
    "velocity": "Tốc độ (giá trị/phút)",
    "concentration": "Độ tập trung lệnh lớn",
    "impact": "Tác động giá",
    "side_confidence": "Độ tin cậy chiều",
}


def _clamp(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def _log_ratio(x: float, base: float, span: float) -> float:
    """0 at x == base, 1 at x == base * span (log scale)."""
    if x <= 0 or base <= 0:
        return 0.0
    return _clamp(math.log(x / base) / math.log(span))


@dataclass(frozen=True)
class ScoreInput:
    total_value: float
    duration_sec: float
    side: Side
    first_price: float
    last_price: float
    side_confidence: float
    concentration: float
    baseline: Baseline | None


@dataclass(frozen=True)
class ScoreResult:
    score: float
    severity: Severity
    explanation: list[Contribution]


def score(inp: ScoreInput, cfg: RadarConfig, factors: set[str] | None = None) -> ScoreResult:
    """`factors` restricts which factors apply (e.g. auctions only use value/relative)."""
    w = cfg.weights.model_dump()
    raw: dict[str, tuple[float, float, str]] = {}  # factor -> (raw, normalized, note)

    raw["value"] = (
        inp.total_value,
        _log_ratio(inp.total_value, cfg.abs_threshold_value, 20),
        "0 tại ngưỡng tuyệt đối, 1 tại 20× ngưỡng",
    )
    if inp.baseline is not None and inp.baseline.median_trade_value > 0:
        ratio = inp.total_value / inp.baseline.median_trade_value
        raw["relative"] = (ratio, _log_ratio(ratio, 1.0, 500), f"{ratio:.0f}× trung vị lệnh")
    else:
        raw["relative"] = (0.0, 0.0, "thiếu baseline")
    minutes = max(inp.duration_sec / 60, 1.0)
    vpm = inp.total_value / minutes
    raw["velocity"] = (vpm, _log_ratio(vpm, cfg.abs_threshold_value, 20), "giá trị mỗi phút")
    raw["concentration"] = (
        inp.concentration,
        _clamp(inp.concentration),
        "tỷ trọng lệnh lớn trong giá trị chủ động cùng chiều",
    )
    move = (inp.last_price - inp.first_price) / inp.first_price * 100 if inp.first_price else 0.0
    direction = 1 if inp.side is Side.BUY else -1 if inp.side is Side.SELL else 0
    raw["impact"] = (move, _clamp(direction * move / 1.0), "1% theo chiều lệnh = tối đa")
    raw["side_confidence"] = (inp.side_confidence, _clamp(inp.side_confidence), "")

    active = factors or set(raw)
    total_w = sum(w[f] for f in active) or 1.0
    explanation: list[Contribution] = []
    acc = 0.0
    for f, (r, n, note) in raw.items():
        if f not in active:
            continue
        c = 100 * w[f] * n / total_w
        acc += c
        explanation.append(
            Contribution(
                factor=f,
                label=LABELS[f],
                raw=round(r, 4),
                normalized=round(n, 4),
                weight=w[f],
                contribution=round(c, 2),
                note=note,
            )
        )
    s = round(acc, 1)
    sev = (
        Severity.ALERT
        if s >= cfg.alert_score
        else Severity.NOTICE
        if s >= cfg.notice_score
        else Severity.INFO
    )
    return ScoreResult(score=s, severity=sev, explanation=explanation)
