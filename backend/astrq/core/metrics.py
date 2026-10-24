"""Tiny in-process metrics registry with Prometheus text exposition."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

LabelKey = tuple[str, tuple[tuple[str, str], ...]]


def _key(name: str, labels: dict[str, Any]) -> LabelKey:
    return name, tuple(sorted((k, str(v)) for k, v in labels.items()))


@dataclass
class _Summary:
    count: int = 0
    total: float = 0.0
    maximum: float = 0.0
    recent: deque[float] = field(default_factory=lambda: deque(maxlen=512))

    def observe(self, v: float) -> None:
        self.count += 1
        self.total += v
        self.maximum = max(self.maximum, v)
        self.recent.append(v)

    def quantile(self, q: float) -> float:
        if not self.recent:
            return 0.0
        data = sorted(self.recent)
        return data[min(len(data) - 1, int(q * len(data)))]


class Metrics:
    def __init__(self) -> None:
        self.counters: dict[LabelKey, float] = {}
        self.gauges: dict[LabelKey, float] = {}
        self.summaries: dict[LabelKey, _Summary] = {}

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        k = _key(name, labels)
        self.counters[k] = self.counters.get(k, 0.0) + value

    def set(self, name: str, value: float, **labels: Any) -> None:
        self.gauges[_key(name, labels)] = value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        self.summaries.setdefault(_key(name, labels), _Summary()).observe(value)

    def counter(self, name: str, **labels: Any) -> float:
        return self.counters.get(_key(name, labels), 0.0)

    def summary(self, name: str, **labels: Any) -> dict[str, float]:
        s = self.summaries.get(_key(name, labels))
        if s is None:
            return {"count": 0, "p50": 0.0, "p95": 0.0, "max": 0.0}
        return {"count": s.count, "p50": s.quantile(0.5), "p95": s.quantile(0.95), "max": s.maximum}

    def reset(self) -> None:
        self.counters.clear()
        self.gauges.clear()
        self.summaries.clear()

    def render_prometheus(self) -> str:
        def fmt(name: str, labels: tuple[tuple[str, str], ...], extra: str = "") -> str:
            parts = [f'{k}="{v}"' for k, v in labels]
            if extra:
                parts.append(extra)
            return f"astrq_{name}{{{','.join(parts)}}}" if parts else f"astrq_{name}"

        lines: list[str] = []
        for (n, lb), v in sorted(self.counters.items()):
            lines.append(f"{fmt(n, lb)} {v}")
        for (n, lb), v in sorted(self.gauges.items()):
            lines.append(f"{fmt(n, lb)} {v}")
        for (n, lb), s in sorted(self.summaries.items(), key=lambda kv: kv[0]):
            for q in (0.5, 0.95):
                lines.append(f"{fmt(n, lb, f'quantile="{q}"')} {s.quantile(q)}")
            lines.append(f"{fmt(n + '_count', lb)} {s.count}")
            lines.append(f"{fmt(n + '_sum', lb)} {s.total}")
        return "\n".join(lines) + "\n"


METRICS = Metrics()
