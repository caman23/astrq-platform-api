"""Asynchronous JSONL recorder (ticks, signals). Never blocks the hot path:
writes go through a bounded queue and are flushed in a background task.
Recorded tick files are directly usable by `ReplayProvider`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import date, datetime
from pathlib import Path

from pydantic import BaseModel

from astrq.core.metrics import METRICS
from astrq.core.sessions import SessionCalendar

log = logging.getLogger(__name__)


class JsonlRecorder:
    def __init__(self, directory: Path, calendar: SessionCalendar, kind: str) -> None:
        self.dir = directory
        self.calendar = calendar
        self.kind = kind
        self.queue: asyncio.Queue[tuple[date, str]] = asyncio.Queue(maxsize=200_000)
        self._task: asyncio.Task[None] | None = None

    def write(self, item: BaseModel, ts: datetime) -> None:
        self.write_line(item.model_dump_json(), ts)

    def write_line(self, line: str, ts: datetime) -> None:
        """Append one pre-serialized line (e.g. a raw vendor message)."""
        try:
            self.queue.put_nowait((self.calendar.trading_date(ts), line.replace("\n", " ")))
        except asyncio.QueueFull:
            METRICS.inc("recorder_dropped_total", kind=self.kind)

    def start(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.create_task(self._run(), name=f"recorder-{self.kind}")

    def file_for(self, d: date) -> Path:
        return self.dir / f"{d.isoformat()}.jsonl"

    async def _run(self) -> None:
        while True:
            first = await self.queue.get()
            batch = [first]
            while len(batch) < 5000:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await asyncio.to_thread(self._flush, batch)

    def _flush(self, batch: list[tuple[date, str]]) -> None:
        by_day: dict[date, list[str]] = {}
        for d, line in batch:
            by_day.setdefault(d, []).append(line)
        for d, lines in by_day.items():
            with self.file_for(d).open("a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")

    async def close(self) -> None:
        if self._task is not None:
            # drain what is left, then stop
            rest: list[tuple[date, str]] = []
            while not self.queue.empty():
                rest.append(self.queue.get_nowait())
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            if rest:
                self._flush(rest)

    def cleanup(self, keep_days: int) -> int:
        """Delete files older than keep_days (by file mtime). Returns count removed."""
        cutoff = time.time() - keep_days * 86400
        removed = 0
        for f in self.dir.glob("*.jsonl"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        return removed
