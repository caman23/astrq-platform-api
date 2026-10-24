"""Event/signal bus.

`InMemoryBus` fans out to bounded per-subscriber queues. Publishing never
blocks: a slow subscriber overflows its own queue (counted as `dropped`) instead
of stalling the bus or other subscribers. A Redis Streams implementation can be
added behind the same interface (see docs/adr/0002-event-bus.md).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Generic, Protocol, TypeVar

from astrq.core.metrics import METRICS

T = TypeVar("T")


class Subscriber(Generic[T]):
    def __init__(self, name: str, predicate: Callable[[T], bool], maxsize: int) -> None:
        self.name = name
        self.predicate = predicate
        self.queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    async def get(self) -> T:
        return await self.queue.get()

    def get_nowait(self) -> T:
        return self.queue.get_nowait()

    @property
    def lag(self) -> int:
        return self.queue.qsize()


class Bus(Protocol[T]):
    def publish(self, item: T) -> None: ...

    def subscribe(
        self, name: str, predicate: Callable[[T], bool] | None = None, maxsize: int | None = None
    ) -> Subscriber[T]: ...

    def unsubscribe(self, sub: Subscriber[T]) -> None: ...


class InMemoryBus(Generic[T]):
    def __init__(self, name: str, default_maxsize: int = 10_000) -> None:
        self.name = name
        self.default_maxsize = default_maxsize
        self._subs: list[Subscriber[T]] = []

    def publish(self, item: T) -> None:
        for sub in self._subs:
            if not sub.predicate(item):
                continue
            try:
                sub.queue.put_nowait(item)
            except asyncio.QueueFull:
                sub.dropped += 1
                METRICS.inc("bus_dropped_total", bus=self.name, subscriber=sub.name)

    def subscribe(
        self, name: str, predicate: Callable[[T], bool] | None = None, maxsize: int | None = None
    ) -> Subscriber[T]:
        sub: Subscriber[T] = Subscriber(
            name, predicate or (lambda _x: True), maxsize or self.default_maxsize
        )
        self._subs.append(sub)
        return sub

    def unsubscribe(self, sub: Subscriber[T]) -> None:
        if sub in self._subs:
            self._subs.remove(sub)
