"""Scan universe (VN100) loaded from a file and reloadable at runtime."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from astrq.core.models import Instrument

log = logging.getLogger(__name__)


class UniverseFile(BaseModel):
    name: str
    as_of: str | None = None
    source: str | None = None
    verified: bool = False
    instruments: list[Instrument] = Field(default_factory=list)


class Universe:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._mtime = 0.0
        self.meta = UniverseFile(name="empty")
        self.instruments: dict[str, Instrument] = {}
        self.load()

    @classmethod
    def from_instruments(cls, name: str, instruments: list[Instrument]) -> Universe:
        u = cls.__new__(cls)
        u.path = Path("/dev/null")
        u._mtime = 0.0
        u.meta = UniverseFile(name=name, instruments=instruments, verified=False)
        u.instruments = {i.symbol: i for i in instruments}
        return u

    @property
    def symbols(self) -> list[str]:
        return list(self.instruments)

    def load(self) -> None:
        data = yaml.safe_load(self.path.read_text(encoding="utf-8"))
        meta = UniverseFile.model_validate(data)
        self.meta = meta
        self.instruments = {i.symbol.upper(): i for i in meta.instruments}
        self._mtime = self.path.stat().st_mtime
        if not meta.verified:
            log.warning(
                "universe %s loaded from %s is NOT verified against the official list",
                meta.name,
                self.path,
            )
        log.info("universe %s: %d instruments", meta.name, len(self.instruments))

    def maybe_reload(self) -> bool:
        """Reload when the file changed. Returns True if membership changed."""
        try:
            mtime = self.path.stat().st_mtime
        except OSError:
            return False
        if mtime == self._mtime:
            return False
        before = set(self.instruments)
        self.load()
        return before != set(self.instruments)

    def get(self, symbol: str) -> Instrument | None:
        return self.instruments.get(symbol)

    def __contains__(self, symbol: object) -> bool:
        return symbol in self.instruments
