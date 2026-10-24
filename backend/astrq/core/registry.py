"""Tool registry: discovers tools from config (`module:Class`) and validates them."""

from __future__ import annotations

import importlib
import logging

from pydantic import BaseModel

from astrq.core.config import ToolEntry
from astrq.core.tool import Tool

log = logging.getLogger(__name__)


class _EmptyConfig(BaseModel):
    pass


def load_tool(tool_id: str, entry: ToolEntry) -> Tool:
    module_name, _, attr = entry.module.partition(":")
    if not attr:
        raise ValueError(f"tool {tool_id}: module must look like 'pkg.mod:Class'")
    cls = getattr(importlib.import_module(module_name), attr)
    cfg_model: type[BaseModel] = getattr(cls, "config_model", _EmptyConfig)
    cfg = cfg_model.model_validate(entry.config)  # fails fast at startup on bad config
    tool = cls(cfg)
    if not isinstance(tool, Tool):
        raise TypeError(f"tool {tool_id}: {entry.module} does not implement the Tool contract")
    if tool.id != tool_id:
        raise ValueError(f"tool key '{tool_id}' does not match tool.id '{tool.id}'")
    return tool
