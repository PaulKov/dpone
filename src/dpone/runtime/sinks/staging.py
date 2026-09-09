"""Backward-compatible facade for staging managers.

This module stays import-safe in environments without optional PostgreSQL
dependencies by resolving staging managers lazily.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.sinks.staging_managers.bigquery import BigQueryStagingManager
    from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager

__all__ = ["PostgresStagingManager", "BigQueryStagingManager"]

_EXPORTS: dict[str, str] = {
    "BigQueryStagingManager": "dpone.runtime.sinks.staging_managers.bigquery:BigQueryStagingManager",
    "PostgresStagingManager": "dpone.runtime.sinks.staging_managers.postgres:PostgresStagingManager",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
