"""Canonical vendor staging manager implementations."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["BigQueryStagingManager", "PostgresStagingManager"]

_EXPORTS: dict[str, str] = {
    "BigQueryStagingManager": "dpone.runtime.sinks.staging_managers.bigquery:BigQueryStagingManager",
    "PostgresStagingManager": "dpone.runtime.sinks.staging_managers.postgres:PostgresStagingManager",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
