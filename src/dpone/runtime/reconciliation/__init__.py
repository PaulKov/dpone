"""Reconciliation subsystem.

Reconciliation relies on BigQuery tech tables and thus may require optional
heavyweight dependencies.

We export symbols **lazily**.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ReconciliationService",
    "ReconciliationManager",
]

_EXPORTS: dict[str, str] = {
    "ReconciliationService": "dpone.runtime.reconciliation.base:ReconciliationService",
    "ReconciliationManager": "dpone.runtime.reconciliation.manager:ReconciliationManager",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value  # cache
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
