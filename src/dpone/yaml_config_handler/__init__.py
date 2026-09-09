"""DEPRECATED package.

The DAG/dependency subsystem has moved to :mod:`dpone.dag`.

This package is kept for backward compatibility and will be removed in a future release.

We keep imports **lazy** to avoid pulling optional runtime dependencies in lightweight
CLI tools (docs/metrics/help).
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ETLProcessConfig",
    "ETLProcess",
    "DependencyManager",
    "ProcessNode",
    "DependencyConfig",
]

# name -> "module:attribute" (delegates to dpone.dag)
_EXPORTS: dict[str, str] = {
    "ETLProcessConfig": "dpone.dag:ETLProcessConfig",
    "ETLProcess": "dpone.dag:ETLProcess",
    "DependencyManager": "dpone.dag:DependencyManager",
    "ProcessNode": "dpone.dag:ProcessNode",
    "DependencyConfig": "dpone.dag:DependencyConfig",
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
