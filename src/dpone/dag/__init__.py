"""DAG / dependency subsystem for dpone.

This package contains:

- dependency graph building (from dpone manifests)
- "why" explanations for DAG edges and nodes
- DAG reports (edges + reasons + anomalies + lint)

IMPORTANT
---------
Some runtime parts of dpone (sources/sinks/credentials) require optional
dependencies (e.g. GCP SDKs). To keep CLI tooling usable in minimal
environments, we keep this package import-light and resolve heavy symbols
**lazily**.

Canonical import path is :mod:`dpone.dag`.
Legacy import path :mod:`dpone.yaml_config_handler` is kept as a deprecated shim.
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

# name -> "module:attribute"
_EXPORTS: dict[str, str] = {
    "ETLProcessConfig": "dpone.dag.config:ETLProcessConfig",
    "ETLProcess": "dpone.dag.process:ETLProcess",
    "DependencyManager": "dpone.dag.manager:DependencyManager",
    "ProcessNode": "dpone.dag.yaml_types:ProcessNode",
    "DependencyConfig": "dpone.dag.yaml_types:DependencyConfig",
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
