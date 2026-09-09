"""Core orchestration primitives for dpone.

IMPORTANT
---------
This package is imported by lightweight CLI commands (manifest lint/validate/explain)
that should work even when optional runtime dependencies (e.g. GCP SDKs) are not
installed.

Therefore we keep imports LAZY here.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ETLProcessConfig",
    "ETLProcess",
    "RunContext",
    "ETLConfigurationError",
    "ETLProcessError",
    "build_dag_from_yaml",
    "validate_dependencies",
    "get_dependency_tree",
]

# name -> "module:attribute"
_EXPORTS: dict[str, str] = {
    "ETLProcessConfig": "dpone.dag:ETLProcessConfig",
    "ETLProcess": "dpone.dag:ETLProcess",
    "RunContext": "dpone.core.runtime:RunContext",
    "ETLConfigurationError": "dpone.core.errors:ETLConfigurationError",
    "ETLProcessError": "dpone.core.errors:ETLProcessError",
    "build_dag_from_yaml": "dpone.core.dag_builder:build_dag_from_yaml",
    "validate_dependencies": "dpone.core.dag_builder:validate_dependencies",
    "get_dependency_tree": "dpone.core.dag_builder:get_dependency_tree",
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
