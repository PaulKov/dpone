"""Compatibility facade for the canonical runtime dbt profile adapter."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "TemporaryDbtProfileStore": "dpone.adapters.dbt_runtime_profile",
}
__all__ = sorted(_EXPORT_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    return getattr(import_module(module_name), name)
