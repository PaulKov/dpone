from __future__ import annotations

from importlib import import_module
from typing import Any

_BASE_HOOK_MODULES = (
    "airflow.sdk.bases.hook",
    "airflow.hooks.base",
)


def load_airflow_base_hook() -> type[Any] | None:
    """Load Airflow BaseHook without touching deprecated paths on Airflow 3."""
    for module_name in _BASE_HOOK_MODULES:
        try:
            module = import_module(module_name)
        except ImportError:
            continue
        return getattr(module, "BaseHook", None)
    return None
