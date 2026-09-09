"""Compatibility facade for canonical dbt runtime execution components."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "MAX_DBT_EXECUTION_PACK_BYTES": "dpone.runtime.dbt_execution_bootstrap",
    "execute_dbt_pack": "dpone.runtime.dbt_execution_bootstrap",
    "MAX_DBT_RUN_RESULTS_BYTES": "dpone.runtime.dbt_execution_service",
    "DbtExecutionInterval": "dpone.runtime.dbt_execution_service",
    "DbtExecutionService": "dpone.runtime.dbt_execution_service",
}
__all__ = sorted(_EXPORT_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    return getattr(import_module(module_name), name)
