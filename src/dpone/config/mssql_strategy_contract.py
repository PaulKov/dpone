"""Public facade for the typed SQL Server load-strategy capability contract.

The implementation is split into a normalization engine, immutable models,
and common policy parsing. Lazy facade exports keep callers stable while
avoiding dependency triangles between those cohesive modules.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "BackfillPolicy": "dpone.config.mssql_strategy_contract_models",
    "FullRefreshPolicy": "dpone.config.mssql_strategy_contract_models",
    "IncrementalAppendPolicy": "dpone.config.mssql_strategy_contract_models",
    "IncrementalMergePolicy": "dpone.config.mssql_strategy_contract_models",
    "MSSQLLoadStrategyContract": "dpone.config.mssql_strategy_contract_models",
    "MSSQLStrategyContractError": "dpone.config.mssql_strategy_contract_error",
    "PartitionReplacePolicy": "dpone.config.mssql_strategy_contract_models",
    "ReplacePolicy": "dpone.config.mssql_strategy_contract_models",
    "SCD2Policy": "dpone.config.mssql_strategy_contract_models",
    "SnapshotDiffPolicy": "dpone.config.mssql_strategy_contract_models",
    "normalize_mssql_authoring_strategy": "dpone.config.mssql_strategy_contract_engine",
    "normalize_mssql_backfill_campaign_strategy": "dpone.config.mssql_strategy_contract_engine",
    "normalize_mssql_load_strategy": "dpone.config.mssql_strategy_contract_engine",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


__all__ = sorted(_EXPORTS)
