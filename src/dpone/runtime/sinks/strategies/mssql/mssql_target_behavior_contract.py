"""Pure SQL Server target DML-behaviour authority and connector adapter."""

from __future__ import annotations

from typing import Any

from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.mssql_target_behavior_policy import (
    MssqlTargetBehaviorError,
    mssql_target_behavior_blockers,
    require_ordinary_mssql_target_behavior,
)
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot


class MssqlTargetBehaviorContract:
    """Catalog-owner adapter retained for XMin and direct strategy contracts."""

    def __init__(self, catalog_owner: Any) -> None:
        self._catalog_owner = catalog_owner

    def validate(self, load_config: Any) -> None:
        snapshot = read_schema_catalog_snapshot(self._catalog_owner, load_config)
        if not snapshot.exists:
            _raise("target_object_missing")
        try:
            require_ordinary_mssql_target_behavior(
                snapshot,
                strategy=normalize_mssql_load_strategy(load_config),
            )
        except MssqlTargetBehaviorError as exc:
            raise SnapshotReconciliationError(str(exc)) from exc


def _raise(suffix: str) -> None:
    raise SnapshotReconciliationError(f"mssql_target_contract.{suffix}")


__all__ = [
    "MssqlTargetBehaviorContract",
    "mssql_target_behavior_blockers",
    "require_ordinary_mssql_target_behavior",
]
