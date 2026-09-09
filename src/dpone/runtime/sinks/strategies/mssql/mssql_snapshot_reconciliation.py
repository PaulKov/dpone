"""SQL and metric contracts for MSSQL complete-key reconciliation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from dpone.contracts.soft_delete import SoftDeleteMode, SoftDeletePolicy
from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError


@dataclass(frozen=True, slots=True)
class SnapshotActionMetrics:
    """Disjoint target lifecycle counters for one source snapshot."""

    inserted: int
    updated: int
    reactivated: int
    unchanged: int
    soft_deleted: int
    hard_deleted: int
    active: int
    total: int
    staging: int
    snapshot_effective_at: datetime | None = None

    @classmethod
    def from_action_counts(
        cls,
        *,
        inserted: int,
        updated: int,
        reactivated: int,
        soft_deleted: int,
        hard_deleted: int,
        active: int,
        total: int,
        staging: int,
        snapshot_effective_at: datetime | None = None,
    ) -> SnapshotActionMetrics:
        counts = (inserted, updated, reactivated, soft_deleted, hard_deleted, active, total, staging)
        changed_active = inserted + updated + reactivated
        if any(value < 0 for value in counts) or active > total or changed_active > active:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.metric_invariant")
        return cls(
            inserted=inserted,
            updated=updated,
            reactivated=reactivated,
            unchanged=active - changed_active,
            soft_deleted=soft_deleted,
            hard_deleted=hard_deleted,
            active=active,
            total=total,
            staging=staging,
            snapshot_effective_at=snapshot_effective_at,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.snapshot_effective_at is not None:
            payload["snapshot_effective_at"] = self.snapshot_effective_at.isoformat()
        return payload


class SnapshotReconciliationSql:
    """Render set-based DML without SQL Server ``MERGE``."""

    def __init__(self, quote_identifier: Callable[[str], str], policy: SoftDeletePolicy) -> None:
        self._quote = quote_identifier
        self.policy = policy
        catalog = TechnicalColumnCatalog()
        self.deleted_at = self._quote(catalog.name(TechnicalColumnRole.DELETED_AT))
        self.is_deleted = self._quote(catalog.name(TechnicalColumnRole.IS_DELETED))
        self.row_hash = self._quote(catalog.name(TechnicalColumnRole.ROW_HASH))
        self.loaded_at = self._quote(catalog.name(TechnicalColumnRole.LOADED_AT))

    def active_predicate(self, alias: str) -> str:
        if self.policy.timestamp_is_source_of_truth:
            return f"{alias}.{self.deleted_at} IS NULL"
        return f"{alias}.{self.is_deleted} = 0"

    def deleted_predicate(self, alias: str) -> str:
        if self.policy.timestamp_is_source_of_truth:
            return f"{alias}.{self.deleted_at} IS NOT NULL"
        return f"{alias}.{self.is_deleted} = 1"

    def soft_delete_missing(self, *, target: str, keys: str, unique_key: tuple[str, ...]) -> str:
        assignment = f"{self.deleted_at} = ?" if self.policy.timestamp_is_source_of_truth else f"{self.is_deleted} = 1"
        dml = (
            f"UPDATE t SET {assignment} OUTPUT N'soft_deleted' INTO @actions "
            f"FROM {target} AS t WHERE {self.active_predicate('t')} "
            f"AND NOT EXISTS (SELECT 1 FROM {keys} AS k WHERE {self.key_condition('k', 't', unique_key)});"
        )
        return _action_count_sql(dml)

    def reactivate(
        self,
        *,
        target: str,
        delta: str,
        unique_key: tuple[str, ...],
        assignments: tuple[str, ...],
        row_hash_expression: str,
    ) -> str:
        active_assignment = (
            f"{self.deleted_at} = NULL" if self.policy.timestamp_is_source_of_truth else f"{self.is_deleted} = 0"
        )
        values = (*assignments, f"{self.row_hash} = {row_hash_expression}", f"{self.loaded_at} = ?", active_assignment)
        dml = (
            f"UPDATE t SET {', '.join(values)} OUTPUT N'reactivated' INTO @actions "
            f"FROM {target} AS t INNER JOIN {delta} AS s "
            f"ON {self.key_condition('s', 't', unique_key)} WHERE {self.deleted_predicate('t')};"
        )
        return _action_count_sql(dml)

    def update_changed(
        self,
        *,
        target: str,
        delta: str,
        unique_key: tuple[str, ...],
        assignments: tuple[str, ...],
        row_hash_expression: str,
    ) -> str:
        values = (*assignments, f"{self.row_hash} = {row_hash_expression}", f"{self.loaded_at} = ?")
        dml = (
            f"UPDATE t SET {', '.join(values)} OUTPUT N'updated' INTO @actions "
            f"FROM {target} AS t INNER JOIN {delta} AS s "
            f"ON {self.key_condition('s', 't', unique_key)} "
            f"WHERE {self.active_predicate('t')} "
            f"AND (t.{self.row_hash} IS NULL OR t.{self.row_hash} <> {row_hash_expression});"
        )
        return _action_count_sql(dml)

    def key_condition(self, left: str, right: str, unique_key: tuple[str, ...]) -> str:
        return " AND ".join(f"{left}.{self._quote(key)} = {right}.{self._quote(key)}" for key in unique_key)


def _action_count_sql(dml: str) -> str:
    return (
        "SET NOCOUNT ON; DECLARE @actions TABLE ([action] nvarchar(32) NOT NULL); "
        f"{dml} SELECT COUNT_BIG(*) AS action_count FROM @actions;"
    )


__all__ = [
    "SnapshotActionMetrics",
    "SnapshotReconciliationError",
    "SnapshotReconciliationSql",
    "SoftDeleteMode",
    "SoftDeletePolicy",
]
