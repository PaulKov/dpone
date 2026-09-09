"""Target-local SQL mutations and parity probes for snapshot finalization."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import staging_scalar_expression
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import (
    SnapshotReconciliationError,
    SnapshotReconciliationSql,
    SoftDeleteMode,
)


class MssqlSnapshotMutationExecutor:
    """Execute target DML and count proofs on one injected connector."""

    def __init__(self, strategy: Any, connector: Any) -> None:
        self._strategy = strategy
        self._connector = connector

    def insert_absent(
        self,
        load_config: Any,
        delta: StagingTableArtifact,
        unique_key: Sequence[str],
        business: Sequence[str],
        row_hash: str,
        renderer: SnapshotReconciliationSql,
        run_id: Any,
        load_id: Any,
        extracted_at: Any,
        effective_at: Any,
    ) -> int:
        """Insert source keys that are absent from the target."""

        target = self._strategy._target_name(load_config)
        columns = [
            *business,
            "__dpone__run_id",
            "__dpone__load_id",
            "__dpone__row_hash",
            "__dpone__extracted_at",
            "__dpone__loaded_at",
        ]
        values = [staging_scalar_expression(self._strategy, delta, column, "s") for column in business]
        values.extend(("?", "?", row_hash, "?", "?"))
        if renderer.policy.mode == SoftDeleteMode.FLAG_ONLY:
            columns.append("__dpone__is_deleted")
            values.append("0")
        quoted = ", ".join(self._connector.quote_identifier(column) for column in columns)
        sql = (
            "SET NOCOUNT ON; DECLARE @actions TABLE ([action] nvarchar(32) NOT NULL); "
            f"INSERT INTO {target} ({quoted}) OUTPUT N'inserted' INTO @actions "
            f"SELECT {', '.join(values)} FROM {self._strategy._staging_name(delta)} AS s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {target} AS t WHERE {renderer.key_condition('s', 't', unique_key)}); "
            "SELECT COUNT_BIG(*) AS action_count FROM @actions;"
        )
        return self.action_count(sql, (run_id, load_id, extracted_at, effective_at))

    def assert_key_parity(
        self,
        target: str,
        keys: str,
        unique_key: Sequence[str],
        renderer: SnapshotReconciliationSql,
    ) -> None:
        """Fail closed unless active target keys exactly equal source keys."""

        source_only = self.count(
            keys,
            f"NOT EXISTS (SELECT 1 FROM {target} AS t WHERE {renderer.active_predicate('t')} "
            f"AND {renderer.key_condition('k', 't', unique_key)})",
            alias="k",
        )
        target_only = self.count(
            target,
            f"{renderer.active_predicate('t')} AND NOT EXISTS (SELECT 1 FROM {keys} AS k WHERE "
            f"{renderer.key_condition('k', 't', unique_key)})",
            alias="t",
        )
        if source_only or target_only:
            raise SnapshotReconciliationError(
                "mssql_snapshot_reconciliation.key_drift",
                full_repair_required=True,
            )

    def missing_active_count(
        self,
        target: str,
        keys: str,
        unique_key: Sequence[str],
        renderer: SnapshotReconciliationSql,
    ) -> int:
        """Count active target keys missing from the source snapshot."""

        return self.count(
            target,
            f"{renderer.active_predicate('t')} AND NOT EXISTS (SELECT 1 FROM {keys} AS k WHERE "
            f"{renderer.key_condition('k', 't', unique_key)})",
            alias="t",
        )

    def count(self, table: str, predicate: str | None = None, *, alias: str | None = None) -> int:
        """Execute a canonical ``COUNT_BIG`` query."""

        from_sql = f"{table} AS {alias}" if alias else table
        where = f" WHERE {predicate}" if predicate else ""
        rows = self._connector.get_records(f"SELECT COUNT_BIG(*) AS row_count FROM {from_sql}{where}", as_dict=True)
        return scalar(rows, "row_count")

    def action_count(self, sql: str, params: tuple[Any, ...]) -> int:
        """Return the exact action count emitted by one DML statement."""

        return scalar(self._connector.get_records(sql, params, as_dict=True), "action_count")


def scalar(rows: Any, name: str) -> int:
    """Read an integer scalar from dict- or tuple-shaped connector rows."""

    if not rows:
        return 0
    row = rows[0]
    return int(row[name] if isinstance(row, dict) else row[0])


__all__ = ["MssqlSnapshotMutationExecutor", "scalar"]
