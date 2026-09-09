"""Production-grade SQL Server finalizers for diff/history load strategies."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import staging_scalar_expression
from dpone.runtime.sinks.strategies.mssql.mssql_strategies import MSSQLIncrementMergeStrategy
from dpone.runtime.support.mssql_native_canonical import null_safe_difference_expression


class MSSQLSnapshotDiffStrategy(MSSQLIncrementMergeStrategy):
    """Apply a full snapshot diff to SQL Server with staging-first SQL."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        contract = normalize_mssql_load_strategy(load_config)
        policy = contract.snapshot_diff
        assert policy is not None
        unique_key = policy.unique_key
        delete_policy = policy.delete_policy

        def handler(staging: StagingTableArtifact) -> LoadResult:
            _require_nonempty_destructive_snapshot(
                staging,
                destructive=delete_policy != "ignore",
                code="mssql.strategy.snapshot_diff.empty_destructive_snapshot",
            )
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema, staging=staging)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging)
                return LoadResult(
                    inserted_rows=inserted,
                    updated_rows=0,
                    unchanged_rows=0,
                    total_rows=self._count_target(load_config),
                )

            deleted_missing = self._apply_missing_key_policy(load_config, staging, unique_key, delete_policy)
            changed = self._change_predicate(staging, unique_key, policy.compare)
            reactivated = self._count_reactivated(load_config, staging, unique_key)
            updated = self._update_target_from_staging(
                load_config,
                staging,
                unique_key,
                where=changed,
            )
            inserted = self._insert_missing_staging_rows(load_config, staging, unique_key)
            unchanged = max(0, staging.row_count - updated - inserted)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=updated,
                unchanged_rows=unchanged,
                reactivated_rows=reactivated,
                total_rows=self._count_target(load_config),
                replaced_rows=deleted_missing if delete_policy == "hard_delete" else 0,
                hard_deleted_rows=deleted_missing if delete_policy == "hard_delete" else 0,
                soft_deleted_rows=deleted_missing if delete_policy == "soft_delete" else 0,
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _change_predicate(
        self,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
        compare: str,
    ) -> str:
        catalog = TechnicalColumnCatalog()
        deleted_at = self.connector.quote_identifier(catalog.name(TechnicalColumnRole.DELETED_AT))
        reactivation = f"t.{deleted_at} IS NOT NULL"
        if compare == "row_hash":
            row_hash = self.connector.quote_identifier(catalog.name(TechnicalColumnRole.ROW_HASH))
            return (
                f"({reactivation} OR "
                f"ISNULL(CONVERT(nvarchar(max), t.{row_hash}), N'') <> "
                f"ISNULL(CONVERT(nvarchar(max), s.{row_hash}), N''))"
            )
        if compare != "all_columns":
            raise ValueError(f"Unsupported MSSQL snapshot_diff compare mode: {compare}")
        keys = set([unique_key] if isinstance(unique_key, str) else unique_key)
        columns = [
            column
            for column in self._data_columns(staging.columns)
            if column not in keys and not column.startswith("__dpone__")
        ]
        differences: list[str] = []
        for column in columns:
            quoted = self.connector.quote_identifier(column)
            source = staging_scalar_expression(self, staging, column, "s")
            differences.append(
                null_safe_difference_expression(
                    f"t.{quoted}",
                    source,
                    staging.target_column_types[column],
                )
            )
        return "(" + " OR ".join((reactivation, *differences)) + ")"

    def _count_reactivated(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        deleted_at = self.connector.quote_identifier(TechnicalColumnCatalog().name(TechnicalColumnRole.DELETED_AT))
        rows = self.connector.get_records(
            f"SELECT COUNT_BIG(*) FROM {self._target_name(load_config)} AS t "
            f"INNER JOIN {self._staging_name(staging)} AS s "
            f"ON {self._key_condition(staging, 's', 't', unique_key)} "
            f"WHERE t.{deleted_at} IS NOT NULL"
        )
        return int(rows[0][0]) if rows else 0

    def _apply_missing_key_policy(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
        delete_policy: str,
    ) -> int:
        if delete_policy == "ignore":
            return 0
        if delete_policy == "hard_delete":
            return self._delete_target_missing_from_staging(load_config, staging, unique_key)
        if delete_policy == "soft_delete":
            return self._soft_delete_target_missing_from_staging(load_config, staging, unique_key)
        raise ValueError(
            "Unsupported MSSQL snapshot_diff delete_policy. Supported values: hard_delete, soft_delete, ignore."
        )

    def _delete_target_missing_from_staging(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        return self._execute_counted_dml(
            f"DELETE t FROM {self._target_name(load_config)} AS t "
            f"WHERE NOT EXISTS (SELECT 1 FROM {self._staging_name(staging)} AS s "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )

    def _soft_delete_target_missing_from_staging(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        deleted_at = self.connector.quote_identifier(TechnicalColumnCatalog().name(TechnicalColumnRole.DELETED_AT))
        return self._execute_counted_dml(
            f"UPDATE t SET {deleted_at} = SYSUTCDATETIME() "
            f"FROM {self._target_name(load_config)} AS t "
            f"WHERE t.{deleted_at} IS NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {self._staging_name(staging)} AS s "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )


class MSSQLSCD2Strategy(MSSQLIncrementMergeStrategy):
    """Maintain SQL Server SCD2 history with set-based staged finalization."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        contract = normalize_mssql_load_strategy(load_config)
        policy = contract.scd2
        assert policy is not None
        unique_key = policy.unique_key
        columns = _Scd2Columns.from_policy(policy)

        def handler(staging: StagingTableArtifact) -> LoadResult:
            _require_nonempty_destructive_snapshot(
                staging,
                destructive=columns.delete_policy == "expire",
                code="mssql.strategy.scd2.empty_destructive_snapshot",
            )
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema, staging=staging)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            expired_changed = self._expire_changed_current_rows(load_config, staging, unique_key, columns)
            expired_deleted = self._apply_delete_policy(load_config, staging, unique_key, columns)
            inserted = self._insert_new_current_versions(load_config, staging, unique_key, columns)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=expired_changed + expired_deleted,
                total_rows=self._count_target(load_config),
                soft_deleted_rows=expired_deleted,
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _expire_changed_current_rows(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        valid_to = self.connector.quote_identifier(columns.valid_to)
        is_current = self.connector.quote_identifier(columns.is_current)
        row_hash = self.connector.quote_identifier(columns.row_hash)
        return self._execute_counted_dml(
            f"UPDATE t SET {valid_to} = SYSUTCDATETIME(), {is_current} = 0 "
            f"FROM {self._target_name(load_config)} AS t "
            f"INNER JOIN {self._staging_name(staging)} AS s "
            f"ON {self._key_condition(staging, 's', 't', unique_key)} "
            f"WHERE t.{is_current} = 1 AND ISNULL(CONVERT(nvarchar(max), s.{row_hash}), '') "
            f"<> ISNULL(CONVERT(nvarchar(max), t.{row_hash}), '')"
        )

    def _apply_delete_policy(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        if columns.delete_policy == "ignore":
            return 0
        if columns.delete_policy != "expire":
            raise ValueError("MSSQL scd2 supports delete_policy values: expire, ignore.")
        valid_to = self.connector.quote_identifier(columns.valid_to)
        is_current = self.connector.quote_identifier(columns.is_current)
        return self._execute_counted_dml(
            f"UPDATE t SET {valid_to} = SYSUTCDATETIME(), {is_current} = 0 "
            f"FROM {self._target_name(load_config)} AS t "
            f"WHERE t.{is_current} = 1 "
            f"AND NOT EXISTS (SELECT 1 FROM {self._staging_name(staging)} AS s "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )

    def _insert_new_current_versions(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        data_columns = self._data_columns(staging.columns)
        target_columns = ", ".join(self.connector.quote_identifier(column) for column in data_columns)
        source_columns = ", ".join(self._staging_select_expression(staging, column, "s") for column in data_columns)
        is_current = self.connector.quote_identifier(columns.is_current)
        row_hash = self.connector.quote_identifier(columns.row_hash)
        return self._execute_counted_dml(
            f"INSERT INTO {self._target_name(load_config)} ({target_columns}) "
            f"SELECT {source_columns} FROM {self._staging_name(staging)} AS s "
            f"WHERE NOT EXISTS ("
            f"SELECT 1 FROM {self._target_name(load_config)} AS t "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)} "
            f"AND t.{is_current} = 1 "
            f"AND ISNULL(CONVERT(nvarchar(max), t.{row_hash}), '') = ISNULL(CONVERT(nvarchar(max), s.{row_hash}), '')"
            f")"
        )


class _Scd2Columns:
    def __init__(
        self,
        *,
        valid_to: str,
        is_current: str,
        row_hash: str,
        delete_policy: str,
    ) -> None:
        self.valid_to = valid_to
        self.is_current = is_current
        self.row_hash = row_hash
        self.delete_policy = delete_policy

    @classmethod
    def from_policy(cls, policy: Any) -> _Scd2Columns:
        return cls(
            valid_to=policy.valid_to_column,
            is_current=policy.current_flag_column,
            row_hash=policy.row_hash_column,
            delete_policy=policy.delete_policy,
        )


def _require_nonempty_destructive_snapshot(
    staging: StagingTableArtifact,
    *,
    destructive: bool,
    code: str,
) -> None:
    """Require explicit future repair authority before an all-delete action."""

    if destructive and staging.row_count == 0:
        raise SnapshotReconciliationError(code)
