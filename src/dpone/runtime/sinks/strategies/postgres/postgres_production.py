"""Production-grade PostgreSQL finalizers for diff/history load strategies."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from psycopg import sql

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import require_unique_key
from dpone.runtime.sinks.strategies.postgres.postgres_increment_merge import PostgresIncrementMergeStrategy


class PostgresSnapshotDiffStrategy(PostgresIncrementMergeStrategy):
    """Apply a full snapshot diff using staged, set-based PostgreSQL SQL."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)
        delete_policy = _strategy_option(load_config, "diff", "delete_policy", default="hard_delete")

        def handler(staging: StagingTableArtifact) -> LoadResult:
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            deleted_missing = self._apply_missing_key_policy(load_config, staging, unique_key, delete_policy)
            updated = self._count_target_matching_staging_keys(load_config, staging, unique_key)
            self._delete_existing(load_config, staging, unique_key)
            inserted = self._insert_from_staging(load_config, staging, payload.schema)
            return LoadResult(
                inserted_rows=max(0, inserted - updated),
                updated_rows=updated,
                total_rows=self._count_target(load_config),
                hard_deleted_rows=deleted_missing if delete_policy == "hard_delete" else 0,
                soft_deleted_rows=deleted_missing if delete_policy == "soft_delete" else 0,
            )

        return self._consume_with_staging(load_config, payload, handler)

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
            "Unsupported PostgreSQL snapshot_diff delete_policy. Supported values: hard_delete, soft_delete, ignore."
        )

    def _delete_target_missing_from_staging(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        condition = self._build_key_condition("s", "t", unique_key)
        query = sql.SQL(
            """
            DELETE FROM {}.{} AS t
            WHERE NOT EXISTS (
                SELECT 1 FROM {}.{} AS s
                WHERE {}
            )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            condition,
        )
        return self.connector.execute_query(query)

    def _soft_delete_target_missing_from_staging(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        condition = self._build_key_condition("s", "t", unique_key)
        deleted_at = TechnicalColumnCatalog().name(TechnicalColumnRole.DELETED_AT)
        query = sql.SQL(
            """
            UPDATE {}.{} AS t
            SET {} = timezone('utc', now())
            WHERE {} IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {}.{} AS s
                  WHERE {}
              )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.Identifier(deleted_at),
            sql.Identifier(deleted_at),
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            condition,
        )
        return self.connector.execute_query(query)


class PostgresSCD2Strategy(PostgresIncrementMergeStrategy):
    """Maintain a PostgreSQL type-2 history table with staged SQL."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)
        columns = _Scd2Columns.from_config(load_config)

        def handler(staging: StagingTableArtifact) -> LoadResult:
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            expired_changed = self._expire_changed_current_rows(load_config, staging, unique_key, columns)
            expired_deleted = self._apply_delete_policy(load_config, staging, unique_key, columns)
            inserted = self._insert_new_current_versions(load_config, staging, payload.schema, unique_key, columns)
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
        condition = self._build_key_condition("s", "t", unique_key)
        query = sql.SQL(
            """
            UPDATE {}.{} AS t
            SET {} = COALESCE(s.{}, timezone('utc', now())),
                {} = FALSE
            FROM {}.{} AS s
            WHERE {}
              AND t.{} IS TRUE
              AND s.{} IS DISTINCT FROM t.{}
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.Identifier(columns.valid_to),
            sql.Identifier(columns.valid_from),
            sql.Identifier(columns.is_current),
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            condition,
            sql.Identifier(columns.is_current),
            sql.Identifier(columns.row_hash),
            sql.Identifier(columns.row_hash),
        )
        return self.connector.execute_query(query)

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
            raise ValueError("PostgreSQL scd2 supports delete_policy values: expire, ignore.")
        condition = self._build_key_condition("s", "t", unique_key)
        query = sql.SQL(
            """
            UPDATE {}.{} AS t
            SET {} = timezone('utc', now()),
                {} = FALSE
            WHERE t.{} IS TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM {}.{} AS s
                  WHERE {}
              )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.Identifier(columns.valid_to),
            sql.Identifier(columns.is_current),
            sql.Identifier(columns.is_current),
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            condition,
        )
        return self.connector.execute_query(query)

    def _insert_new_current_versions(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        schema: Sequence[tuple[str, str]],
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        data_columns = [column for column, _ in schema if column != "__dpone__xmin"]
        column_list = sql.SQL(", ").join(sql.Identifier(column) for column in data_columns)
        select_list = sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(column)) for column in data_columns)
        condition = self._build_key_condition("s", "t", unique_key)
        query = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {}
            FROM {}.{} AS s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {}.{} AS t
                WHERE {}
                  AND t.{} IS TRUE
                  AND t.{} IS NOT DISTINCT FROM s.{}
            )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            column_list,
            select_list,
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            condition,
            sql.Identifier(columns.is_current),
            sql.Identifier(columns.row_hash),
            sql.Identifier(columns.row_hash),
        )
        return self.connector.execute_query(query)


class _Scd2Columns:
    def __init__(
        self,
        *,
        valid_from: str,
        valid_to: str,
        is_current: str,
        row_hash: str,
        delete_policy: str,
    ) -> None:
        self.valid_from = valid_from
        self.valid_to = valid_to
        self.is_current = is_current
        self.row_hash = row_hash
        self.delete_policy = delete_policy

    @classmethod
    def from_config(cls, load_config: Any) -> _Scd2Columns:
        options = _strategy_options(load_config, "scd2")
        catalog = TechnicalColumnCatalog()
        return cls(
            valid_from=str(options.get("valid_from_column", catalog.name(TechnicalColumnRole.VALID_FROM_AT))),
            valid_to=str(options.get("valid_to_column", catalog.name(TechnicalColumnRole.VALID_TO_AT))),
            is_current=str(options.get("current_flag_column", catalog.name(TechnicalColumnRole.IS_CURRENT))),
            row_hash=str(options.get("row_hash_column", catalog.name(TechnicalColumnRole.ROW_HASH))),
            delete_policy=str(options.get("delete_policy", "expire")),
        )


def _strategy_options(load_config: Any, section: str) -> dict[str, Any]:
    options = getattr(load_config, "options", {}) or {}
    nested = options.get(section)
    return nested if isinstance(nested, dict) else {}


def _strategy_option(load_config: Any, section: str, key: str, *, default: str) -> str:
    return str(_strategy_options(load_config, section).get(key, default))
