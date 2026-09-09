"""Production PostgreSQL strategies: snapshot_diff and SCD2."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from psycopg import sql

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import require_unique_key
from dpone.runtime.sinks.strategies.postgres.postgres_increment_merge import PostgresIncrementMergeStrategy


class PostgresSnapshotDiffStrategy(PostgresIncrementMergeStrategy):
    """Converges target to a complete source snapshot through staging."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)

        def handler(staging):
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

            deleted = self._apply_target_only_delete_policy(load_config, staging, unique_key)
            updated = self._count_target_matching_staging_keys(load_config, staging, unique_key)
            self._delete_existing(load_config, staging, unique_key)
            inserted = self._insert_from_staging(load_config, staging, payload.schema)
            return LoadResult(
                inserted_rows=max(0, inserted - updated),
                updated_rows=updated,
                total_rows=self._count_target(load_config),
                soft_deleted_rows=deleted if self._delete_policy(load_config) == "soft_delete" else None,
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _apply_target_only_delete_policy(self, load_config: Any, staging: Any, unique_key: str | Sequence[str]) -> int:
        policy = self._delete_policy(load_config)
        if policy == "ignore":
            return 0
        if policy == "soft_delete":
            return self._soft_delete_target_only_keys(load_config, staging, unique_key)
        return self._hard_delete_target_only_keys(load_config, staging, unique_key)

    def _delete_policy(self, load_config: Any) -> str:
        return str(((getattr(load_config, "options", {}) or {}).get("diff") or {}).get("delete_policy", "hard_delete"))

    def _hard_delete_target_only_keys(self, load_config: Any, staging: Any, unique_key: str | Sequence[str]) -> int:
        condition = self._build_key_condition("s", "t", unique_key)
        return self.connector.execute_query(
            sql.SQL(
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
        )

    def _soft_delete_target_only_keys(self, load_config: Any, staging: Any, unique_key: str | Sequence[str]) -> int:
        condition = self._build_key_condition("s", "t", unique_key)
        deleted_at = TechnicalColumnCatalog().name(TechnicalColumnRole.DELETED_AT)
        return self.connector.execute_query(
            sql.SQL(
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
        )


class PostgresSCD2Strategy(PostgresIncrementMergeStrategy):
    """Maintains SCD Type 2 history through staged set-based SQL."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)

        def handler(staging):
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

            expired = self._expire_changed_current_rows(load_config, staging, unique_key)
            deleted = self._expire_deleted_current_rows(load_config, staging, unique_key)
            inserted = self._insert_new_current_versions(load_config, staging, payload.schema, unique_key)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=expired + deleted,
                total_rows=self._count_target(load_config),
                soft_deleted_rows=deleted,
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _expire_changed_current_rows(self, load_config: Any, staging: Any, unique_key: str | Sequence[str]) -> int:
        c = TechnicalColumnCatalog()
        condition = self._build_key_condition("s", "t", unique_key)
        return self.connector.execute_query(
            sql.SQL(
                """
                UPDATE {}.{} AS t
                SET {} = timezone('utc', now()),
                    {} = false
                WHERE {} = true
                  AND EXISTS (
                    SELECT 1 FROM {}.{} AS s
                    WHERE {} AND s.{} <> t.{}
                  )
                """
            ).format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
                sql.Identifier(c.name(TechnicalColumnRole.VALID_TO_AT)),
                sql.Identifier(c.name(TechnicalColumnRole.IS_CURRENT)),
                sql.Identifier(c.name(TechnicalColumnRole.IS_CURRENT)),
                sql.Identifier(staging.schema),
                sql.Identifier(staging.table),
                condition,
                sql.Identifier(c.name(TechnicalColumnRole.ROW_HASH)),
                sql.Identifier(c.name(TechnicalColumnRole.ROW_HASH)),
            )
        )

    def _expire_deleted_current_rows(self, load_config: Any, staging: Any, unique_key: str | Sequence[str]) -> int:
        if self._scd2_delete_policy(load_config) != "expire":
            return 0
        c = TechnicalColumnCatalog()
        condition = self._build_key_condition("s", "t", unique_key)
        return self.connector.execute_query(
            sql.SQL(
                """
                UPDATE {}.{} AS t
                SET {} = timezone('utc', now()),
                    {} = false
                WHERE {} = true
                  AND NOT EXISTS (
                    SELECT 1 FROM {}.{} AS s
                    WHERE {}
                  )
                """
            ).format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
                sql.Identifier(c.name(TechnicalColumnRole.VALID_TO_AT)),
                sql.Identifier(c.name(TechnicalColumnRole.IS_CURRENT)),
                sql.Identifier(c.name(TechnicalColumnRole.IS_CURRENT)),
                sql.Identifier(staging.schema),
                sql.Identifier(staging.table),
                condition,
            )
        )

    def _insert_new_current_versions(
        self,
        load_config: Any,
        staging: Any,
        schema: Sequence[tuple[str, str]],
        unique_key: str | Sequence[str],
    ) -> int:
        c = TechnicalColumnCatalog()
        condition = self._build_key_condition("s", "t", unique_key)
        data_columns = [column for column, _ in schema if column != "__dpone__xmin"]
        columns_sql = sql.SQL(", ").join(sql.Identifier(column) for column in data_columns)
        select_sql = sql.SQL(", ").join(sql.Identifier("s", column) for column in data_columns)
        return self.connector.execute_query(
            sql.SQL(
                """
                INSERT INTO {}.{} ({})
                SELECT {}
                FROM {}.{} AS s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {}.{} AS t
                    WHERE {} = true
                      AND {}
                      AND t.{} = s.{}
                )
                """
            ).format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
                columns_sql,
                select_sql,
                sql.Identifier(staging.schema),
                sql.Identifier(staging.table),
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
                sql.Identifier(c.name(TechnicalColumnRole.IS_CURRENT)),
                condition,
                sql.Identifier(c.name(TechnicalColumnRole.ROW_HASH)),
                sql.Identifier(c.name(TechnicalColumnRole.ROW_HASH)),
            )
        )

    def _scd2_delete_policy(self, load_config: Any) -> str:
        return str(((getattr(load_config, "options", {}) or {}).get("scd2") or {}).get("delete_policy", "expire"))
