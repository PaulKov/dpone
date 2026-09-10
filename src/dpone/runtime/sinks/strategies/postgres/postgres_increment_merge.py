"""Стратегия MERGE для PostgreSQL (удалить+вставить)."""

from __future__ import annotations

import uuid

from psycopg import sql

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import (
    MergePolicy,
    require_unique_key,
    resolve_merge_policy,
    validate_duplicate_policy,
    validate_partition_replace,
)
from dpone.runtime.sinks.strategies.postgres.native_partition_replace import PostgresNativePartitionReplacer
from dpone.runtime.sinks.strategies.postgres.postgres_base import PostgresStrategyBase


class PostgresIncrementMergeStrategy(PostgresStrategyBase):
    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)
        merge_policy = resolve_merge_policy(load_config, "postgres")

        def handler(staging):
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_created = self._ensure_target_table(load_config, payload.schema)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

            if merge_policy == MergePolicy.SHADOW_SWAP:
                return self._load_with_shadow_swap(load_config, staging, payload, unique_key)

            if merge_policy != MergePolicy.DELETE_INSERT:
                raise ValueError(f"Unsupported PostgreSQL merge_policy: {merge_policy}")

            deleted = self._delete_existing(load_config, staging, unique_key)
            inserted = self._insert_from_staging(load_config, staging, payload.schema)
            net_new_rows = max(0, inserted - deleted)

            return LoadResult(
                inserted_rows=net_new_rows,  # Только реально новые записи
                updated_rows=deleted,  # Обновлённые (удалены старые + вставлены новые версии)
                total_rows=inserted,  # Всего вставлено в таблицу
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _delete_existing(self, load_config, staging, unique_key) -> int:
        condition = self._build_key_condition("s", "t", unique_key)

        delete_sql = sql.SQL(
            """
            DELETE FROM {}.{} AS t
            WHERE EXISTS (
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

        return self.connector.execute_query(delete_sql)

    def _load_with_shadow_swap(self, load_config, staging, payload: LoadPayload, unique_key) -> LoadResult:
        updated = self._count_target_matching_staging_keys(load_config, staging, unique_key)
        suffix = uuid.uuid4().hex[:8]
        shadow_table = f"{load_config.target_table}__dpone_shadow_{suffix}"
        backup_table = f"{load_config.target_table}__dpone_backup_{suffix}"

        self.connector.execute_query(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(shadow_table),
            )
        )
        self.connector.execute_query(
            sql.SQL("CREATE TABLE {}.{} (LIKE {}.{} INCLUDING ALL)").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(shadow_table),
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
            )
        )
        self._copy_target_excluding_staging_keys(load_config, staging, shadow_table, unique_key)
        inserted = self._insert_from_staging_to_table(load_config, staging, payload.schema, shadow_table)

        self.connector.execute_query(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(backup_table),
            )
        )
        self.connector.execute_query(
            sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
                sql.Identifier(backup_table),
            )
        )
        self.connector.execute_query(
            sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(shadow_table),
                sql.Identifier(load_config.target_table),
            )
        )
        self.connector.execute_query(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(backup_table),
            )
        )

        return LoadResult(
            inserted_rows=max(0, inserted - updated),
            updated_rows=updated,
            total_rows=self._count_target(load_config),
        )

    def _copy_target_excluding_staging_keys(self, load_config, staging, shadow_table: str, unique_key) -> int:
        condition = self._build_key_condition("s", "t", unique_key)
        query = sql.SQL(
            """
            INSERT INTO {}.{}
            SELECT t.*
            FROM {}.{} AS t
            WHERE NOT EXISTS (
                SELECT 1 FROM {}.{} AS s
                WHERE {}
            )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(shadow_table),
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            condition,
        )
        return self.connector.execute_query(query)

    def _insert_from_staging_to_table(self, load_config, staging, schema, destination_table: str) -> int:
        data_columns = [column for column, _ in schema if column != "__dpone__xmin"]
        columns_sql = sql.SQL(", ").join(sql.Identifier(column) for column in data_columns)
        select_sql = sql.SQL(", ").join(sql.Identifier(column) for column in data_columns)
        query = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {}
            FROM {}.{}
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(destination_table),
            columns_sql,
            select_sql,
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
        )
        return self.connector.execute_query(query)

    def _count_target_matching_staging_keys(self, load_config, staging, unique_key) -> int:
        condition = self._build_key_condition("s", "t", unique_key)
        rows = self.connector.get_records(
            sql.SQL(
                """
                SELECT COUNT(*) FROM {}.{} AS t
                WHERE EXISTS (
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
        return int(rows[0][0]) if rows else 0

    def _validate_staging_duplicates(self, load_config, staging, unique_key) -> None:
        validate_duplicate_policy(load_config)
        columns = sql.SQL(", ").join(sql.Identifier(column) for column in unique_key)
        rows = self.connector.get_records(
            sql.SQL(
                """
                SELECT COUNT(*) FROM (
                    SELECT {}
                    FROM {}.{}
                    GROUP BY {}
                    HAVING COUNT(*) > 1
                    LIMIT 1
                ) AS duplicate_keys
                """
            ).format(
                columns,
                sql.Identifier(staging.schema),
                sql.Identifier(staging.table),
                columns,
            )
        )
        duplicate_count = int(rows[0][0]) if rows else 0
        if duplicate_count:
            raise ValueError(
                "PostgreSQL incremental_merge staging contains duplicate unique_key values. "
                "Default duplicate_policy=fail rejected the batch before target mutation."
            )

    def _count_target(self, load_config) -> int:
        rows = self.connector.get_records(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(load_config.target_schema),
                sql.Identifier(load_config.target_table),
            )
        )
        return int(rows[0][0]) if rows else 0


class PostgresPartitionReplaceStrategy(PostgresIncrementMergeStrategy):
    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        partition = validate_partition_replace(load_config, "postgres")

        def handler(staging):
            target_created = self._ensure_target_table(load_config, payload.schema)
            if target_created:
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

            native_result = PostgresNativePartitionReplacer(self.connector, self.logger).try_replace(
                load_config, staging, payload, partition
            )
            if native_result is not None:
                return native_result
            if partition.require_native:
                raise ValueError(
                    "PostgreSQL partition_replace native_mode=required could not use declarative "
                    "partition DETACH/ATTACH. Ensure target is partitioned, staging contains existing "
                    "partition values, and the runtime can resolve existing partition bounds."
                )

            deleted = self._delete_matching_partition_values(load_config, staging, partition.column)
            inserted = self._insert_from_staging(load_config, staging, payload.schema)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=self._count_target(load_config),
                replaced_rows=inserted,
                hard_deleted_rows=deleted,
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _delete_matching_partition_values(self, load_config, staging, partition_column: str) -> int:
        column = sql.Identifier(partition_column)
        query = sql.SQL(
            """
            DELETE FROM {}.{} AS t
            WHERE EXISTS (
                SELECT 1 FROM {}.{} AS s
                WHERE s.{}::text IS NOT DISTINCT FROM t.{}::text
            )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            column,
            column,
        )
        return self.connector.execute_query(query)
