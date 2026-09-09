"""Стратегия MERGE для BigQuery (удалить+вставить)."""

from __future__ import annotations

import uuid

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import (
    MergePolicy,
    require_unique_key,
    resolve_merge_policy,
    validate_duplicate_policy,
    validate_partition_replace,
)
from dpone.runtime.sinks.strategies.bigquery.bigquery_base import BigQueryStrategyBase
from dpone.runtime.sinks.strategies.bigquery.bigquery_job_config import BigQueryJobConfigFactory


class BigQueryIncrementMergeStrategy(BigQueryStrategyBase):
    """Стратегия INCREMENTAL_MERGE для BigQuery.

    Поведение:
    1. Если таблица не существует - создает её и вставляет все данные
    2. Если существует:
       a. Удаляет строки из целевой таблицы по unique_key
       b. Вставляет новые данные из staging

    Используется для:
    - Инкрементальных обновлений (append + update)
    """

    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с merge стратегией."""
        unique_key = require_unique_key(load_config)
        merge_policy = resolve_merge_policy(load_config, "bigquery")
        columns = [col for col, _ in payload.schema]
        fq_staging = f"`{self.connector.project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"
        self._validate_staging_duplicates(load_config, fq_staging, unique_key)

        # 1. Создаем целевую таблицу если нужно
        target_created = self._create_target_table_if_not_exists(load_config, payload.schema)
        if not target_created and merge_policy == MergePolicy.SHADOW_SWAP:
            return self._load_with_shadow_swap(load_config, payload, fq_staging, unique_key)
        if merge_policy != MergePolicy.DELETE_INSERT:
            raise ValueError(f"Unsupported BigQuery merge_policy: {merge_policy}")

        # 2. Если таблица уже существует - удаляем старые строки по unique_key
        deleted = 0
        if not target_created:
            deleted = self._delete_existing(load_config, fq_staging, unique_key)

        # 3. Вставляем новые данные
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)

        # Строим SELECT с PARSE_JSON() для JSON колонок
        include_tech = self._include_technical_columns(load_config)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=include_tech,
        )

        query = f"""
        INSERT INTO {fq_target} ({columns_str})
        SELECT {select_str}
        FROM {fq_staging}
        """

        self.logger.log_etl_progress(
            "BQ_INCREMENT_MERGE",
            {
                "Target": fq_target,
                "Action": "DELETE BY KEY + INSERT",
                "Unique_Key": str(load_config.unique_key),
                "Deleted_Rows": deleted,
                "Columns": len(columns),
            },
        )

        inserted = self._execute_dml_query(query)

        if inserted and inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
            self._log_target_sample(fq_target, load_config.log_sample_rows)

        net_new_rows = max(0, (inserted or 0) - deleted)

        return LoadResult(
            inserted_rows=net_new_rows,  # Только реально новые записи
            updated_rows=deleted,  # Обновлённые (удалены старые + вставлены новые версии)
            total_rows=inserted or 0,  # Всего вставлено в таблицу
            deleted_lookback_rows=0,  # INCREMENTAL_MERGE не использует lookback deletion (это для INCREMENTAL_APPEND)
        )

    def _load_with_shadow_swap(
        self,
        load_config,
        payload: LoadPayload,
        fq_staging: str,
        unique_key,
    ) -> LoadResult:
        suffix = uuid.uuid4().hex[:8]
        shadow_table = f"{load_config.target_table}__dpone_shadow_{suffix}"
        backup_table = f"{load_config.target_table}__dpone_backup_{suffix}"
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        fq_shadow = self._get_fq_table(load_config.target_schema, shadow_table)
        condition = self._build_unique_key_condition("s", "t", unique_key)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=self._include_technical_columns(load_config),
        )
        deleted = self._count_target_matching_staging_keys(load_config, fq_staging, unique_key)

        self._execute_dml_query(
            f"""
            CREATE TABLE {fq_shadow} AS
            SELECT t.*
            FROM {fq_target} AS t
            WHERE NOT EXISTS (
                SELECT 1 FROM {fq_staging} AS s
                WHERE {condition}
            )
            """
        )
        inserted = self._execute_dml_query(
            f"""
            INSERT INTO {fq_shadow} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
        )
        self._rename_table(load_config, load_config.target_table, backup_table)
        self._rename_table(load_config, shadow_table, load_config.target_table)
        self._drop_table(load_config, backup_table)

        return LoadResult(
            inserted_rows=max(0, (inserted or 0) - deleted),
            updated_rows=deleted,
            total_rows=inserted or 0,
            deleted_lookback_rows=0,
        )

    def _validate_staging_duplicates(self, load_config, fq_staging: str, unique_key) -> None:
        validate_duplicate_policy(load_config)
        key_sql = ", ".join(f"`{column}`" for column in unique_key)
        rows = self.connector.get_records(
            f"""
            SELECT COUNT(*) AS duplicate_count
            FROM (
                SELECT {key_sql}
                FROM {fq_staging}
                GROUP BY {key_sql}
                HAVING COUNT(*) > 1
                LIMIT 1
            )
            """
        )
        if not rows:
            return
        first = rows[0]
        duplicate_count = int(first.get("duplicate_count", 0)) if isinstance(first, dict) else int(first[0])
        if duplicate_count:
            raise ValueError(
                "BigQuery incremental_merge staging contains duplicate unique_key values. "
                "Default duplicate_policy=fail rejected the batch before target mutation."
            )

    def _count_target_matching_staging_keys(self, load_config, fq_staging: str, unique_key) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        condition = self._build_unique_key_condition("s", "t", unique_key)
        rows = self.connector.get_records(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {fq_target} AS t
            WHERE EXISTS (
                SELECT 1 FROM {fq_staging} AS s
                WHERE {condition}
            )
            """
        )
        if not rows:
            return 0
        first = rows[0]
        if isinstance(first, dict):
            return int(first.get("row_count", 0))
        return int(first[0])


class BigQueryPartitionReplaceStrategy(BigQueryIncrementMergeStrategy):
    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        partition = validate_partition_replace(load_config, "bigquery")
        fq_staging = f"`{self.connector.project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"
        target_created = self._create_target_table_if_not_exists(load_config, payload.schema)
        if target_created:
            inserted = self._insert_partition_rows(load_config, payload, fq_staging)
            return LoadResult(inserted_rows=inserted or 0, updated_rows=0, total_rows=inserted or 0)

        native_result = self._try_native_partition_overwrite(load_config, payload, fq_staging, partition)
        if native_result is not None:
            return native_result
        if partition.require_native:
            raise ValueError(
                "BigQuery partition_replace native_mode=required could not use partition decorator "
                "WRITE_TRUNCATE. Ensure the target is time-partitioned and partition values can be "
                "converted to YYYYMMDD decorators."
            )

        replaced = self._delete_partition_values(load_config, fq_staging, partition.column)
        inserted = self._insert_partition_rows(load_config, payload, fq_staging)
        return LoadResult(
            inserted_rows=inserted or 0,
            updated_rows=0,
            total_rows=inserted or 0,
            replaced_rows=replaced or 0,
        )

    def _try_native_partition_overwrite(self, load_config, payload: LoadPayload, fq_staging: str, partition):
        if not partition.native:
            return None
        client = getattr(self.connector, "connection", None)
        if client is None or not hasattr(client, "query") or not hasattr(client, "get_table"):
            self._warn_native_partition_fallback(load_config, "BigQuery client query/get_table API is unavailable")
            return None

        table_id = f"{self.connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
        try:
            target_table = client.get_table(table_id)
        except Exception as exc:
            self._warn_native_partition_fallback(load_config, f"target table metadata unavailable: {exc}")
            return None

        time_partitioning = getattr(target_table, "time_partitioning", None)
        if time_partitioning is None:
            self._warn_native_partition_fallback(load_config, "target table is not time-partitioned")
            return None
        # Decorator WRITE_TRUNCATE is only correct for column-partitioned tables whose
        # partition field matches partition.column. Ingestion-time partitioning (no field)
        # would truncate a different physical partition than the business-date window.
        partition_field = getattr(time_partitioning, "field", None)
        if not partition_field or str(partition_field) != str(partition.column):
            self._warn_native_partition_fallback(
                load_config,
                "native partition decorator overwrite requires time partitioning on "
                f"partition.column={partition.column!r} (got field={partition_field!r})",
            )
            return None

        values = self._partition_values_from_staging(fq_staging, partition.column)
        if len(values) > partition.max_partitions_per_run:
            raise ValueError(
                "BigQuery partition_replace would replace "
                f"{len(values)} partitions, above max_partitions_per_run={partition.max_partitions_per_run}."
            )
        decorators = [(value, self._partition_decorator(value)) for value in values]
        if any(decorator is None for _, decorator in decorators):
            self._warn_native_partition_fallback(
                load_config,
                "one or more partition values cannot be converted to a BigQuery time partition decorator",
            )
            return None

        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=self._include_technical_columns(load_config),
        )
        del columns_str
        inserted_total = 0
        job_config_factory = BigQueryJobConfigFactory()
        for value, decorator in decorators:
            destination = f"{table_id}${decorator}"
            job_config = job_config_factory.query_job_config(
                destination=destination,
                partition_value=str(value),
            )
            query = f"""
            SELECT {select_str}
            FROM {fq_staging}
            WHERE CAST(`{partition.column}` AS STRING) = @partition_value
            """
            job = client.query(query, job_config=job_config)
            job.result()
            inserted_total += self._count_staging_partition_rows(fq_staging, partition.column, value)

        return LoadResult(
            inserted_rows=inserted_total,
            updated_rows=0,
            total_rows=inserted_total,
            replaced_rows=len(decorators),
        )

    def _insert_partition_rows(self, load_config, payload: LoadPayload, fq_staging: str) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=self._include_technical_columns(load_config),
        )
        return self._execute_dml_query(
            f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
        )

    def _delete_partition_values(self, load_config, fq_staging: str, partition_column: str) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        column = f"`{partition_column}`"
        return self._execute_dml_query(
            f"""
            DELETE FROM {fq_target}
            WHERE CAST({column} AS STRING) IN (
                SELECT DISTINCT CAST({column} AS STRING)
                FROM {fq_staging}
            )
            """
        )

    def _partition_values_from_staging(self, fq_staging: str, partition_column: str) -> list[str]:
        rows = self.connector.get_records(
            f"""
            SELECT DISTINCT CAST(`{partition_column}` AS STRING) AS partition_value
            FROM {fq_staging}
            """
        )
        values: list[str] = []
        for row in rows:
            if isinstance(row, dict):
                values.append(str(row["partition_value"]))
            else:
                values.append(str(row[0]))
        return values

    def _count_staging_partition_rows(self, fq_staging: str, partition_column: str, partition_value: str) -> int:
        value_literal = str(partition_value).replace("\\", "\\\\").replace("'", "\\'")
        rows = self.connector.get_records(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {fq_staging}
            WHERE CAST(`{partition_column}` AS STRING) = '{value_literal}'
            """
        )
        if not rows:
            return 0
        first = rows[0]
        return int(first.get("row_count", 0)) if isinstance(first, dict) else int(first[0])

    @staticmethod
    def _partition_decorator(value: str) -> str | None:
        normalized = str(value).strip()
        if not normalized:
            return None
        date_part = normalized.split("T", 1)[0].split(" ", 1)[0]
        if len(date_part) == 10 and date_part[4] == "-" and date_part[7] == "-":
            return date_part.replace("-", "")
        if len(normalized) == 8 and normalized.isdigit():
            return normalized
        return None

    def _warn_native_partition_fallback(self, load_config, reason: str) -> None:
        if hasattr(self.logger, "warning"):
            self.logger.warning(
                "BigQuery native partition_replace unavailable for "
                f"{load_config.target_schema}.{load_config.target_table}: {reason}. "
                "Using staging-first partition predicate fallback unless native_mode=required."
            )
