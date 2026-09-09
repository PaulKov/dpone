"""Стратегия REPLACE для BigQuery."""

from __future__ import annotations

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.bigquery.bigquery_base import BigQueryStrategyBase


class BigQueryReplaceStrategy(BigQueryStrategyBase):
    """Стратегия REPLACE для BigQuery.

    Поведение:
    1. Если таблица не существует - создает её и вставляет все данные
    2. Если существует:
       a. Удаляет строки по custom_predicate (например, WHERE date = '2025-10-20')
       b. Вставляет новые данные
    """

    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с replace стратегией."""
        if not load_config.custom_predicate:
            raise ValueError("REPLACE стратегия требует custom_predicate (например: WHERE date = '2025-10-20')")

        columns = [col for col, _ in payload.schema]

        # 1. Создаем целевую таблицу если нужно
        target_created = self._create_target_table_if_not_exists(load_config, payload.schema)

        # 2. Если таблица существует - удаляем старые данные по predicate
        deleted = 0
        if not target_created:
            deleted = self._delete_with_predicate(load_config, load_config.custom_predicate)

        # 3. Вставляем новые данные
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        fq_staging = f"`{self.connector.project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"

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
            "BQ_REPLACE",
            {
                "Target": fq_target,
                "Action": "DELETE BY PREDICATE + INSERT",
                "Predicate": load_config.custom_predicate[:100],
                "Deleted_Rows": deleted,
                "Columns": len(columns),
            },
        )

        inserted = self._execute_dml_query(query)

        if inserted and inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
            self._log_target_sample(fq_target, load_config.log_sample_rows)

        return LoadResult(
            inserted_rows=0,  # REPLACE не создаёт новые строки, только заменяет существующие
            updated_rows=0,  # REPLACE не делает UPDATE
            total_rows=inserted or 0,
            replaced_rows=inserted or 0,  # Все вставленные строки = заменённые (DELETE + INSERT)
            deleted_lookback_rows=0,  # REPLACE не использует lookback deletion (это для INCREMENTAL_APPEND)
        )
