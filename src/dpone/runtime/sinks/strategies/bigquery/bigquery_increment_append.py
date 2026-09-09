"""Единая стратегия INCREMENTAL_APPEND для BigQuery."""

from __future__ import annotations

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.bigquery.bigquery_base import BigQueryStrategyBase


class BigQueryIncrementAppendStrategy(BigQueryStrategyBase):
    """Стратегия INCREMENTAL_APPEND для BigQuery.

    Поведение зависит от флага load_config.only_new_rows:

    only_new_rows=True (INSERT NOT EXISTS):
    - Если таблица не существует - создает её и вставляет все данные
    - Если существует - вставляет только новые записи (по unique_key)
    - Требует unique_key для проверки дубликатов
    - Используется для: append-only таблиц (события, логи, истории)

    only_new_rows=False (INSERT ALL):
    - Если таблица не существует - создает её
    - Вставляет ВСЕ данные без проверок на дубликаты
    - Используется для: append-only таблиц где дубликаты возможны

    ⚠️ ВНИМАНИЕ: only_new_rows=False может привести к дубликатам!
    """

    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с incremental_append стратегией."""
        columns = [col for col, _ in payload.schema]
        only_new = load_config.only_new_rows

        # Валидация: only_new_rows требует unique_key
        if only_new and not load_config.unique_key:
            raise ValueError(
                "only_new_rows=True требует unique_key для проверки дубликатов. "
                "Либо укажите unique_key, либо установите only_new_rows=False."
            )

        # 1. Создаем целевую таблицу если нужно
        target_created = self._create_target_table_if_not_exists(load_config, payload.schema)

        # 2. LOOKBACK MODE: Удаляем lookback партиции если они есть
        deleted_lookback = 0

        options = getattr(load_config, "options", {}) or {}
        lookback_partitions = options.get("_lookback_partitions")

        if lookback_partitions and not target_created:
            partition_column = options.get("incremental_column", "dt")

            deleted_lookback = self._delete_partitions(
                load_config, lookback_partitions, partition_column=partition_column
            )

        # 3. Вставляем данные
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        fq_staging = f"`{self.connector.project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"

        # Строим SELECT с PARSE_JSON() для JSON колонок
        include_tech = self._include_technical_columns(load_config)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=include_tech,
        )
        select_str_with_alias = self._build_select_with_json_parse(
            payload.schema,
            "s",
            include_technical_columns=include_tech,
        )[1]

        # Определяем режим вставки
        if target_created:
            # Таблица только что создана - вставляем все данные
            query = f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
            action = "INSERT ALL (new table)"
            reason = "Target table created"
        elif lookback_partitions:
            query = f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
            action = f"INSERT ALL (lookback refresh, deleted={deleted_lookback})"
            reason = f"Lookback partitions: {lookback_partitions}"
        elif not only_new:
            # Таблица существует, но only_new=False - вставляем всё (могут быть дубликаты!)
            query = f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
            action = "INSERT ALL (⚠️ duplicates possible)"
            reason = "only_new_rows=False"
        else:
            # Таблица существует + only_new=True - вставляем только новые записи
            condition = self._build_unique_key_condition("s", "t", load_config.unique_key)
            query = f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str_with_alias}
            FROM {fq_staging} AS s
            WHERE NOT EXISTS (
                SELECT 1 FROM {fq_target} AS t
                WHERE {condition}
            )
            """
            action = "INSERT NOT EXISTS (by unique_key)"
            reason = "only_new_rows=True + table exists"

        self.logger.log_etl_progress(
            "BQ_INCREMENT_APPEND",
            {
                "Target": fq_target,
                "Action": action,
                "Reason": reason,
                "Only_New_Rows": only_new,
                "Unique_Key": str(load_config.unique_key) if load_config.unique_key else "None",
                "Lookback_Partitions": lookback_partitions or "None",
                "Deleted_Lookback_Rows": deleted_lookback,
                "Columns": len(columns),
            },
        )

        inserted = self._execute_dml_query(query)

        # Валидация партиций после загрузки (только для ClickHouse source с партициями)
        # Используем DRY метод из базового класса
        partition_validation_results = self._validate_partitions_clickhouse(
            load_config=load_config,
            payload=payload,
            inserted=inserted,
        )

        if inserted and inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
            self._log_target_sample(fq_target, load_config.log_sample_rows)

        # Создаем load_result с результатами валидации партиций
        return LoadResult(
            inserted_rows=inserted or 0,
            updated_rows=0,
            total_rows=inserted or 0,
            deleted_lookback_rows=deleted_lookback,
            partition_validation_results=partition_validation_results if partition_validation_results else None,
        )
