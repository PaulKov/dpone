"""Инкрементальная стратегия извлечения из ClickHouse по дате из sink.

**Рефакторинг по SOLID/DRY:**
- IncrementalStateManager: Управление state из BigQuery
- TimezoneConverter: Конвертация UTC ↔ Europe/Moscow
- GCS экспорт: Через методы коннектора (export_to_gcs, export_incremental_with_cleanup)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.streaming_rows import StreamingRowsArtifact


from datetime import date, timedelta

from dpone.contracts.clickhouse_incremental_cursor import assert_clickhouse_mssql_cursor_supported
from dpone.runtime.cloud_artifacts import GCSExportArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.clickhouse.clickhouse_base import ClickHouseBaseStrategy
from dpone.runtime.sources.strategies.clickhouse.incremental_state_manager import IncrementalStateManager
from dpone.runtime.support.gcs import get_env_code, get_gcs_bucket_name
from dpone.runtime.support.timezone import TimezoneConverter


class ClickHouseIncrementalExtractStrategy(ClickHouseBaseStrategy):
    """
    Стратегия инкрементальной выборки из ClickHouse по колонке даты.
    """

    def __init__(
        self,
        connector,
        sink_connector,
        logger,
    ):
        """
        Args:
            connector: ClickHouse connector
            sink_connector: BigQuery connector (для получения max())
            logger: ETL logger
        """
        super().__init__(connector, logger)
        self.sink_connector = sink_connector

        # Инициализируем компоненты (Dependency Injection)
        self.state_manager = IncrementalStateManager(
            sink_connector=sink_connector, logger=logger, clickhouse_timezone="Europe/Moscow"
        )
        self.tz_converter = TimezoneConverter(target_tz="Europe/Moscow")

    def get_state(self, load_config) -> dict | None:
        """
        Получает инкрементальное состояние из sink БД.

        Делегирует работу IncrementalStateManager.

        Returns:
            {"last_value": "2025-10-28 23:59:59.994", "column": "dt"} или None
        """
        return self.state_manager.get_state(load_config)

    def extract(self, load_config, last_state: dict | None) -> ExtractResult:
        """
        Извлекает инкрементальные данные из ClickHouse.

        **Два режима:**
        1. export_to_gcs=True: Нативный экспорт ClickHouse → GCS → BigQuery
        2. export_to_gcs=False: Streaming через Python (legacy)

        **Режим lookback_days:**
        Если указан lookback_days > 0, то:
        - Партиции за последние N дней будут полностью перезагружены (DELETE + INSERT)
        - Остальные партиции загружаются инкрементально (WHERE dt > max)
        - Это позволяет детектировать и загружать долеты (late-arriving data)
        """
        options = self.get_options(load_config)
        assert_clickhouse_mssql_cursor_supported(
            configured_sink=options.get("sink_type") or options.get("target_type"),
            sink_connector=self.sink_connector,
        )
        export_to_gcs = options.get("export_to_gcs", False)

        # Получаем схему таблицы
        schema = self.get_table_schema(load_config)
        columns = [col for col, _ in schema]

        # Строим инкрементальный запрос
        incremental_column = last_state["column"] if last_state else None
        last_value = last_state["last_value"] if last_state else None
        lookback_days = options.get("lookback_days", 0)

        column_timezone = None
        if incremental_column:
            for col_name, col_type in schema:
                if col_name == incremental_column:
                    column_timezone = self.extract_timezone_from_column_type(col_type)
                    if column_timezone:
                        self.logger.info(
                            f"📍 Обнаружена timezone в колонке {incremental_column}: {column_timezone} (тип: {col_type})"
                        )
                    break

        # Проверяем, указаны ли явные даты (date_from/date_to)
        # Если да, это явный full refresh для диапазона дат
        date_from = options.get("date_from")
        date_to = options.get("date_to")
        has_explicit_date_range = date_from and date_to

        if not incremental_column or not last_value:
            # Не выводим warning, если указан явный диапазон дат (это нормально для full refresh)
            if not has_explicit_date_range:
                self.logger.warning("Нет инкрементального состояния, выполняется full refresh")
            # Fallback на full refresh
            from dpone.runtime.sources.strategies.clickhouse.clickhouse_full_extract import (
                ClickHouseFullExtractStrategy,
            )

            full_strategy = ClickHouseFullExtractStrategy(self.connector, self.logger)
            return full_strategy.extract(load_config, None)

        # Определяем режим загрузки
        if lookback_days > 0:
            # Режим lookback: загружаем данные с учетом lookback-окна
            query, lookback_start_date = self._build_lookback_query(
                schema=load_config.source_schema,
                table=load_config.source_table,
                column_types=schema,
                incremental_column=incremental_column,
                last_value=last_value,
                lookback_days=lookback_days,
                predicate=options.get("custom_predicate"),
            )
        else:
            # Обычный инкремент: WHERE dt > max
            query = self.build_incremental_query(
                schema=load_config.source_schema,
                table=load_config.source_table,
                columns=columns,
                incremental_column=incremental_column,
                last_value=last_value,
                predicate=options.get("custom_predicate"),
                column_types=schema,
            )
            lookback_start_date = None

        self.logger.log_etl_progress(
            "CH_INCREMENTAL_QUERY_BUILT",
            {
                "IncrementalColumn": incremental_column,
                "LastValue": last_value,
                "LookbackDays": lookback_days,
                "LookbackStartDate": str(lookback_start_date) if lookback_start_date else "None",
                "Query": query[:200] + "..." if len(query) > 200 else query,
            },
        )

        # Выбираем режим экспорта
        artifact: GCSExportArtifact | StreamingRowsArtifact
        if export_to_gcs:
            artifact = self._perform_gcs_export_incremental(
                load_config,
                schema,
                query,
                incremental_column,
                last_value,
                lookback_start_date=lookback_start_date,
                column_timezone=column_timezone,
            )
        else:
            artifact = self._perform_streaming_extract(load_config, schema, query, columns)

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state=None,
            force_full_refresh=False,
        )

    def _perform_gcs_export_incremental(
        self,
        load_config,
        schema,
        query: str,
        incremental_column: str,
        last_value: str,
        lookback_start_date: date | None = None,
        column_timezone: str | None = None,
    ) -> GCSExportArtifact:
        """
        Выполняет нативный экспорт инкрементальных данных в GCS с партиционированием.
        """
        options = self.get_options(load_config)

        # Конфигурация GCS
        gcs_format = options.get("gcs_format", "parquet").lower()
        gcs_chunk_rows = options.get("gcs_chunk_rows")
        gcs_file_prefix = options.get("gcs_file_prefix", "data")
        partition_by = options.get("partition_by", "day")

        # Генерируем бакет и путь (используем target_schema для бакета)
        env_code = get_env_code()
        bucket_name = get_gcs_bucket_name(load_config.target_schema, env_code)

        # Базовый путь: {target_schema}/transfer/{source_schema}/{source_schema}.{source_table}
        base_table_path = f"{load_config.target_schema}/transfer/{load_config.source_schema}/{load_config.source_schema}.{load_config.source_table}"

        # Определяем диапазон дат для инкремента
        if lookback_start_date:
            date_from = lookback_start_date
            date_to = self.tz_converter.get_current_date_in_tz()
        else:
            # Обычный инкремент: начиная с даты last_value
            date_from, date_to = self._calculate_date_range(last_value)

        # Получаем текущую дату в ClickHouse timezone
        today_date = self.tz_converter.get_current_date_in_tz()

        # Генерируем партиции с учетом инкрементального фильтра для текущей партиции
        partitions = self.connector.generate_date_partitions(
            date_from=date_from,
            date_to=date_to,
            partition_by=partition_by,
            date_column=incremental_column,
            current_date=today_date,  # Всегда передаем для проверки
            last_value=last_value,  # Всегда передаем для проверки
        )

        self.logger.log_etl_progress(
            "CH_GCS_INCREMENTAL_EXPORT_CONFIG",
            {
                "Bucket": bucket_name,
                "Path": base_table_path,
                "Format": gcs_format.upper(),
                "ChunkRows": gcs_chunk_rows or "AUTO",
                "PartitionBy": partition_by,
                "DateFrom": str(date_from),
                "DateTo": str(date_to),
                "Partitions": len(partitions),
            },
        )

        # Определяем lookback партиции (если используется lookback режим)
        lookback_partition_labels = None
        if lookback_start_date:
            # В режиме lookback ALL партиции должны перезагружаться (DELETE + INSERT в BQ)
            lookback_partition_labels = [
                label for label, _ in partitions if date_from <= date.fromisoformat(label) <= today_date
            ]

            all_partition_labels = [label for label, _ in partitions]

            self.logger.info(
                f"🔍 LOOKBACK MODE: "
                f"Все партиции будут удалены в BQ перед загрузкой (DELETE + INSERT): "
                f"{lookback_partition_labels}"
            )

            self.logger.info(
                f"📊 PARTITION DETAILS: "
                f"date_from={date_from}, today={today_date}, "
                f"all_partitions={all_partition_labels}"
            )

        # Экспортируем партиции через коннектор
        result = self.connector.export_incremental_with_cleanup(
            query=query,
            schema=schema,
            partitions=partitions,
            bucket_name=bucket_name,
            base_table_path=base_table_path,
            base_gcs_uri="",
            format=gcs_format,
            chunk_rows=gcs_chunk_rows,
            file_prefix=gcs_file_prefix,
            lookback_partitions=lookback_partition_labels,
            incremental_column=incremental_column,
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            load_config=load_config,
        )

        columns = [col for col, _ in schema]
        created_partitions = result["created_partitions"]
        incremental_partition_labels = result["incremental_partitions"]
        attempt_gcs_uri = result["attempt_gcs_uri"]
        attempt_scope = result["attempt_scope"]
        attempt_table_prefix = result["attempt_table_prefix"]
        prior_generation_prefixes = result["prior_generation_prefixes"]

        self.logger.info(f"🌐 GCS Export: передача timezone={column_timezone} в artifact")

        return GCSExportArtifact(
            gcs_uri=attempt_gcs_uri,
            columns=columns,
            format=gcs_format,
            new_partitions=created_partitions,
            incremental_partitions=incremental_partition_labels,
            incremental_column=incremental_column,
            column_timezone=column_timezone,
            hive_partitioning=True,
            source_uri_prefix=f"{attempt_gcs_uri}/",
            cleanup_gcs=True,
            bucket_name=bucket_name,
            attempt_table_prefix=attempt_table_prefix,
            prior_generation_prefixes=prior_generation_prefixes,
            attempt_scope=attempt_scope,
        )

    def _perform_streaming_extract(
        self,
        load_config,
        schema,
        query: str,
        columns: list[str],
    ) -> StreamingRowsArtifact:
        """
        Выполняет streaming extraction через Python.

        Используется когда export_to_gcs=False.
        """
        self.logger.log_etl_progress(
            "CH_INCREMENTAL_STREAMING_START",
            {
                "Mode": "Python Streaming (legacy)",
                "BatchSize": load_config.batch_size,
            },
        )

        return self._build_rows_artifact(
            self.connector,
            query,
            schema,
            batch_size=load_config.batch_size,
        )

    def _build_lookback_query(
        self,
        schema: str,
        table: str,
        column_types: list[tuple[str, str]],
        incremental_column: str,
        last_value: str,
        lookback_days: int,
        predicate: str | None = None,
    ) -> tuple[str, date]:
        """
        Строит запрос с учетом lookback-окна для детектирования долетов.

        **Логика lookback:**
        - Вычисляем lookback_start_date = current_date - lookback_days
        - Строим запрос: WHERE toDate(dt) >= 'lookback_start_date'
        - Это позволяет перезагрузить последние N дней полностью
        """
        current_date = self.tz_converter.get_current_date_in_tz()
        lookback_start_date = current_date - timedelta(days=lookback_days)

        # Форматируем для ClickHouse
        lookback_start_str = lookback_start_date.strftime("%Y-%m-%d")

        # Строим WHERE clause
        where_clause = f"toDate(`{incremental_column}`) >= '{lookback_start_str}'"

        if predicate:
            where_clause = f"({where_clause}) AND ({predicate})"

        # Формируем SELECT с трансформацией типов
        transformed_cols = [
            f"{self.transform_column_for_export(col, col_type)} AS `{col}`" for col, col_type in column_types
        ]
        columns_str = ", ".join(transformed_cols)

        query = f"""
            SELECT {columns_str}
            FROM `{schema}`.`{table}`
            WHERE {where_clause}
        """

        self.logger.info(
            f"🔄 LOOKBACK MODE: будут перезагружены партиции с {lookback_start_str} (lookback_days={lookback_days})"
        )

        return query, lookback_start_date

    def _calculate_date_range(self, last_value: str) -> tuple[date, date]:
        """
        Вычисляет диапазон дат для партиционирования.

        **Логика:**
        - Если last_value за прошлые дни → загружаем со следующего дня
        - Если last_value за сегодня → загружаем сегодня (с инкрементальным фильтром)
        """
        last_dt = self.tz_converter.parse_and_convert(last_value, from_tz="Europe/Moscow")

        last_date = last_dt.date()

        date_to = self.tz_converter.get_current_date_in_tz()

        # Определяем date_from с учетом текущей партиции
        if last_date >= date_to:
            date_from = date_to
        else:
            date_from = last_date + timedelta(days=1)

        return date_from, date_to
