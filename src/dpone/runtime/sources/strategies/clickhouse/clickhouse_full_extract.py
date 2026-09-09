"""Полная выборка из ClickHouse с оптимизацией."""

from __future__ import annotations

from typing import Any

from dpone.runtime.cloud_artifacts import GCSExportArtifact
from dpone.runtime.gcs_replacement import build_gcs_export_identity, derive_partition_attempt_uri
from dpone.runtime.sink_dialect import is_mssql_dialect
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.clickhouse.clickhouse_base import ClickHouseBaseStrategy
from dpone.runtime.sources.strategies.clickhouse.clickhouse_query_extract import ClickHouseQueryExtractService
from dpone.runtime.support.gcs import (
    build_gcs_file_pattern,
    get_env_code,
    get_gcs_bucket_name,
)


class ClickHouseFullExtractStrategy(ClickHouseBaseStrategy):
    """
    Стратегия полной выборки данных из ClickHouse.

    Поддерживает:
    - Автоматическое построение предикатов по дате (day/month/year)
    - Партиционирование при выгрузке
    - Оптимизацию через custom_predicate
    - Нативный экспорт в GCS (для BigQuery sink)
    """

    def __init__(self, connector, logger):
        super().__init__(connector, logger)

    def get_state(self, load_config) -> Any | None:
        """Full refresh не требует состояния."""
        return None

    def get_table_schema(self, load_config):
        """Resolve query-source schema without inspecting its placeholder table."""

        options = self.get_options(load_config)
        if isinstance(options.get("query"), dict):
            return ClickHouseQueryExtractService(self.connector, self.logger).describe(load_config)
        return super().get_table_schema(load_config)

    def extract(self, load_config, last_state: Any | None) -> ExtractResult:
        """Извлекает все данные из таблицы ClickHouse."""
        options = self.get_options(load_config)
        if isinstance(options.get("query"), dict):
            return ClickHouseQueryExtractService(self.connector, self.logger).extract(load_config)

        schema = self.get_table_schema(load_config)

        # Проверяем, нужен ли GCS экспорт
        export_to_gcs = options.get("export_to_gcs", False)

        self.logger.log_etl_progress(
            "CH_EXTRACT_OPTIONS_DEBUG",
            {
                "export_to_gcs": export_to_gcs,
                "options_keys": list(options.keys()) if options else [],
                "has_options": bool(options),
            },
        )

        if export_to_gcs:
            # Нативный экспорт в GCS (для BigQuery sink)
            self.logger.log_etl_progress(
                "CH_GCS_EXPORT_START",
                {
                    "Database": load_config.source_schema,
                    "Table": load_config.source_table,
                    "Mode": "NATIVE",
                },
            )

            artifact = self._perform_gcs_export(load_config, schema)

            return ExtractResult(artifact=artifact, schema=schema, state=None)
        else:
            # Обычный streaming экспорт через Python
            self.logger.log_etl_progress(
                "CH_USING_PYTHON_STREAMING",
                {
                    "Reason": "export_to_gcs=False or not set",
                },
            )

            predicate = self.build_where_clause(load_config)

            query = self.build_select_query(
                load_config.source_schema,
                load_config.source_table,
                [column for column, _ in schema],
                predicate,
                column_types=schema,
            )

            artifact = self._build_rows_artifact(
                self.connector,
                query,
                schema,
                batch_size=load_config.batch_size,
                preflight_count=not is_mssql_dialect(options.get("sink_type")),
            )

            return ExtractResult(artifact=artifact, schema=schema, state=None)

    def build_where_clause(self, load_config) -> str | None:
        """
        Строит предикат для фильтрации по дате.
        """
        options = self.get_options(load_config)

        date_column = options.get("date_column")
        date_from = options.get("date_from")
        date_to = options.get("date_to")
        partition_by = options.get("partition_by")  # 'day' | 'month' | 'year'

        # source_custom_predicate — предикат в синтаксисе SOURCE БД (ClickHouse SQL)
        source_custom_predicate = options.get("source_custom_predicate") or options.get("custom_predicate")

        predicates = []

        # Если задан date_column и диапазон дат
        if date_column:
            date_predicates = self.build_date_predicates(date_column, date_from, date_to, partition_by)
            predicates.extend(date_predicates)

        if source_custom_predicate:
            predicates.append(f"({source_custom_predicate})")

        return " AND ".join(predicates) if predicates else None

    def _export_partitioned_data(
        self,
        load_config,
        schema: list[tuple[str, str]],
        bucket_name: str,
        partitions: list,
        partition_by: str,
        gcs_format: str,
        gcs_file_prefix: str,
        gcs_chunk_rows: int | None,
        *,
        attempt_scope,
        base_table_path: str,
        attempt_table_prefix: str,
    ) -> None:
        """
        Выполняет партиционированный экспорт данных в GCS через методы коннектора.

        ClickHouse автоматически пропускает пустые партиции (не создает файлы).
        """
        # Экспортируем каждую партицию через connector
        for partition_date, partition_predicate in partitions:
            gcs_uri = derive_partition_attempt_uri(
                bucket_name,
                base_table_path,
                attempt_scope,
                str(partition_date),
            )

            # Формируем запрос с WHERE для партиции (с трансформацией типов)
            transformed_cols = [
                f"{self.transform_column_for_export(col, col_type)} AS `{col}`" for col, col_type in schema
            ]
            columns_str = ", ".join(transformed_cols)
            query = f"""
                SELECT {columns_str}
                FROM `{load_config.source_schema}`.`{load_config.source_table}`
                WHERE {partition_predicate}
            """

            # Логируем
            self.logger.log_etl_progress(
                "CH_GCS_PARTITION_EXPORT",
                {
                    "Partition": partition_date,
                    "GCS_URI": gcs_uri,
                    "Format": gcs_format.upper(),
                },
            )

            try:
                # Экспортируем партицию через connector
                self.connector.export_to_gcs(
                    query=query,
                    gcs_uri=gcs_uri,
                    format=gcs_format,
                    chunk_rows=gcs_chunk_rows,
                    file_prefix=gcs_file_prefix,
                )

                self.logger.log_etl_progress(
                    "CH_GCS_PARTITION_SUCCESS",
                    {
                        "Partition": partition_date,
                        "Destination": f"{gcs_uri}/{gcs_file_prefix}",
                    },
                )

            except Exception as exc:
                self.logger.log_etl_error(
                    f"Ошибка экспорта партиции {partition_date}: {str(exc)}",
                    {
                        "Partition": partition_date,
                        "GCS_URI": gcs_uri,
                        "Query": query[:200],
                    },
                )
                raise

    def _perform_gcs_export(self, load_config, schema) -> GCSExportArtifact:
        """
        Выполняет нативный экспорт ClickHouse → GCS с поддержкой партиционирования.

        Returns:
            GCSExportArtifact с base URI и wildcard pattern для BigQuery Load
        """
        options = self.get_options(load_config)

        # === Параметры GCS экспорта ===
        gcs_format = options.get("gcs_format", "parquet")
        gcs_chunk_rows = options.get("gcs_chunk_rows")
        gcs_file_prefix = options.get("gcs_file_prefix", "data")

        # === Параметры партиционирования ===
        date_column = options.get("date_column")
        date_from_str = options.get("date_from")
        date_to_str = options.get("date_to")
        partition_by = options.get("partition_by", "day")

        # === Построение base GCS URI ===
        env_code = get_env_code()
        bucket_name = get_gcs_bucket_name(load_config.target_schema, env_code)
        base_table_path = f"{load_config.target_schema}/transfer/{load_config.source_schema}/{load_config.source_schema}.{load_config.source_table}"

        columns = [column for column, _ in schema]

        # === Определяем, нужно ли партиционирование ===
        lookback_partition_labels = None
        new_partition_labels = None
        incremental_column = options.get("incremental_column")
        attempt_scope = None
        attempt_table_prefix = None
        attempt_gcs_uri = None
        prior_generation_prefixes: tuple[str, ...] = ()
        pattern = None
        hive_partitioning = False
        source_uri_prefix = None

        if date_column and date_from_str and date_to_str:
            # Генерируем партиции
            partitions = self.connector.generate_date_partitions(
                date_from=date_from_str,
                date_to=date_to_str,
                partition_by=partition_by,
                date_column=date_column,
            )

            self.logger.log_etl_progress(
                "CH_GCS_PARTITION_EXPORT_START",
                {
                    "PartitionBy": partition_by,
                    "DateFrom": date_from_str,
                    "DateTo": date_to_str,
                    "PartitionCount_Total": len(partitions),
                },
            )

            # === PARTITION DISCOVERY: находим непустые партиции одним запросом ===
            nonempty_partitions = self.connector.discover_nonempty_partitions(
                schema=load_config.source_schema,
                table=load_config.source_table,
                date_column=date_column,
                date_from=date_from_str,
                date_to=date_to_str,
                partition_by=partition_by,
            )

            # Фильтруем только непустые партиции
            partitions_to_export = [
                (label, predicate) for label, predicate in partitions if label in nonempty_partitions
            ]

            # Когда указаны date_from/date_to, ВСЕ найденные партиции должны перезагружаться (DELETE + INSERT)
            # Это явный full refresh для диапазона дат
            partition_labels_to_export = [label for label, _ in partitions_to_export]
            lookback_partition_labels = partition_labels_to_export
            new_partition_labels = partition_labels_to_export
            (
                attempt_scope,
                attempt_table_prefix,
                attempt_gcs_uri,
                prior_generation_prefixes,
            ) = build_gcs_export_identity(
                load_config,
                bucket_name=bucket_name,
                base_table_path=base_table_path,
                partition_labels=partition_labels_to_export,
            )

            self.logger.log_etl_progress(
                "CH_GCS_PARTITION_FILTERED",
                {
                    "PartitionCount_Total": len(partitions),
                    "PartitionCount_NonEmpty": len(nonempty_partitions),
                    "PartitionCount_ToExport": len(partitions_to_export),
                    "Skipped": len(partitions) - len(partitions_to_export),
                },
            )

            # Делегируем экспорт только непустых партиций
            self._export_partitioned_data(
                load_config=load_config,
                schema=schema,
                bucket_name=bucket_name,
                partitions=partitions_to_export,
                partition_by=partition_by,
                gcs_format=gcs_format,
                gcs_file_prefix=gcs_file_prefix,
                gcs_chunk_rows=gcs_chunk_rows,
                attempt_scope=attempt_scope,
                base_table_path=base_table_path,
                attempt_table_prefix=attempt_table_prefix,
            )

            # Pattern для партиционированного экспорта
            pattern = build_gcs_file_pattern(
                gcs_file_prefix,
                gcs_format,
                gcs_chunk_rows,
                partitioned=True,
            )

            hive_partitioning = True
            source_uri_prefix = f"{attempt_gcs_uri}/"

            self.logger.log_etl_progress(
                "CH_GCS_PARTITION_EXPORT_COMPLETE",
                {
                    "PartitionCount": len(partitions),
                    "BaseURI": attempt_gcs_uri,
                    "Pattern": pattern,
                    "HivePartitioning": hive_partitioning,
                    "SourceURIPrefix": source_uri_prefix,
                },
            )

        else:
            # ===== НЕПАРТИЦИОНИРОВАННЫЙ ЭКСПОРТ =====
            (
                attempt_scope,
                attempt_table_prefix,
                attempt_gcs_uri,
                prior_generation_prefixes,
            ) = build_gcs_export_identity(
                load_config,
                bucket_name=bucket_name,
                base_table_path=base_table_path,
            )
            predicate = self.build_where_clause(load_config)

            # Формируем запрос с трансформацией типов
            transformed_cols = [
                f"{self.transform_column_for_export(col, col_type)} AS `{col}`" for col, col_type in schema
            ]
            columns_str = ", ".join(transformed_cols)
            query = f"""
                SELECT {columns_str}
                FROM `{load_config.source_schema}`.`{load_config.source_table}`
            """

            if predicate:
                query = f"{query} WHERE {predicate}"

            # Экспортируем через connector.export_to_gcs()
            self.connector.export_to_gcs(
                query=query,
                gcs_uri=attempt_gcs_uri,
                format=gcs_format,
                chunk_rows=gcs_chunk_rows,
                file_prefix=gcs_file_prefix,
            )

            pattern = build_gcs_file_pattern(
                gcs_file_prefix,
                gcs_format,
                gcs_chunk_rows,
                partitioned=False,
            )

            hive_partitioning = False
            source_uri_prefix = None

        # === Возвращаем GCSExportArtifact ===
        # Когда указаны date_from/date_to, все партиции должны перезагружаться (lookback_partitions)
        return GCSExportArtifact(
            gcs_uri=attempt_gcs_uri,
            columns=columns,
            format=gcs_format,
            pattern=pattern,
            hive_partitioning=hive_partitioning,
            source_uri_prefix=source_uri_prefix,
            cleanup_gcs=True,
            bucket_name=bucket_name,
            attempt_table_prefix=attempt_table_prefix,
            prior_generation_prefixes=prior_generation_prefixes,
            attempt_scope=attempt_scope,
            estimated_rows=None,
            lookback_partitions=lookback_partition_labels,
            new_partitions=new_partition_labels,
            incremental_column=incremental_column,
        )
