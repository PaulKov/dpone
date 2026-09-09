"""BigQuery приёмник данных."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.bigquery_connector import BigQuerySinkConnectorPort
    from dpone.ports.source_state_storage import SourceStateStoragePort
    from dpone.readiness.schema_evolution import SchemaPlan
    from dpone.runtime.sink_logging import ETLLogger
    from dpone.runtime.sinks.load_result import LoadResult
    from dpone.runtime.sinks.strategies.base import SinkStrategy


from dataclasses import replace
from typing import Any

from dpone.runtime.sink_logging import etl_logger
from dpone.runtime.sinks.bigquery_strategy_factory import BigQuerySinkCompositionFactory
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.sink_protocol import AbstractSink


class BigQuerySink(AbstractSink):
    """Приёмник данных BigQuery.

    Поддерживает все стратегии загрузки:
    - FULL_REFRESH: DELETE все + INSERT новые
    - INCREMENTAL_MERGE: DELETE по unique_key + INSERT
    - INCREMENTAL_APPEND: INSERT (с флагом only_new_rows)
    - REPLACE: DELETE по predicate + INSERT

    Reconciliation выполняется в ETLProcessor ДО sink.load().
    """

    def __init__(
        self,
        connector: BigQuerySinkConnectorPort,
        state_storage: SourceStateStoragePort | None = None,
        logger: ETLLogger | None = None,
        composition_factory: BigQuerySinkCompositionFactory | None = None,
    ):
        """Инициализирует BigQuery sink.

        Args:
            connector: BigQueryConnector для выполнения запросов
            state_storage: XMinStateStorage для управления состоянием (опционально)
            logger: ETLLogger для логирования
        """
        self.connector = connector
        self.state_storage = state_storage
        self.logger = logger or etl_logger
        composition = (composition_factory or BigQuerySinkCompositionFactory()).build(connector, self.logger)
        self.staging_manager = composition.staging_manager
        self._strategy_map = composition.strategy_map

    def load(self, load_config: LoadConfig, payload: LoadPayload) -> LoadResult:
        """Загружает данные в BigQuery используя подходящую стратегию.

        Reconciliation выполняется в ETLProcessor ДО этого метода.

        Args:
            load_config: Конфигурация загрузки (схема, таблица, стратегия, и т.д.)
            payload: Данные для загрузки (artifact, schema)

        Returns:
            LoadResult с метриками (inserted_rows, updated_rows, total_rows)

        Raises:
            ValueError: Если стратегия не поддерживается
        """
        strategy = self._resolve_strategy(load_config)

        self.logger.log_etl_progress(
            "BQ_LOAD_START",
            {
                "Target": f"{load_config.target_schema}.{load_config.target_table}",
                "Strategy": load_config.load_strategy.value,
            },
        )

        try:
            # Материализуем данные в staging таблицу
            try:
                staging_artifact = payload.artifact.materialize(
                    self.staging_manager,
                    load_config,
                    payload.schema,
                )
            except Exception:
                raise

            try:
                # Создаем payload с materialized artifact для стратегии
                materialized_payload = LoadPayload(
                    artifact=staging_artifact,
                    schema=payload.schema,
                )
                result = strategy.load(load_config, materialized_payload)

                # Добавляем staging_rows из materialized artifact
                if staging_artifact.row_count > 0:
                    result = replace(result, staging_rows=staging_artifact.row_count)

                self.logger.log_etl_progress(
                    "BQ_LOAD_COMPLETE",
                    {
                        "Target": f"{load_config.target_schema}.{load_config.target_table}",
                        "Inserted": result.inserted_rows,
                        "Updated": result.updated_rows,
                        "Total": result.total_rows,
                    },
                )

                try:
                    staging_artifact.cleanup()
                except Exception as cleanup_exc:
                    self.logger.log_etl_error(
                        f"Ошибка cleanup staging artifact: {str(cleanup_exc)}",
                        {"artifact": str(staging_artifact)},
                    )

                return result
            except Exception:
                try:
                    staging_artifact.cleanup()
                except Exception:
                    pass
                raise
        except Exception as exc:
            self.logger.log_etl_error(
                f"Ошибка загрузки в BigQuery: {str(exc)}",
                {
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Strategy": load_config.load_strategy.value,
                },
            )
            raise

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        table_id = f"{self.connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
        try:
            table = self.connector.connection.get_table(table_id)
        except Exception:
            return []
        return [
            (str(field.name), str(getattr(field, "field_type", getattr(field, "type", "STRING"))))
            for field in table.schema
        ]

    def apply_schema_plan(self, load_config: LoadConfig, plan: SchemaPlan) -> None:
        qualified = f"{self.connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
        for statement in plan.ddl_sql("bigquery", qualified):
            self.connector.execute_query(statement)

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        """Сохраняет состояние инкрементальной загрузки.

        Args:
            load_config: Конфигурация загрузки
            state: Состояние для сохранения (XMinState для PostgreSQL, None для других)
        """
        if state is None or not self.state_storage:
            return

        self.state_storage.save_state(
            load_config.source_schema,
            load_config.source_table,
            state,
        )

        self.logger.log_etl_progress(
            "BQ_STATE_SAVED",
            {
                "Source": f"{load_config.source_schema}.{load_config.source_table}",
            },
        )

    def _resolve_strategy(self, load_config: LoadConfig) -> SinkStrategy:
        """Выбирает подходящую стратегию по LoadStrategy.

        Args:
            load_config: Конфигурация загрузки

        Returns:
            Объект стратегии

        Raises:
            ValueError: Если стратегия не поддерживается
        """
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(
                f"Неизвестная стратегия load_strategy: {load_config.load_strategy}. "
                f"Поддерживаемые: {', '.join(s.value for s in self._strategy_map.keys())}"
            )
        return strategy
