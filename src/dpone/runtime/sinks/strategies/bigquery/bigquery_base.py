"""Базовые классы для BigQuery стратегий."""

from __future__ import annotations

from abc import ABC
from typing import Any

from dpone.contracts.technical_columns import include_technical_columns
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.bigquery.dml_helper import BigQueryDmlHelper
from dpone.runtime.sinks.strategies.bigquery.exchange_logger import BigQueryExchangeLogger
from dpone.runtime.sinks.strategies.bigquery.partition_validation_service import BigQueryPartitionValidationService
from dpone.runtime.sinks.strategies.bigquery.target_table_manager import BigQueryTargetTableManager

_TARGET_TABLE_DELEGATES = {
    "_get_fq_table": "get_fq_table",
    "_get_staging_table": "get_staging_table",
    "_create_target_table_if_not_exists": "create_target_table_if_not_exists",
    "_maybe_update_table_metadata": "maybe_update_table_metadata",
    "_ensure_technical_columns": "ensure_technical_columns",
    "_build_schema_fields": "build_schema_fields",
    "_check_table_exists": "check_table_exists",
    "_rename_table": "rename_table",
    "_drop_table": "drop_table",
}

_DML_DELEGATES = {
    "_build_select_with_json_parse": "build_select_with_json_parse",
    "_insert_from_staging": "insert_from_staging",
    "_build_unique_key_condition": "build_unique_key_condition",
    "_delete_existing": "delete_existing",
    "_delete_with_predicate": "delete_with_predicate",
    "_delete_partitions": "delete_partitions",
    "_truncate_table": "truncate_table",
    "_execute_dml_query": "execute_dml_query",
    "_log_target_sample": "log_target_sample",
}

_EXCHANGE_LOG_DELEGATES = {
    "_log_exchange_start": "log_exchange_start",
    "_log_full_refresh_create_new": "log_full_refresh_create_new",
    "_log_exchange_create_tmp": "log_exchange_create_tmp",
    "_log_exchange_tmp_created": "log_exchange_tmp_created",
    "_log_exchange_backup": "log_exchange_backup",
    "_log_exchange_swap": "log_exchange_swap",
    "_log_exchange_cleanup": "log_exchange_cleanup",
    "_log_exchange_complete": "log_exchange_complete",
    "_log_exchange_rollback": "log_exchange_rollback",
    "_log_exchange_rollback_failed": "log_exchange_rollback_failed",
    "_log_full_refresh": "log_full_refresh",
}


class BigQueryStrategyBase(SinkStrategy, ABC):
    """Общий базовый класс для стратегий BigQuery.

    Responsibilities are delegated to focused collaborators:
    - target table lifecycle / metadata / technical columns
    - SQL + DML helpers
    - exchange/full-refresh progress logging
    - partition validation for ClickHouse loads
    """

    def __init__(self, connector: Any, logger: ETLLogger | None = None):
        self.connector = connector
        self.logger = logger or etl_logger

        self.target_table_manager = BigQueryTargetTableManager(
            connector=self.connector,
            logger=self.logger,
            include_technical_columns=self._include_technical_columns,
        )
        self.dml_helper = BigQueryDmlHelper(
            connector=self.connector,
            logger=self.logger,
            include_technical_columns=self._include_technical_columns,
            get_fq_table=self.target_table_manager.get_fq_table,
        )
        self.exchange_logger = BigQueryExchangeLogger(self.logger)
        self.partition_validation_service = BigQueryPartitionValidationService(self.logger)

    def __getattr__(self, name: str) -> Any:
        if name in _TARGET_TABLE_DELEGATES:
            return getattr(self.target_table_manager, _TARGET_TABLE_DELEGATES[name])
        if name in _DML_DELEGATES:
            return getattr(self.dml_helper, _DML_DELEGATES[name])
        if name in _EXCHANGE_LOG_DELEGATES:
            return getattr(self.exchange_logger, _EXCHANGE_LOG_DELEGATES[name])
        if name == "_validate_partitions_clickhouse":
            return self.partition_validation_service.validate_clickhouse
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def _include_technical_columns(self, load_config: Any) -> bool:
        """Whether to ensure/populate technical columns __dpone__loaded_at/__dpone__deleted_at.

        UX:
            Prefer ``sink.options.technical_columns`` with tri-state values:
            - required | optional | forbidden

        Backward compatibility:
            ``sink.options.include_technical_columns`` is still supported.
        """
        opts = getattr(load_config, "options", {}) or {}
        return include_technical_columns(opts)

    def _create_table_from_staging(
        self,
        load_config: Any,
        target_table: str,
        payload: Any,
    ) -> int:
        return self.target_table_manager.create_table_from_staging(
            load_config,
            target_table,
            payload,
            build_select_with_json_parse=self._build_select_with_json_parse,
        )
