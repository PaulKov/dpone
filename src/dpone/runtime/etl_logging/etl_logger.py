"""
Красивый и подробный логгер для ETL процессов.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from dpone.runtime.etl_logging.clickhouse_validation import validate_clickhouse_rows, validate_single_partition
from dpone.runtime.etl_logging.formatting import format_message_lines, format_table
from dpone.runtime.etl_logging.models import Colors, ETLMetrics, LogLevel
from dpone.runtime.etl_logging.strategy_formatting import (
    build_strategy_string as _build_strategy_string,
)
from dpone.runtime.etl_logging.strategy_formatting import (
    format_strategy_with_color,
)

logger = logging.getLogger(__name__)


def _format_strategy_with_color(
    strategy: str,
    *,
    show_colors: bool,
    unique_key: str | list[str] | None = None,
    custom_predicate: str | None = None,
) -> str:
    return format_strategy_with_color(
        strategy,
        colors=Colors,
        show_colors=show_colors,
        unique_key=unique_key,
        custom_predicate=custom_predicate,
    )


class ETLLogger:
    """Production-ready ETL Logger с красивым форматированием"""

    def __init__(self, log_level: str = "INFO", show_colors: bool = True):
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        self.show_colors = show_colors
        self.metrics = None
        self.current_run_id = None
        self._logged_connections = set()

    def __getattr__(self, name: str) -> Any:
        if name == "_build_strategy_string":
            return _build_strategy_string
        if name == "_format_strategy_with_color":
            return lambda strategy, unique_key=None, custom_predicate=None: _format_strategy_with_color(
                strategy,
                show_colors=self.show_colors,
                unique_key=unique_key,
                custom_predicate=custom_predicate,
            )
        if name == "_format_table":
            return format_table
        if name == "_format_message_lines":
            return format_message_lines
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def _log_with_format(self, level: LogLevel, message: str, details: dict[str, Any] | None = None):
        lines = format_message_lines(level, message, details)

        for line in lines:
            if level == LogLevel.ERROR:
                self.logger.error(line)
            elif level == LogLevel.WARNING:
                self.logger.warning(line)
            else:
                self.logger.info(line)

    def _log_level(self, name: str) -> LogLevel:
        return LogLevel[name]

    def log_etl_start(self, config: dict[str, Any]) -> None:
        self.metrics = ETLMetrics(start_time=datetime.now())

        details = {
            "🕐 Start Time": self.metrics.start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "EMPTY_LINE": "",
            "⚙️ CONFIGURATION": "",
            "📂 Source": f"{config.get('source_schema', 'unknown')}.{config.get('source_table', 'unknown')}",
            "🎯 Target": f"{config.get('target_schema', 'unknown')}.{config.get('target_table', 'unknown')}",
            "🔄 Strategy": _format_strategy_with_color(
                config.get("load_strategy", "unknown"),
                show_colors=self.show_colors,
                unique_key=config.get("unique_key"),
                custom_predicate=config.get("custom_predicate"),
            ),
            "📦 Batch Size": f"{config.get('batch_size', 0):,}",
            "EMPTY_LINE2": "",
        }

        self._log_with_format(LogLevel.SUCCESS, "ETL PROCESS STARTED", details)

    def log_etl_end(self, result: dict[str, Any], additional_info: dict[str, Any] | None = None) -> None:
        if self.metrics:
            self.metrics.end_time = datetime.now()
            self.metrics.extracted_rows = result.get("extracted_rows", 0)
            self.metrics.staging_rows = result.get("staging_rows", 0)
            self.metrics.loaded_rows = result.get("loaded_rows", 0)
            self.metrics.final_rows = result.get("final_rows", 0)
            self.metrics.inserted_rows = result.get("inserted_rows", 0)
            self.metrics.updated_rows = result.get("updated_rows", 0)
            self.metrics.soft_deleted_rows = result.get("soft_deleted_rows", 0)
            self.metrics.replaced_rows = result.get("replaced_rows", 0)
            self.metrics.deleted_lookback_rows = result.get("deleted_lookback_rows", 0)

        status = result.get("status", "unknown")

        details = {
            "📊 Status": status.upper(),
            "⏱️ Duration": str(self.metrics.duration) if self.metrics and self.metrics.duration else "N/A",
            "EMPTY_LINE": "",
            "📈 DATA FLOW": "",
            "📥 Extracted": f"{self.metrics.extracted_rows:,}" if self.metrics else "0",
            "📦 Staging": f"{self.metrics.staging_rows:,}" if self.metrics else "0",
            "📤 Loaded": f"{self.metrics.loaded_rows:,}" if self.metrics else "0",
            "➕ Inserted (New)": f"{self.metrics.inserted_rows:,}" if self.metrics else "0",
            "🔄 Updated": f"{self.metrics.updated_rows:,}" if self.metrics else "0",
            "🗑️ Soft Deleted": f"{self.metrics.soft_deleted_rows:,}"
            if self.metrics and self.metrics.soft_deleted_rows > 0
            else "0",
            "🔁 Replaced": f"{self.metrics.replaced_rows:,}"
            if self.metrics and self.metrics.replaced_rows > 0
            else "0",
            "⏮️ Lookback Deleted": f"{self.metrics.deleted_lookback_rows:,}"
            if self.metrics and self.metrics.deleted_lookback_rows > 0
            else "0",
            "🎯 Final Count": f"{self.metrics.final_rows:,}" if self.metrics else "0",
            "EMPTY_LINE2": "",
            "⚡ PERFORMANCE": "",
            "🚀 Rows/Second": f"{self.metrics.rows_per_second:.2f}"
            if self.metrics and self.metrics.rows_per_second
            else "N/A",
            "📦 Batches Processed": self.metrics.batch_count if self.metrics else 0,
            "❌ Errors": self.metrics.error_count if self.metrics else 0,
            "⚠️ Warnings": self.metrics.warnings_count if self.metrics else 0,
        }

        if additional_info and "data_flow_explanation" in additional_info:
            flow_info = additional_info["data_flow_explanation"]
            details["EMPTY_LINE3"] = ""
            details["🔄 DATA FLOW EXPLANATION"] = ""
            details["   Source"] = flow_info.get("extracted_from", "Unknown")
            details["   Staging"] = flow_info.get("loaded_to_staging", "Unknown")
            details["   Target"] = flow_info.get("final_target", "Unknown")
            details["   Strategy"] = flow_info.get("strategy", "Unknown")
            details["   Deduplication"] = flow_info.get("deduplication", "Unknown")

        # Валидация для ClickHouse source: сравнение с подсчетом строк в CH
        validation_info = result.get("validation_info")
        if validation_info and status == "success":
            self._validate_clickhouse_rows(result, validation_info, details)

        if status == "success":
            self._log_with_format(LogLevel.SUCCESS, "ETL PROCESS COMPLETED SUCCESSFULLY", details)
        else:
            errors = result.get("errors", [])
            if errors:
                details["EMPTY_LINE3"] = ""
                details["🚨 ERRORS"] = ""
                for i, error in enumerate(errors, 1):
                    details[f"   {i}"] = error
            self._log_with_format(LogLevel.ERROR, "ETL PROCESS FAILED", details)

    def log_etl_error(self, error: str, context: dict[str, Any] | None = None) -> None:
        if self.metrics:
            self.metrics.error_count += 1

        details = {"❌ Error": error, "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

        if context:
            details["EMPTY_LINE"] = ""
            details["🔍 CONTEXT"] = ""
            for key, value in context.items():
                details[f"   {key}"] = str(value)

        self._log_with_format(LogLevel.ERROR, "ETL PROCESS ERROR", details)

    def warning(self, message: str, *args, **kwargs) -> None:
        """Логирует warning сообщение с поддержкой форматирования"""
        if self.metrics:
            self.metrics.warnings_count += 1
        self.logger.warning(message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs) -> None:
        """Логирует info сообщение с поддержкой форматирования"""
        self.logger.info(message, *args, **kwargs)

    def log_etl_progress(self, stage: str, details: dict[str, Any] | None = None) -> None:
        progress_details = {"🎯 Stage": stage, "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

        if details:
            progress_details["EMPTY_LINE"] = ""
            for key, value in details.items():
                progress_details[f"   {key}"] = str(value)

        self._log_with_format(LogLevel.STAGE, f"ETL STAGE: {stage.upper()}", progress_details)

    def log_data_sample(self, data_type: str, sample_data: list, max_rows: int = 5) -> None:
        if not sample_data:
            return

        sample_size = min(max_rows, len(sample_data))

        details = {
            "📊 Data Type": data_type,
            "📏 Sample Size": f"{sample_size} of {len(sample_data):,} rows",
            "EMPTY_LINE": "",
            "📋 SAMPLE DATA": "",
        }

        for i, row in enumerate(sample_data[:sample_size], 1):
            details[f"   Row {i}"] = str(row)

        self._log_with_format(LogLevel.DATA, f"DATA SAMPLE: {data_type.upper()}", details)

    def log_batch_progress(self, batch_num: int, total_batches: int, batch_size: int, processed_rows: int) -> None:
        if self.metrics:
            self.metrics.batch_count = batch_num

        progress_percent = (batch_num / total_batches) * 100 if total_batches > 0 else 0

        details = {
            "📊 Progress": f"{batch_num}/{total_batches} ({progress_percent:.1f}%)",
            "📦 Batch Size": f"{batch_size:,} rows",
            "✅ Processed": f"{processed_rows:,} rows",
            "⏳ Remaining": f"{(total_batches - batch_num) * batch_size:,} rows",
        }

        self._log_with_format(LogLevel.BATCH, f"BATCH PROGRESS: {batch_num}/{total_batches}", details)

    def log_connection_info(self, connection_type: str, host: str, port: int, database: str) -> None:
        connection_key = f"{connection_type}:{host}:{port}:{database}"

        if connection_key in self._logged_connections:
            return

        self._logged_connections.add(connection_key)

        details = {
            "🔌 Connection Type": connection_type,
            "🗄️ Database": database,
            "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        self._log_with_format(LogLevel.CONNECTION, f"CONNECTION ESTABLISHED: {connection_type.upper()}", details)

    def log_proxy_initialization(
        self,
        vault_path: str,
        proxy_name: str,
        client_type: str = "BigQuery",
    ) -> None:
        """
        Логирует инициализацию прокси в одном красивом блоке.

        Args:
            vault_path: Путь к секрету в Vault (например, "{env_code}/network/proxy/gcp/current")
            proxy_name: Имя прокси (например, "dp-squid-proxy")
            client_type: Тип клиента (например, "BigQuery")
        """
        details = {
            "🔐 Vault Path": vault_path,
            "🌐 Proxy Name": proxy_name,
            "🔌 Client Type": client_type,
            "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        self._log_with_format(LogLevel.CONNECTION, "PROXY INITIALIZATION COMPLETE", details)

    def log_performance_metrics(self, metrics: dict[str, Any]) -> None:
        details = {
            "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "EMPTY_LINE": "",
            "⚡ PERFORMANCE METRICS": "",
        }

        for key, value in metrics.items():
            if isinstance(value, int | float):
                if "time" in key.lower() or "duration" in key.lower():
                    details[f"   {key}"] = f"{value:.3f}s"
                elif "rate" in key.lower() or "speed" in key.lower():
                    details[f"   {key}"] = f"{value:.2f}/s"
                elif "size" in key.lower() or "rows" in key.lower():
                    details[f"   {key}"] = f"{value:,}"
                else:
                    details[f"   {key}"] = f"{value:,}"
            else:
                details[f"   {key}"] = str(value)

        self._log_with_format(LogLevel.PERFORMANCE, "PERFORMANCE METRICS", details)

    def log_sql_query(self, query: str, params: dict[str, Any] | None = None) -> None:
        details = {
            "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "EMPTY_LINE": "",
            "🔍 SQL QUERY": "",
            "SQL": query.strip(),
        }

        self._log_with_format(LogLevel.DEBUG, "SQL QUERY EXECUTION", details)

    def log_data_quality_check(self, check_name: str, result: dict[str, Any]) -> None:
        details = {
            "🔍 Check Name": check_name,
            "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "EMPTY_LINE": "",
            "📊 QUALITY CHECK RESULTS": "",
        }

        for key, value in result.items():
            details[f"   {key}"] = str(value)

        status = result.get("status", "unknown")
        level = LogLevel.SUCCESS if status == "passed" else LogLevel.WARNING

        self._log_with_format(level, f"DATA QUALITY CHECK: {check_name.upper()}", details)

    def log_state_change(self, state_type: str, old_state: Any, new_state: Any) -> None:
        details = {
            "🔄 State Type": state_type,
            "🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "EMPTY_LINE": "",
            "📊 STATE CHANGE": "",
            "   Previous": str(old_state),
            "   New": str(new_state),
        }

        self._log_with_format(LogLevel.INFO, f"STATE CHANGE: {state_type.upper()}", details)

    def log_xmin_state_info(self, message: str, details: dict[str, Any] | None = None) -> None:
        log_details = {"🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

        if details:
            log_details.update(details)

        self._log_with_format(LogLevel.INFO, f"XMIN STATE: {message}", log_details)

    def log_data_loader_info(self, message: str, details: dict[str, Any] | None = None) -> None:
        log_details = {"🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

        if details:
            log_details.update(details)

        self._log_with_format(LogLevel.INFO, f"DATA LOADER: {message}", log_details)

    def log_run_state_info(self, message: str, details: dict[str, Any] | None = None) -> None:
        """Логирует информацию о состоянии выполнения ETL процессов."""
        log_details = {"🕐 Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

        if details:
            log_details.update(details)

        self._log_with_format(LogLevel.INFO, f"RUN STATE: {message}", log_details)

    def _validate_single_partition(
        self,
        partition_date: str,
        source_connector: Any,
        source_schema: str,
        source_table: str,
        date_column: str,
        partition_by: str,
        target_count: int,
    ) -> bool:
        return validate_single_partition(
            self,
            partition_date,
            source_connector,
            source_schema,
            source_table,
            date_column,
            partition_by,
            target_count,
        )

    def _validate_clickhouse_rows(
        self,
        result: dict[str, Any],
        validation_info: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        validate_clickhouse_rows(self, result, validation_info, details)

    def format_strategy_from_config(self, load_config) -> str:
        strategy = load_config.load_strategy.value
        unique_key = str(load_config.unique_key) if load_config.unique_key else None
        custom_predicate = load_config.custom_predicate

        return _format_strategy_with_color(
            strategy,
            show_colors=self.show_colors,
            unique_key=unique_key,
            custom_predicate=custom_predicate,
        )


etl_logger = ETLLogger()
