"""Shared implementation for runtime logging ports."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from dpone.runtime.etl_logging.etl_logger import ETLLogger as _ConcreteETLLogger
from dpone.runtime.etl_logging.etl_logger import etl_logger as _etl_logger


@runtime_checkable
class RuntimeLogger(Protocol):
    """Shared logging capability implemented by the default ETL logger."""

    def info(self, message: str, *args: Any, **kwargs: Any) -> None: ...

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None: ...

    def log_etl_start(self, config: dict[str, Any]) -> None: ...

    def log_etl_end(self, result: dict[str, Any], additional_info: dict[str, Any] | None = None) -> None: ...

    def log_etl_error(self, error: str, context: dict[str, Any] | None = None) -> None: ...

    def log_etl_progress(self, stage: str, details: dict[str, Any] | None = None) -> None: ...

    def log_proxy_initialization(
        self,
        vault_path: str,
        proxy_name: str,
        client_type: str = "BigQuery",
    ) -> None: ...

    def log_run_state_info(self, message: str, details: dict[str, Any] | None = None) -> None: ...

    def log_sql_query(self, query: str, params: dict[str, Any] | None = None) -> None: ...

    def log_xmin_state_info(self, message: str, details: dict[str, Any] | None = None) -> None: ...


etl_logger: RuntimeLogger = _etl_logger


def create_etl_logger(log_level: str = "INFO", show_colors: bool = True) -> RuntimeLogger:
    """Create the default runtime logger implementation."""

    return _ConcreteETLLogger(log_level=log_level, show_colors=show_colors)


__all__ = ["RuntimeLogger", "create_etl_logger", "etl_logger"]
