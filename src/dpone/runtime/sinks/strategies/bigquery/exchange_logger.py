"""Structured logging helpers for BigQuery exchange/full-refresh flows."""

from __future__ import annotations

from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.strategies.progress import ProgressEvent, StrategyProgressLogger


class BigQueryExchangeLogger:
    """BigQuery semantic progress events over the shared strategy logger."""

    def __init__(self, logger: ETLLogger | None) -> None:
        self.logger = logger or etl_logger
        self._progress = StrategyProgressLogger(self.logger)

    def log_exchange_start(self, fq_target: str) -> None:
        self._emit(
            "BQ_EXCHANGE_START",
            payload={
                "Target": fq_target,
                "Mode": "Exchange Pattern (атомарная замена)",
            },
        )

    def log_full_refresh_create_new(self, fq_target: str) -> None:
        self._emit(
            "BQ_FULL_REFRESH_CREATE_NEW",
            payload={
                "Target": fq_target,
                "Mode": "CREATE TABLE AS (первая загрузка)",
            },
        )

    def log_exchange_create_tmp(self, schema: str, tmp_table: str) -> None:
        self._emit(
            "BQ_EXCHANGE_CREATE_TMP",
            payload={
                "TmpTable": f"{schema}.{tmp_table}",
                "Source": "staging",
            },
        )

    def log_exchange_tmp_created(self, schema: str, tmp_table: str, inserted: int) -> None:
        self._emit(
            "BQ_EXCHANGE_TMP_CREATED",
            payload={
                "TmpTable": f"{schema}.{tmp_table}",
                "Inserted": inserted,
            },
        )

    def log_exchange_backup(self, schema: str, target_table: str, backup_table: str) -> None:
        self._emit(
            "BQ_EXCHANGE_BACKUP",
            payload={
                "Target": f"{schema}.{target_table}",
                "Backup": f"{schema}.{backup_table}",
            },
        )

    def log_exchange_swap(self, schema: str, target_table: str, tmp_table: str) -> None:
        self._emit(
            "BQ_EXCHANGE_SWAP",
            payload={
                "Target": f"{schema}.{target_table}",
                "TmpTable": f"{schema}.{tmp_table}",
                "Status": "Swapped",
            },
        )

    def log_exchange_cleanup(self, schema: str, backup_table: str) -> None:
        self._emit(
            "BQ_EXCHANGE_CLEANUP",
            payload={
                "Backup": f"{schema}.{backup_table}",
                "Status": "Dropped",
            },
        )

    def log_exchange_complete(self, fq_target: str, inserted: int) -> None:
        self._emit(
            "BQ_EXCHANGE_COMPLETE",
            payload={
                "Target": fq_target,
                "Inserted": inserted,
                "Mode": "Exchange Pattern",
            },
        )

    def log_exchange_rollback(self, schema: str, target_table: str) -> None:
        self._emit(
            "BQ_EXCHANGE_ROLLBACK",
            payload={
                "Target": f"{schema}.{target_table}",
                "Status": "Restored from backup",
            },
        )

    def log_exchange_rollback_failed(self, rollback_error: Exception, original_error: Exception) -> None:
        self._emit(
            "BQ_EXCHANGE_ROLLBACK_FAILED",
            payload={
                "Error": str(rollback_error),
                "Original_Error": str(original_error),
            },
        )

    def log_full_refresh(self, fq_target: str, columns_count: int) -> None:
        self._emit(
            "BQ_FULL_REFRESH",
            payload={
                "Target": fq_target,
                "Action": "TRUNCATE + INSERT",
                "Columns": columns_count,
            },
        )

    def _emit(self, code: str, *, payload: dict[str, object]) -> None:
        self._progress.emit(ProgressEvent(code=code, payload=payload))
