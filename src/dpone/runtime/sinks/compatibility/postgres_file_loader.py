"""Загрузка PostgreSQL sink из FileExportArtifact."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager


class PostgresFileExportLoader:
    """Загрузка target-таблиц PostgreSQL из экспортированных файлов."""

    def __init__(
        self,
        connector: Any,
        logger: ETLLogger | None,
        target_table_manager: PostgresTargetTableManager,
        log_target_sample: Callable[[Any, int], None],
    ) -> None:
        self.connector = connector
        self.logger = logger or etl_logger
        self.target_table_manager = target_table_manager
        self._log_target_sample = log_target_sample

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        artifact: FileExportArtifact = payload.artifact
        overwrite_type = getattr(load_config, "overwrite_type", None)

        if overwrite_type == "exchange":
            return self.load_with_exchange(load_config, payload, artifact)
        if overwrite_type == "truncate_insert":
            return self.load_with_truncate(load_config, payload, artifact)
        return self.load_standard(load_config, payload, artifact)

    def load_standard(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        return self._load_selected_strategy(load_config, payload, artifact)

    def load_with_truncate(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        return self._load_selected_strategy(load_config, payload, artifact)

    def load_with_exchange(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        return self._load_selected_strategy(load_config, payload, artifact)

    def _load_selected_strategy(
        self,
        load_config: Any,
        payload: LoadPayload,
        artifact: FileExportArtifact,
    ) -> LoadResult:
        """Retain loader signatures; the configured strategy owns write semantics."""
        from dpone.runtime.sinks.postgres import PostgresSink

        try:
            return PostgresSink(self.connector, None, self.logger).load(load_config, payload.rebind(artifact=artifact))
        finally:
            # Historical standalone loaders own their temporary export file.
            # cleanup_file is best-effort and cannot hide a database OSError.
            self.cleanup_file(artifact)

    def copy_from_artifact(
        self,
        target_schema: str,
        target_table: str,
        schema: Sequence[tuple[str, str]],
        artifact: FileExportArtifact,
    ) -> int:
        """Adapt the historical COPY helper to the canonical file transport."""
        manager = PostgresStagingManager(self.connector, self.logger)
        handle = StagingTableArtifact(target_schema, target_table, [name for name, _ in schema], manager)
        return manager.load_from_file(handle, artifact)

    def cleanup_file(self, artifact: FileExportArtifact) -> None:
        try:
            os.remove(artifact.file_path)
        except OSError:
            pass
